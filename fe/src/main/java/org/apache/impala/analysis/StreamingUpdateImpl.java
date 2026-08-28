// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.KuduTableSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.util.KuduUtil;

import com.google.common.base.Preconditions;

public class StreamingUpdateImpl extends StreamingModifyImpl {
  private boolean useLogicalPredicateUpdate_ = false;
  private int deletePredicateExprIdx_ = -1;
  private int assignmentExprsExprIdx_ = -1;

  public StreamingUpdateImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
  }

  private String buildLogicalUpdateSourceSql(List<SelectListItem> normalSelectList,
      String logicalPredicateSql) {
    FeTable baseTable = getBaseTable();
    long kuduMigrationTs = baseTable.getPIT().second;
    String sourceAlias = ObjectUtils.firstNonNull(
      modifyStmt_.fromClause_.get(0).getExplicitAlias(), "base");
    String normalPredicateSql = modifyStmt_.wherePredicate_.toSql(ToSqlOptions.FOR_HBO);
    List<Expr> assignmentExprs = new ArrayList<>();
    for (int i = 0; i < modifyStmt_.assignments_.size(); ++i) {
      assignmentExprs.add(normalSelectList.get(keyColumnsOffset_ + i).getExpr());
    }
    String assignmentsSql = KuduUtil.encodeStreamingUpdateAssignments(
        modifyStmt_.assignments_, assignmentExprs);

    List<String> normalItems = new ArrayList<>();
    List<String> markerItems = new ArrayList<>();
    List<Integer> normalReferencedColumns = new ArrayList<>(referencedColumns_);
    referencedColumns_.clear();
    for (int i = 0; i < normalSelectList.size(); ++i) {
      SelectListItem item = normalSelectList.get(i);
      normalItems.add(item.toSql(ToSqlOptions.FOR_HBO));
      markerItems.add("cast(null as %s)".formatted(item.getExpr().getType().toSql()));
      referencedColumns_.add(normalReferencedColumns.get(i));
    }
    normalItems.add("cast(null as string) as `%s`".formatted(
        FeTable.STREAMING_DELS_PREDICATE));
    normalItems.add("cast(null as string) as `%s`".formatted(
        FeTable.STREAMING_DELS_ASSIGNMENTS));
    markerItems.add("%s as `%s`".formatted(
        StreamingLogicalSourceHelper.sqlStringLiteral(logicalPredicateSql),
        FeTable.STREAMING_DELS_PREDICATE));
    markerItems.add("%s as `%s`".formatted(
        StreamingLogicalSourceHelper.sqlStringLiteral(assignmentsSql),
        FeTable.STREAMING_DELS_ASSIGNMENTS));
    deletePredicateExprIdx_ = normalSelectList.size();
    assignmentExprsExprIdx_ = normalSelectList.size() + 1;
    referencedColumns_.add(-1);
    referencedColumns_.add(-1);

    return """
        select * from (
          select %1$s from %2$s for system_time from %3$s as of now() %4$s
          where %5$s
          union all
          select %6$s
        ) logical_update_src
        """.formatted(String.join(", ", normalItems), getKuduTable().getFullName(),
            kuduMigrationTs, sourceAlias, normalPredicateSql,
            String.join(", ", markerItems));
  }

  @Override
  protected void createSourceStmt(Analyzer analyzer) throws AnalysisException {
    if (sourceStmt_ != null) {
      sourceStmt_.analyze(analyzer);
      return;
    }

    ArrayList<SelectListItem> selectList = new ArrayList<>();
    buildAndValidateSelectExprs(analyzer, selectList);
  String logicalPredicateSql = modifyStmt_.wherePredicate_.toSql(ToSqlOptions.FOR_HBO);
    ((UpdateStmt) modifyStmt_).rewriteWherePredicate(analyzer);
    sourceStmt_ = new SelectStmt(new SelectList(selectList), modifyStmt_.fromClause_,
      modifyStmt_.wherePredicate_);
    sourceStmt_.analyze(analyzer);
    addCastsToAssignmentsInSourceStmt(analyzer);
    useLogicalPredicateUpdate_ = assignmentExprsColIdx_ >= 0
        && canUseLogicalPredicateModify(analyzer);
    if (!useLogicalPredicateUpdate_) return;

    StatementBase parsed = Parser.parse(
    buildLogicalUpdateSourceSql(selectList, logicalPredicateSql),
    analyzer.getQueryOptions());
    Preconditions.checkState(parsed instanceof SelectStmt);
    sourceStmt_ = (SelectStmt) parsed;
    sourceStmt_.analyze(analyzer);
    resultExprs_ = sourceStmt_.getResultExprs();
  }

  @Override
  public DataSink createDataSink() {
    // UPDATE -> select all matching primary keys and missing rows, delete rows, then
    // upsert new rows into Kudu.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeKuduTable);
    TableSink.Op op = isKuduOnly_ ? TableSink.Op.UPDATE : TableSink.Op.UPSERT;
    if (useLogicalPredicateUpdate_) {
      return new KuduTableSink(modifyStmt_.table_, op, referencedColumns_,
          sourceStmt_.getResultExprs(), modifyStmt_.getKuduTransactionToken(),
          deleteTableId_, deleteRowIdColIdx_, deletePredicateColIdx_,
          deletePredicateExprIdx_, assignmentExprsColIdx_, assignmentExprsExprIdx_);
    }
    if (getKuduTable().isPrimaryKeyUnique() || isKuduOnly_) {
      // For tables with unique primary keys we can directly upsert the modified rows
      // without deleting first.
      return new KuduTableSink(modifyStmt_.table_, op, referencedColumns_,
          sourceStmt_.getResultExprs(), modifyStmt_.getKuduTransactionToken());
    }
    // With non-unique keys, migrate treats all modified rows as new rows, so we need to
    // delete the old rows in Iceberg first by adding them to the delete table. Not needed
    // for Kudu-only updates because a non-unique row can only be in Kudu or Iceberg and
    // it won't match any in Iceberg.
    return new KuduTableSink(modifyStmt_.table_, op, referencedColumns_,
        sourceStmt_.getResultExprs(), modifyStmt_.getKuduTransactionToken(),
        deleteTableId_, deleteRowIdColIdx_);
  }
}
