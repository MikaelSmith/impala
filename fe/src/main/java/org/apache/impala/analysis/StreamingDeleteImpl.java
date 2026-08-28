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
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.KuduTableSink;
import org.apache.impala.planner.TableSink;

import com.google.common.base.Preconditions;

public class StreamingDeleteImpl extends StreamingModifyImpl {
  private boolean useLogicalPredicateDelete_ = false;
  private int deletePredicateExprIdx_ = -1;

  public StreamingDeleteImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    Preconditions.checkState(modifyStmt.assignments_.isEmpty(),
        "DELETE should not have any assignments.");
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
  }

  private int getKuduColumnIndex(String colName) {
    List<Column> columns = getKuduTable().getColumnsInHiveOrder();
    for (int i = 0; i < columns.size(); ++i) {
      if (columns.get(i).getName().equalsIgnoreCase(colName)) return i;
    }
    throw new IllegalStateException("Column not found in Kudu table: " + colName);
  }

  private String buildLogicalDeleteSourceSql() {
    FeTable baseTable = getBaseTable();
    long kuduMigrationTs = baseTable.getPIT().second;
    String sourceAlias = ObjectUtils.firstNonNull(
      modifyStmt_.fromClause_.get(0).getExplicitAlias(), "base");
    String predicateSql = modifyStmt_.wherePredicate_ == null ?
      "true" : modifyStmt_.wherePredicate_.toSql(ToSqlOptions.FOR_HBO);
    String whereSql = modifyStmt_.wherePredicate_ == null ? "" : " where " + predicateSql;

    List<String> kuduDeleteItems = new ArrayList<>();
    List<String> markerItems = new ArrayList<>();
    referencedColumns_.clear();
    for (Column col : getKuduTable().getColumns()) {
      KuduColumn kcol = (KuduColumn)col;
      if (!kcol.isKey()) continue;
      String colName = kcol.getName();
      kuduDeleteItems.add("`%s`".formatted(colName));
      markerItems.add("cast(%s as %s) as `%s`".formatted(
          FeTable.STREAMING_DELS_PREDICATE_ROW_ID, kcol.getType().toSql(), colName));
      referencedColumns_.add(getKuduColumnIndex(colName));
    }
    kuduDeleteItems.add("cast(null as string) as `%s`".formatted(
        FeTable.STREAMING_DELS_PREDICATE));
    markerItems.add("%s as `%s`".formatted(
        StreamingLogicalSourceHelper.sqlStringLiteral(predicateSql),
        FeTable.STREAMING_DELS_PREDICATE));
    referencedColumns_.add(-1);
    keyColumnsOffset_ = referencedColumns_.size();
    deletePredicateExprIdx_ = referencedColumns_.size() - 1;

    return """
        select * from (
          select %1$s from %2$s for system_time from %3$s as of now() %4$s%5$s
          union all
          select %6$s
        ) logical_delete_src
        """.formatted(String.join(", ", kuduDeleteItems), getKuduTable().getFullName(),
            kuduMigrationTs, sourceAlias, whereSql, String.join(", ", markerItems));
  }

  @Override
  protected void createSourceStmt(Analyzer analyzer) throws AnalysisException {
    if (sourceStmt_ != null) {
      sourceStmt_.analyze(analyzer);
      return;
    }

    super.createSourceStmt(analyzer);
    useLogicalPredicateDelete_ = canUseLogicalPredicateModify(analyzer);
    if (!useLogicalPredicateDelete_) return;

    StatementBase parsed = Parser.parse(
        buildLogicalDeleteSourceSql(), analyzer.getQueryOptions());
    Preconditions.checkState(parsed instanceof SelectStmt);
    sourceStmt_ = (SelectStmt) parsed;
    sourceStmt_.analyze(analyzer);
    resultExprs_ = sourceStmt_.getResultExprs();
  }

  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeKuduTable);
    if (isKuduOnly_) {
      // For Kudu-only deletes we can skip the dels table.
      return new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
          referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken());
    }
    if (useLogicalPredicateDelete_) {
      return new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
          referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken(),
          deleteTableId_, deleteRowIdColIdx_, deletePredicateColIdx_,
          deletePredicateExprIdx_);
    }
    return new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
        referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken(),
        deleteTableId_, deleteRowIdColIdx_);
  }
}
