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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.THybridMergeOpts;
import org.apache.impala.thrift.TMergeCaseType;
import org.apache.impala.thrift.TMergeMatchType;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.KuduUtil;

import com.google.common.base.Preconditions;

/**
 * Representation of the MERGE statement. The MERGE statement has one target table, and a
 * source expression that is joined with an ON clause. The statement consists of
 * WHEN MATCHED / WHEN NOT MATCHED clauses referenced as merge cases, the evaluation of
 * these cases are following the order of their definition. The merge cases can have
 * additional filter expressions, for example: WHEN MATCHED AND s.id > 10. One MERGE
 * statement can have at most 1000 cases. The statement can insert, update and delete rows
 * of the target table by conditions defined by merge cases.
 */
public class MergeStmt extends DmlStatementBase {
  private static final int MERGE_CASE_LIMIT = 1000;
  private TableRef targetTableRef_;
  private TableRef sourceTableRef_;
  private List<MergeCase> cases_;
  private Expr onClause_;
  private MergeImpl impl_;
  private THybridMergeOpts hybridMerge_ = null;

  public MergeStmt(TableRef table) {
    sourceTableRef_ = null;
    targetTableRef_ = table;
    onClause_ = null;
    cases_ = Collections.emptyList();
  }

  public MergeStmt(TableRef target, TableRef source, Expr onClause,
      List<MergeCase> cases) {
    targetTableRef_ = target;
    sourceTableRef_ = source;
    onClause_ = onClause;
    cases_ = cases;
  }

  private String getStreamingMergeSql(Analyzer analyzer) throws AnalysisException {
    // Identify source and target tables for MERGE statement based on the streaming
    // table properties. Then construct the full merge statement.
    Map<String, String> props = table_.getMetaStoreTable().getParameters();
    String db = table_.getDb().getName();
    String icebergTable = db + "." + props.get(FeTable.STREAMING_ICEBERG);
    String delsTable = db + "." + props.get(FeTable.STREAMING_DELS);

    FeKuduTable kuduTbl = KuduUtil.getKuduTable(analyzer, db, props.get(FeTable.STREAMING_KUDU));
    String kuduMasters = kuduTbl.getKuduMasterHosts();
    List<String> primaryKeys = kuduTbl.getExplicitPrimaryKeyColumnNames();
    Preconditions.checkState(!primaryKeys.isEmpty(), "Kudu table %s has no primary keys",
        kuduTbl.getFullName());
    List<String> quotedPrimaryKeys = primaryKeys.stream()
        .map(col -> "`" + col + "`").collect(Collectors.toList());
    List<String> nonPrimaryKeys = table_.getColumnNames().stream()
        .filter(colName -> !primaryKeys.contains(colName))
        .map(col -> "`" + col + "`").collect(Collectors.toList());
    List<String> columnNames = table_.getColumnNames().stream()
        .map(col -> "`" + col + "`").collect(Collectors.toList());
    String pkJoinCondition = KuduUtil.buildJoinCondition(quotedPrimaryKeys, "src", "tgt");
    String updateList = nonPrimaryKeys.stream()
        .map(col -> "%1$s = coalesce(src.%1$s, tgt.%1$s)".formatted(col))
        .collect(Collectors.joining(", "));
    String columnList = String.join(", ", columnNames);
    String valuesList = columnNames.stream().map(col -> "src.%s".formatted(col))
        .collect(Collectors.joining(", "));

    String pitTable = KuduUtil.getKuduTableName(
        db, props.get(FeTable.STREAMING_PIT), kuduMasters);
    hybridMerge_ = new THybridMergeOpts(kuduMasters, pitTable,
        KuduUtil.getKuduTableName(db, props.get(FeTable.STREAMING_KUDU), kuduMasters),
        KuduUtil.getKuduTableName(db, props.get(FeTable.STREAMING_DELS), kuduMasters));
    KuduUtil.kuduPITStartMigration(kuduMasters, pitTable);
    try {
      Pair<Long, Long> kuduLastPIT = KuduUtil.kuduPITLookup(kuduMasters,
          pitTable, KuduUtil.LAST_MIGRATION_ID);
      Pair<Long, Long> kuduNextPIT = KuduUtil.kuduPITLookup(kuduMasters,
          pitTable, KuduUtil.NEXT_MIGRATION_ID);
      long kuduStartMigrationTs = kuduLastPIT.second;
      long kuduEndMigrationTs = kuduNextPIT.second;
      // If migration timestamp is available, use it to construct the MERGE statement to
      // capture changes since last migration. Otherwise, fallback to a full table scan.
      if (kuduStartMigrationTs > 0) {
        // If the primary key is unique, we want to ignore all Iceberg rows that match
        // keys in the Kudu table. Otherwise we only omit rows from the delete log.
        String omitKuduRows = kuduTbl.isPrimaryKeyUnique() ? "" :
            "where not is_deleted";
        String delsList = quotedPrimaryKeys.stream()
            .map(pk -> "coalesce(updates.%1$s, dels.%1$s) as %1$s".formatted(pk))
            .collect(Collectors.joining(", "));
        String delsPkJoinCondition = quotedPrimaryKeys.stream()
            .map(pk -> "updates.%1$s = dels.%1$s".formatted(pk))
            .collect(Collectors.joining(" and "));
        return """
            merge into %1$s as tgt using (
              -- Collect Kudu updates since last migration. If a row is in kudu, use
              -- DiffScan is_deleted; otherwise is_delete=true for rows in delete log.
              select %2$s, %3$s, coalesce(is_deleted, dels.is_delete) as is_delete from (
                select *, is_deleted from %4$s for system_time from %5$s as of %6$s %14$s
              ) updates full outer join (
                select distinct %7$s, true as is_delete
                from %8$s for system_time from %5$s as of %6$s) dels
              on %9$s
            ) as src on %10$s
            when matched and src.is_delete then delete
            when matched and not src.is_delete then update set %11$s
            when not matched and not src.is_delete then insert (%12$s) values (%13$s);
            """.formatted(icebergTable, delsList, String.join(", ", nonPrimaryKeys),
                kuduTbl.getFullName(), kuduStartMigrationTs, kuduEndMigrationTs,
                String.join(", ", quotedPrimaryKeys), delsTable, delsPkJoinCondition,
                pkJoinCondition, updateList, columnList, valuesList, omitKuduRows);
      } else {
        return """
            merge into %1$s as tgt
            using (select %6$s from %2$s for system_time as of %3$s) as src on %4$s
            when matched then update set %5$s
            when not matched then insert (%6$s) values (%7$s);
            """.formatted(icebergTable, kuduTbl.getFullName(), kuduEndMigrationTs,
                pkJoinCondition, updateList, columnList, valuesList);
      }
    } catch (Exception e) {
      // Cleanup the PIT entry if there is an error to avoid blocking future migrations.
      // TODO: need to handle this on failures throughout the merge, and ensure we only
      // clean up if this coordinator initiated the merge.
      try {
        KuduUtil.kuduPITEndMigration(hybridMerge_, -1);
      } catch (AnalysisException ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);
    if (targetTableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(
          String.format("Cannot modify view: %s", targetTableRef_.toSql()));
    }

    if (cases_.size() > MERGE_CASE_LIMIT) {
      String sql = toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of cases " +
          "(%s).\nStatement has %s cases:\n%s...",
          MERGE_CASE_LIMIT, cases_.size(), sqlSubstr));
    }

    targetTableRef_ = analyzer.resolveTableRef(targetTableRef_);
    table_ = targetTableRef_.getTable();
    if (sourceTableRef_ == null) {
      if (!table_.isStreaming()) {
        throw new AnalysisException(String.format(
            "Source table must be specified for non-streaming target table: %s",
            targetTableRef_.toSql()));
      }
      // TODO: this should be its own statement, that proxies to the MergeStmt it constructs.
      String sql = getStreamingMergeSql(analyzer);
      try {
        StatementBase parsed = Parser.parse(sql.toString(), analyzer.getQueryOptions());
        Preconditions.checkState(parsed instanceof MergeStmt);
        MergeStmt mergeStmt = (MergeStmt) parsed;
        mergeStmt.analyze(analyzer);
        table_ = mergeStmt.table_;
        maxTableSinks_ = mergeStmt.maxTableSinks_;
        sourceTableRef_ = mergeStmt.sourceTableRef_;
        targetTableRef_ = mergeStmt.targetTableRef_;
        onClause_ = mergeStmt.onClause_;
        cases_ = mergeStmt.cases_;
        impl_ = mergeStmt.impl_;
      } catch (Exception e) {
        try {
          KuduUtil.kuduPITEndMigration(hybridMerge_, -1);
        } catch (AnalysisException ex) {
          e.addSuppressed(ex);
        }
        throw e;
      }
      return;
    }

    sourceTableRef_ = analyzer.resolveTableRef(sourceTableRef_);

    if (impl_ == null) {
      if (table_ instanceof FeIcebergTable) {
        impl_ = new IcebergMergeImpl(this, targetTableRef_, sourceTableRef_, onClause_);
        setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());
      } else {
        throw new AnalysisException(String.format(
            "Target table must be an Iceberg table: %s", table_.getFullName()));
      }
    }

    impl_.analyze(analyzer);

    for (MergeCase mergeCase : getCases()) {
      mergeCase.setParent(this);
      mergeCase.analyze(analyzer);
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    super.collectTableRefs(tblRefs);
    if (sourceTableRef_ instanceof InlineViewRef) {
      ((InlineViewRef) sourceTableRef_).queryStmt_.collectTableRefs(tblRefs);
    } else if (sourceTableRef_ != null) {
      tblRefs.add(sourceTableRef_);
    }
    if (!(targetTableRef_ instanceof InlineViewRef)) { tblRefs.add(targetTableRef_); }
  }

  @Override
  public DataSink createDataSink() { return impl_.createDataSink(); }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    impl_.substituteResultExprs(smap, analyzer);
  }

  @Override
  public List<Expr> getResultExprs() { return impl_.getResultExprs(); }
  @Override
  public List<Expr> getPartitionKeyExprs() { return impl_.getPartitionKeyExprs(); }
  @Override
  public List<Expr> getShuffleExprs() { return impl_.getShuffleExprs(); }

  @Override
  public List<Expr> getSortExprs() { return impl_.getSortExprs(); }

  @Override
  public TSortingOrder getSortingOrder() { return impl_.getSortingOrder(); }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("MERGE INTO ");
    builder.append(targetTableRef_.toSql(options));
    builder.append(" USING ");
    builder.append(sourceTableRef_.toSql(options));
    builder.append(" ON ");
    builder.append(onClause_.toSql(options));
    for (MergeCase mergeCase : cases_) {
      builder.append(" ");
      builder.append(mergeCase.toSql(options));
    }
    return builder.toString();
  }

  @Override
  public void reset() {
    super.reset();
    impl_.reset();
    onClause_.reset();
    for (MergeCase mergeCase : cases_) {
      mergeCase.reset();
    }
  }
  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return getQueryStmt().resolveTableMask(analyzer);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    getQueryStmt().rewriteExprs(rewriter);
    for (MergeCase mergeCase : cases_) {
      mergeCase.rewriteExprs(rewriter);
    }
  }

  public QueryStmt getQueryStmt() { return impl_.getQueryStmt(); }

  public TableRef getTargetTableRef() { return targetTableRef_; }

  public PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException {
    return impl_.getPlanNode(ctx, child, analyzer);
  }

  public List<MergeCase> getCases() { return cases_; }

  public boolean hasOnlyMatchedCases() {
    return cases_.stream().allMatch(mergeCase -> mergeCase.matchType().equals(
        TMergeMatchType.MATCHED));
  }

  public boolean hasOnlyInsertCases() {
    return cases_.stream().allMatch(
        mergeCase -> mergeCase.caseType().equals(TMergeCaseType.INSERT));
  }

  public boolean hasOnlyDeleteCases() {
    return cases_.stream().allMatch(
        mergeCase -> mergeCase.caseType().equals(TMergeCaseType.DELETE));
  }

  public boolean hasUpdateCase() {
    return cases_.stream().anyMatch(
        mergeCase -> mergeCase.caseType().equals(TMergeCaseType.UPDATE));
  }

  public TableRef getSourceTableRef() {
    return sourceTableRef_;
  }

  public THybridMergeOpts getHybridMerge() {
    return hybridMerge_;
  }
}
