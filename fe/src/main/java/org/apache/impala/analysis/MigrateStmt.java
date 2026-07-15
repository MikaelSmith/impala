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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.THybridMergeOpts;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.KuduUtil;

import com.google.common.base.Preconditions;

/**
 * Representation of the MIGRATE statement. The MIGRATE statement has a target table and
 * generates a MERGE statement that has the target table and a source expression that is
 * constructed based on the streaming table properties.
 */
public class MigrateStmt extends DmlStatementBase {
  private TableRef streamingTableRef_;
  private MergeStmt mergeStmt_ = null;
  private TimeTravelSpec asof_ = null;
  private THybridMergeOpts hybridMerge_ = null;

  public MigrateStmt(TableRef table, TimeTravelSpec asof) {
    this.streamingTableRef_ = table;
    this.asof_ = asof;
  }

  private String getStreamingMergeSql(Analyzer analyzer, long endMigrationTs)
      throws AnalysisException {
    // Identify source and target tables for MERGE statement based on the streaming
    // table properties. Then construct the full merge statement.
    String db = table_.getDb().getName();
    String icebergTable = db + "." + table_.getParameter(FeTable.STREAMING_ICEBERG);
    String delsTableName = table_.getParameter(FeTable.STREAMING_DELS);
    String delsTable = db + "." + delsTableName;
    String kuduTableName = table_.getParameter(FeTable.STREAMING_KUDU);

    FeKuduTable kuduTbl = KuduUtil.getKuduTable(analyzer, db, kuduTableName);
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
        db, table_.getParameter(FeTable.STREAMING_PIT), kuduMasters);
    hybridMerge_ = new THybridMergeOpts(kuduMasters, pitTable,
        KuduUtil.getKuduTableName(db, kuduTableName, kuduMasters),
        KuduUtil.getKuduTableName(db, delsTableName, kuduMasters));
    KuduUtil.kuduPITStartMigration(kuduMasters, pitTable, endMigrationTs);
    try {
      Pair<Long, Long> kuduLastPIT = KuduUtil.kuduPITLookup(kuduMasters,
          pitTable, KuduUtil.LAST_MIGRATION_ID);
      long startMigrationTs = kuduLastPIT.second;
      if (startMigrationTs >= endMigrationTs) {
        throw new AnalysisException(String.format(
            "Invalid Kudu migration timestamps: start=%d, end=%d", startMigrationTs,
            endMigrationTs));
      }
      // If migration timestamp is available, use it to construct the MERGE statement to
      // capture changes since last migration. Otherwise, fallback to a full table scan.
      if (startMigrationTs > 0) {
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
                kuduTbl.getFullName(), startMigrationTs, endMigrationTs,
                String.join(", ", quotedPrimaryKeys), delsTable, delsPkJoinCondition,
                pkJoinCondition, updateList, columnList, valuesList, omitKuduRows);
      } else {
        return """
            merge into %1$s as tgt
            using (select %6$s from %2$s for system_time as of %3$s) as src on %4$s
            when matched then update set %5$s
            when not matched then insert (%6$s) values (%7$s);
            """.formatted(icebergTable, kuduTbl.getFullName(), endMigrationTs,
                pkJoinCondition, updateList, columnList, valuesList);
      }
    } catch (TableLoadingException e) {
      // Cleanup the PIT entry if there is an error to avoid blocking future migrations.
      // TODO: need to handle this on failures throughout the merge, and ensure we only
      // clean up if this coordinator initiated the merge.
      try {
        KuduUtil.kuduPITEndMigration(hybridMerge_);
      } catch (AnalysisException ex) {
        e.addSuppressed(ex);
      }
      throw new AnalysisException("Failed to load PIT for merge statement", e);
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    super.analyze(analyzer);

    streamingTableRef_ = analyzer.resolveTableRef(streamingTableRef_);
    table_ = streamingTableRef_.getTable();
    if (!table_.isStreaming()) {
      throw new AnalysisException(String.format(
          "Migrate requires a streaming table: %s", streamingTableRef_.toSql()));
    }

    long endMigrationTs;
    if (asof_ != null) {
      Preconditions.checkState(asof_.getKind() == TimeTravelSpec.Kind.TIME_AS_OF);
      asof_.analyze(analyzer);
      long asOfMicros = asof_.getAsOfMicros();
      if (asOfMicros <= 0) {
        throw new AnalysisException("Invalid AS OF timestamp: %d".formatted(asOfMicros));
      }
      endMigrationTs = asOfMicros;
    } else {
      // Convert query start time in milliseconds to microseconds.
      endMigrationTs = analyzer.getQueryCtx().getStart_unix_millis() * 1_000;
    }

    String sql = getStreamingMergeSql(analyzer, endMigrationTs);
    try {
      StatementBase parsed = Parser.parse(sql.toString(), analyzer.getQueryOptions());
      Preconditions.checkState(parsed instanceof MergeStmt);
      mergeStmt_ = (MergeStmt) parsed;
      mergeStmt_.analyze(analyzer);
      table_ = mergeStmt_.getTargetTable();
    } catch (Exception e) {
      try {
        KuduUtil.kuduPITEndMigration(hybridMerge_);
      } catch (AnalysisException ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    super.collectTableRefs(tblRefs);
    tblRefs.add(streamingTableRef_);
    Preconditions.checkState(mergeStmt_ == null);
  }

  @Override
  public DataSink createDataSink() { return mergeStmt_.createDataSink(); }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    mergeStmt_.substituteResultExprs(smap, analyzer);
  }

  @Override
  public List<Expr> getResultExprs() { return mergeStmt_.getResultExprs(); }
  @Override
  public List<Expr> getPartitionKeyExprs() { return mergeStmt_.getPartitionKeyExprs(); }
  @Override
  public List<Expr> getShuffleExprs() { return mergeStmt_.getShuffleExprs(); }

  @Override
  public List<Expr> getSortExprs() { return mergeStmt_.getSortExprs(); }

  @Override
  public TSortingOrder getSortingOrder() { return mergeStmt_.getSortingOrder(); }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("MIGRATE ");
    builder.append(streamingTableRef_.toSql(options));
    if (asof_ != null) {
      builder.append(" ").append(asof_.toSql(options));
    }
    return builder.toString();
  }

  @Override
  public void reset() {
    super.reset();
    mergeStmt_.reset();
  }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return mergeStmt_.resolveTableMask(analyzer);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    mergeStmt_.rewriteExprs(rewriter);
  }

  public QueryStmt getQueryStmt() { return mergeStmt_.getQueryStmt(); }

  public PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException {
    return mergeStmt_.getPlanNode(ctx, child, analyzer);
  }

  public THybridMergeOpts getHybridMerge() {
    return hybridMerge_;
  }
}
