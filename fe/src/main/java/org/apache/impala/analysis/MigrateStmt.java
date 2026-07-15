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
  private DmlStatementBase stmt_ = null;
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
        if (kuduTbl.isPrimaryKeyUnique()) {
          // Unique PK: full outer join the Kudu DiffScan with an inline view that is
          // the inner join of Iceberg and dels (on _row_id). COALESCE prefers the Kudu
          // row so a re-upsert after a delete wins. No WHERE clause needed; the full
          // outer join naturally covers all three cases: Kudu-only, dels-only, and both.
          String icePrefixedColumnList = columnNames.stream()
              .map(col -> "ice." + col).collect(Collectors.joining(", "));
          String coalesceSelectList = columnNames.stream()
              .map(col -> "coalesce(diff.%1$s, dels_ice.%1$s) as %1$s".formatted(col))
              .collect(Collectors.joining(", "));
          String kuduDelsPkJoinCond =
              KuduUtil.buildJoinCondition(quotedPrimaryKeys, "diff", "dels_ice");
          String updateStmt = updateList.isEmpty() ? "" :
              "when matched and not src.is_delete then update set %s".formatted(updateList);
          return """
              merge into %1$s as tgt using (
                select %2$s, coalesce(diff.is_deleted, true) as is_delete
                from %3$s for system_time from %4$s as of %5$s as diff
                full outer join (
                  select %6$s from %1$s as ice
                  join %7$s for system_time from %4$s as of %5$s as dels
                    using(_row_id)
                ) dels_ice on %8$s
              ) as src on %9$s
              when matched and src.is_delete then delete
              %10$s
              when not matched and not src.is_delete then insert (%11$s) values (%12$s);
              """.formatted(icebergTable, coalesceSelectList, kuduTbl.getFullName(),
                  startMigrationTs, endMigrationTs, icePrefixedColumnList, delsTable,
                  kuduDelsPkJoinCond, pkJoinCondition, updateStmt, columnList, valuesList);
        } else {
          // Non-unique PK: join dels with the Iceberg snapshot to get rows to delete;
          // insert new Kudu rows via UNION ALL. No subqueries.
          String icePrefixedColumnList = columnNames.stream()
              .map(col -> "ice." + col).collect(Collectors.joining(", "));
          return """
              merge into %1$s as tgt using (
                select %2$s, ice._row_id as row_id, true as is_delete
                from %1$s as ice
                join %3$s for system_time from %4$s as of %5$s as dels
                  using(_row_id)
                union all
                select %6$s, cast(null as bigint) as row_id, false as is_delete
                from %7$s for system_time from %4$s as of %5$s where not is_deleted
              ) as src on tgt._row_id = src.row_id
              when matched and src.is_delete then delete
              when not matched and not src.is_delete then insert (%6$s) values (%8$s);
              """.formatted(icebergTable, icePrefixedColumnList, delsTable,
                  startMigrationTs, endMigrationTs, columnList, kuduTbl.getFullName(), valuesList);
        }
      } else {
        if (kuduTbl.isPrimaryKeyUnique()) {
          // Unique PK: pk-based MERGE for the initial full migration.
          String updateStmt = updateList.isEmpty() ? "" :
              "when matched then update set %s".formatted(updateList);
          return """
              merge into %1$s as tgt
              using (select %6$s from %2$s for system_time as of %3$s) as src
              on %4$s
              %5$s
              when not matched then insert (%6$s) values (%7$s);
              """.formatted(icebergTable, kuduTbl.getFullName(), endMigrationTs,
                  pkJoinCondition, updateStmt, columnList, valuesList);
        } else {
          // Non-unique PK: the Iceberg table is empty on first migration so just
          // insert all Kudu rows. NULL row_id in the source ensures all rows go
          // to WHEN NOT MATCHED (src.row_id IS NOT NULL is always false).
          return "insert into %1$s (%2$s) select %2$s from %3$s for system_time as of %4$s;"
              .formatted(icebergTable, columnList, kuduTbl.getFullName(), endMigrationTs);
        }
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

    if (stmt_ == null) {
      String sql = getStreamingMergeSql(analyzer, endMigrationTs);
      StatementBase parsed = Parser.parse(sql.toString(), analyzer.getQueryOptions());
      Preconditions.checkState(parsed instanceof DmlStatementBase);
      stmt_ = (DmlStatementBase) parsed;
    }
    try {
      stmt_.analyze(analyzer);
      table_ = stmt_.getTargetTable();
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
    Preconditions.checkState(stmt_ == null);
  }

  @Override
  public DataSink createDataSink() { return stmt_.createDataSink(); }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    stmt_.substituteResultExprs(smap, analyzer);
  }

  @Override
  public List<Expr> getResultExprs() { return stmt_.getResultExprs(); }
  @Override
  public List<Expr> getPartitionKeyExprs() { return stmt_.getPartitionKeyExprs(); }
  @Override
  public List<Expr> getShuffleExprs() { return stmt_.getShuffleExprs(); }

  @Override
  public List<Expr> getSortExprs() { return stmt_.getSortExprs(); }

  @Override
  public TSortingOrder getSortingOrder() { return stmt_.getSortingOrder(); }

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
    stmt_.reset();
  }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return stmt_.resolveTableMask(analyzer);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    stmt_.rewriteExprs(rewriter);
  }

  @Override
  public QueryStmt getQueryStmt() { return stmt_.getQueryStmt(); }

  public PlanNode getPlanNode(PlannerContext ctx, PlanNode child, Analyzer analyzer)
      throws ImpalaException {
    if (stmt_ instanceof MergeStmt) {
      return ((MergeStmt) stmt_).getPlanNode(ctx, child, analyzer);
    }
    return child;
  }

  public THybridMergeOpts getHybridMerge() {
    return hybridMerge_;
  }
}
