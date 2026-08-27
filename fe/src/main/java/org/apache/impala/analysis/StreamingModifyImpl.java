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

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.KuduUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

abstract class StreamingModifyImpl extends ModifyImpl {
  private FeTable baseTable_;
  protected FeKuduTable deleteTable_ = null;
  protected int deleteTableId_ = -1;
  protected boolean isKuduOnly_ = false;

  /////////////////////////////////////////
  // START: Members that are set in buildAndValidateSelectExprs().

  // Output expressions that produce the final results to write to the target table. May
  // include casts.
  //
  // In case of DELETE statements it contains the columns that identify the deleted
  // rows (Kudu primary keys, Iceberg file_path / position).
  protected List<Expr> resultExprs_ = new ArrayList<>();
  protected int keyColumnsOffset_ = -1;

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table.
  protected List<Integer> referencedColumns_ = new ArrayList<>();
  // Column index of _row_id in the dels table (-1 when no dels table is used).
  protected int deleteRowIdColIdx_ = -1;
  // Column index of _delete_predicate in the dels table (-1 when not used).
  protected int deletePredicateColIdx_ = -1;

  // END: Members that are set in buildAndValidateSelectExprs().
  /////////////////////////////////////////

  public StreamingModifyImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    baseTable_ = modifyStmt.table_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    deleteTable_ = KuduUtil.getKuduTable(analyzer, baseTable_.getDb().getName(),
        baseTable_.getParameter(FeTable.STREAMING_DELS));
    deleteTableId_ = analyzer.getDescTbl().addTargetTable(deleteTable_);
    Map<String, Integer> deleteTableColIndexMap = indexMap(
      deleteTable_.getColumnsInHiveOrder());
    deleteRowIdColIdx_ = deleteTableColIndexMap.get(FeTable.STREAMING_DELS_ROW_ID);
    deletePredicateColIdx_ = deleteTableColIndexMap.getOrDefault(
      FeTable.STREAMING_DELS_PREDICATE, -1);
    isKuduOnly_ = analyzer.getQueryOptions().direct_kudu_update;
  }

  private Map<String, Integer> indexMap(List<Column> columns) {
    return IntStream.range(0, columns.size()).boxed()
        .collect(Collectors.toMap(i -> columns.get(i).getName(), Function.identity()));
  }

  /**
   * Validates the list of value assignments that should be used to modify the target
   * table. It verifies that only those columns are referenced that belong to the target
   * table, no key columns are modified, and that a single column is not modified multiple
   * times. Analyzes the Exprs and SlotRefs of assignments_ and writes a list of
   * SelectListItems to the out parameter selectList that is used to build the select list
   * for sourceStmt_. A list of integers indicating the column position of an entry in the
   * select list in the target table is written to the field referencedColumns_.
   *
   * In addition to the expressions that are generated for each assignment, the
   * expression list contains an expression for each key column. The key columns
   * are always prepended to the list of expression representing the assignments.
   */
  @Override
  protected void buildAndValidateSelectExprs(Analyzer analyzer,
      List<SelectListItem> selectList) throws AnalysisException {
    // Mapping from column name to index
    Map<String, Integer> colIndexMap = indexMap(
        modifyStmt_.table_.getColumnsInHiveOrder());

    // The order of the referenced columns equals the order of the result expressions
    for (Column col : getKuduTable().getColumns()) {
      KuduColumn kcol = (KuduColumn)col;
      if (!kcol.isKey()) continue;
      Expr ref = makeSlotRef(analyzer, kcol.getName());
      selectList.add(new SelectListItem(ref, null));
      resultExprs_.add(ref);
      referencedColumns_.add(colIndexMap.get(kcol.getName()));
    }
    keyColumnsOffset_ = selectList.size();

    buildAndValidateAssignmentExprs(analyzer, selectList, colIndexMap);

    // For unique primary key tables, auto_incrementing_id is not a Kudu key column but
    // is always present in the streaming view output (as -_row_id for Iceberg rows, or
    // the constant 1 for Kudu rows). Include it here so KuduTableSink can determine the
    // row source: value <= 0 means Iceberg (skip the Kudu op, use delete table);
    // value > 0 means Kudu (perform the Kudu op, skip the delete table insert).
    // For non-unique PK tables, auto_incrementing_id is already included as a key column.
    // Referenced column index -1 signals KuduTableSink to skip the Kudu table write.
    // Skip when isKuduOnly_: in that mode the FROM clause is rewritten to the raw Kudu
    // table, which has no auto_incrementing_id column for unique PK tables, and the
    // delete table is not used so no row-source indicator is needed.
    if (getKuduTable().isPrimaryKeyUnique() && modifyStmt_.assignments_.isEmpty()
        && !isKuduOnly_) {
      Expr ref = makeSlotRef(analyzer, "auto_incrementing_id");
      selectList.add(new SelectListItem(ref, null));
      resultExprs_.add(ref);
      referencedColumns_.add(-1); // Not a Kudu column; skip Kudu table write
    }
  }

  /**
   * Validates assignments_, builds select list items for each assignment, and
   * (when not in isKuduOnly_ mode) appends the remaining non-key columns so the
   * upsert produces a complete row. Called only when assignments_ is non-empty.
   */
  private void buildAndValidateAssignmentExprs(Analyzer analyzer,
      List<SelectListItem> selectList, Map<String, Integer> colIndexMap)
      throws AnalysisException {
    if (modifyStmt_.assignments_.isEmpty()) {
      return;
    }

    Set<SlotId> keySlots = resultExprs_.stream()
        .map(e -> ((SlotRef)e).getSlotId()).collect(Collectors.toSet());
    Set<SlotId> uniqueSlots = new HashSet<>(keySlots);
    boolean convertToUtc = analyzer.getQueryOptions().isWrite_kudu_utc_timestamps();

    // Unhide target table to analyze the lhsSlotRef in its context.
    Preconditions.checkState(modifyStmt_.targetTableRef_.isHidden());
    modifyStmt_.targetTableRef_.setHidden(false);
    modifyStmt_.fromClause_.getTableRefs().forEach(r -> r.setHidden(true));
    for (Pair<SlotRef, Expr> valueAssignment : modifyStmt_.assignments_) {
      valueAssignment.first.analyze(analyzer);
    }
    modifyStmt_.fromClause_.getTableRefs().forEach(r -> r.setHidden(false));
    modifyStmt_.targetTableRef_.setHidden(true);

    for (Pair<SlotRef, Expr> valueAssignment : modifyStmt_.assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      Expr rhsExpr = valueAssignment.second;
      DmlStatementBase.checkSubQuery(lhsSlotRef, rhsExpr);
      rhsExpr.analyze(analyzer);

      DmlStatementBase.checkCorrectTargetTable(lhsSlotRef, rhsExpr,
          modifyStmt_.targetTableRef_);
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      DmlStatementBase.checkLhsIsColumnRef(lhsSlotRef, rhsExpr);

      Column c = lhsSlotRef.getResolvedPath().destColumn();

      if (keySlots.contains(lhsSlotRef.getSlotId())) {
        boolean isSystemGeneratedColumn =
            c instanceof KuduColumn && ((KuduColumn)c).isAutoIncrementing();
        throw new AnalysisException("%s column '%s' cannot be updated.".formatted(
            isSystemGeneratedColumn ? "System generated key" : "Key",
            lhsSlotRef.toSql()));
      }

      if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(
            "Duplicate value assignment to column: '%s'".formatted(lhsSlotRef.toSql()));
      }

      rhsExpr = StatementBase.checkTypeCompatibility(
          modifyStmt_.targetTableRef_.getDesc().getTable().getFullName(),
          c, rhsExpr, analyzer, null /*widestTypeSrcExpr*/);

      if (convertToUtc && rhsExpr.getType().isTimestamp()) {
        rhsExpr = ExprUtil.toUtcTimestampExpr(
            analyzer, rhsExpr, true /*expectPreIfNonUnique*/);
      }
      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns_.add(colIndexMap.get(c.getName()));
    }

    if (isKuduOnly_) {
      // In DIRECT_KUDU_UPDATE mode, we only need the key columns in the select list since
      // we only operate on the Kudu table and can use Update instead of Upsert.
      return;
    }

    // Add all remaining columns to the select and referenced columns lists to ensure the
    // upsert contains a complete row.
    Set<Integer> referencedColumnsSet = new HashSet<>(referencedColumns_);
    for (Column c : modifyStmt_.table_.getColumnsInHiveOrder()) {
      Integer colIndex = colIndexMap.get(c.getName());
      if (!referencedColumnsSet.contains(colIndex)) {
        selectList.add(new SelectListItem(makeSlotRef(analyzer, c.getName()), null));
        referencedColumns_.add(colIndex);
      }
    }
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return Collections.emptyList(); }

  @Override
  public void substituteResultExprs(ExprSubstitutionMap smap, Analyzer analyzer) {
    super.substituteResultExprs(smap, analyzer);
    resultExprs_ = Expr.substituteList(resultExprs_, smap, analyzer, true);
  }

  private Expr makeSlotRef(Analyzer analyzer, String colName) throws AnalysisException {
    List<String> path = Path.createRawPath(
        modifyStmt_.fromClause_.get(0).getUniqueAlias(), colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    boolean convertToUtc = analyzer.getQueryOptions().isWrite_kudu_utc_timestamps();
    if (convertToUtc && ref.getType().isTimestamp()) {
      return ExprUtil.toUtcTimestampExpr(
          analyzer, ref, true /*expectPreIfNonUnique*/);
    } else {
      return ref;
    }
  }

  protected FeKuduTable getKuduTable() { return (FeKuduTable)modifyStmt_.table_; }

  protected FeTable getBaseTable() { return baseTable_; }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // cast result expressions to the correct type of the referenced slot of the
    // target table
    Preconditions.checkState(keyColumnsOffset_ > 0, "keyColumnsOffset_ is not set");
    List<Pair<SlotRef, Expr>> assignments = modifyStmt_.getAssignments();
    for (int i = 0; i < assignments.size(); ++i) {
      int targetColIndex = i + keyColumnsOffset_;
      sourceStmt_.resultExprs_.set(targetColIndex, sourceStmt_.resultExprs_
          .get(targetColIndex).castTo(assignments.get(i).first.getType()));
    }
  }
}
