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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergPositionDeleteTable;
import org.apache.impala.catalog.KuduColumn;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TIcebergFileFormat;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Abstract super class for statements that modify existing data like
 * UPDATE and DELETE.
 *
 * The ModifyStmt has four major parts:
 *   - targetTablePath (not null)
 *   - fromClause (not null)
 *   - assignmentExprs (not null, can be empty)
 *   - wherePredicate (nullable)
 *
 * In the analysis phase, a SelectStmt is created with the result expressions set to
 * match the right-hand side of the assignments in addition to projecting the key columns
 * of the underlying table. During query execution, the plan that
 * is generated from this SelectStmt produces all rows that need to be modified.
 */
public abstract class ModifyStmt extends DmlStatementBase {
  // List of explicitly mentioned assignment expressions in the UPDATE's SET clause
  protected final List<Pair<SlotRef, Expr>> assignments_;

  // Optional WHERE clause of the statement
  protected final Expr wherePredicate_;

  // Path identifying the target table.
  protected final List<String> targetTablePath_;

  // TableRef identifying the target table, set during analysis.
  protected TableRef targetTableRef_;

  protected FromClause fromClause_;

  /////////////////////////////////////////
  // START: Members that are set in first run of analyze().

  // Exprs correspond to the partitionKeyValues, if specified, or to the partition columns
  // for tables.
  protected List<Expr> partitionKeyExprs_ = new ArrayList<>();

  // For every column of the target table that is referenced in the optional
  // 'sort.columns' table property, this list will contain the corresponding result expr
  // from 'resultExprs_'. Before insertion, all rows will be sorted by these exprs. If the
  // list is empty, no additional sorting by non-partitioning columns will be performed.
  // The column list must not contain partition columns and must be empty for non-Hdfs
  // tables.
  protected List<Expr> sortExprs_ = new ArrayList<>();

  // Output expressions that produce the final results to write to the target table. May
  // include casts. Set in first run of analyze().
  //
  // In case of DELETE statements it contains the columns that identify the deleted rows.
  protected List<Expr> resultExprs_ = new ArrayList<>();

  // Result of the analysis of the internal SelectStmt that produces the rows that
  // will be modified.
  protected SelectStmt sourceStmt_;

  // Implementation of the modify statement. Depends on the target table type.
  private ModifyImpl modifyImpl_;

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table. Set in createSourceStmt() during analysis.
  protected List<Integer> referencedColumns_ = new ArrayList<>();

  // END: Members that are set in first run of analyze
  /////////////////////////////////////////

  // SQL string of the ModifyStmt. Set in analyze().
  protected String sqlString_;

  public ModifyStmt(List<String> targetTablePath, FromClause fromClause,
      List<Pair<SlotRef, Expr>> assignmentExprs, Expr wherePredicate) {
    targetTablePath_ = Preconditions.checkNotNull(targetTablePath);
    fromClause_ = Preconditions.checkNotNull(fromClause);
    assignments_ = Preconditions.checkNotNull(assignmentExprs);
    wherePredicate_ = wherePredicate;
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(targetTablePath_, null));
    fromClause_.collectTableRefs(tblRefs);
    if (wherePredicate_ != null) {
      // Collect TableRefs in WHERE-clause subqueries.
      List<Subquery> subqueries = new ArrayList<>();
      wherePredicate_.collect(Subquery.class, subqueries);
      for (Subquery sq : subqueries) {
        sq.getStatement().collectTableRefs(tblRefs);
      }
    }
  }

  /**
   * The analysis of the ModifyStmt proceeds as follows: First, the FROM clause is
   * analyzed and the targetTablePath is verified to be a valid alias into the FROM
   * clause. When the target table is identified, the assignment expressions are
   * validated and as a last step the internal SelectStmt is produced and analyzed.
   * Potential query rewrites for the select statement are implemented here and are not
   * triggered externally by the statement rewriter.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    fromClause_.analyze(analyzer);

    List<Path> candidates = analyzer.getTupleDescPaths(targetTablePath_);
    if (candidates.isEmpty()) {
      throw new AnalysisException(format("'%s' is not a valid table alias or reference.",
          Joiner.on(".").join(targetTablePath_)));
    }

    Preconditions.checkState(candidates.size() == 1);
    Path path = candidates.get(0);
    path.resolve();

    if (path.destTupleDesc() == null) {
      throw new AnalysisException(format(
          "'%s' is not a table alias. Using the FROM clause requires the target table " +
              "to be a table alias.",
          Joiner.on(".").join(targetTablePath_)));
    }

    targetTableRef_ = analyzer.getTableRef(path.getRootDesc().getId());
    if (targetTableRef_ instanceof InlineViewRef) {
      throw new AnalysisException(format("Cannot modify view: '%s'",
          targetTableRef_.toSql()));
    }

    Preconditions.checkNotNull(targetTableRef_);
    FeTable dstTbl = targetTableRef_.getTable();
    table_ = dstTbl;
    // Only Kudu and Iceberg tables can be updated.
    if (!(dstTbl instanceof FeKuduTable) && !(dstTbl instanceof FeIcebergTable)) {
      throw new AnalysisException(
          format("Impala only supports modifying Kudu and Iceberg tables, " +
              "but the following table is neither: %s",
              dstTbl.getFullName()));
    }
    if (dstTbl instanceof FeKuduTable) {
      modifyImpl_ = this.new ModifyKudu();
    } else if (dstTbl instanceof FeIcebergTable) {
      modifyImpl_ = this.new ModifyIceberg();
    }

    modifyImpl_.analyze(analyzer);

    // Make sure that the user is allowed to modify the target table. Use ALL because no
    // UPDATE / DELETE privilege exists yet (IMPALA-3840).
    analyzer.registerAuthAndAuditEvent(dstTbl, Privilege.ALL);

    // Validates the assignments_ and creates the sourceStmt_.
    if (sourceStmt_ == null) createSourceStmt(analyzer);
    sourceStmt_.analyze(analyzer);
    // Add target table to descriptor table.
    analyzer.getDescTbl().setTargetTable(table_);

    sqlString_ = toSql();
  }

  @Override
  public void reset() {
    super.reset();
    fromClause_.reset();
    if (sourceStmt_ != null) sourceStmt_.reset();
    modifyImpl_ = null;
  }

  @Override
  public List<Expr> getPartitionKeyExprs() { return partitionKeyExprs_; }
  @Override
  public List<Expr> getSortExprs() { return sortExprs_; }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return sourceStmt_.resolveTableMask(analyzer);
  }

  /**
   * Builds and validates the sourceStmt_. The select list of the sourceStmt_ contains
   * first the SlotRefs for the key Columns, followed by the expressions representing the
   * assignments. This method sets the member variables for the sourceStmt_ and the
   * referencedColumns_.
   *
   * This is only run once, on the first analysis. Following analysis will reset() and
   * reuse previously created statements.
   */
  private void createSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    // Builds the select list and column position mapping for the target table.
    ArrayList<SelectListItem> selectList = new ArrayList<>();
    buildAndValidateAssignmentExprs(analyzer, selectList);

    // Analyze the generated select statement.
    sourceStmt_ = new SelectStmt(new SelectList(selectList), fromClause_, wherePredicate_,
        null, null, null, null);

    modifyImpl_.addCastsToAssignmentsInSourceStmt(analyzer);
  }

  /**
   * Validates the list of value assignments that should be used to modify the target
   * table. It verifies that only those columns are referenced that belong to the target
   * table, no key columns are modified, and that a single column is not modified multiple
   * times. Analyzes the Exprs and SlotRefs of assignments_ and writes a list of
   * SelectListItems to the out parameter selectList that is used to build the select list
   * for sourceStmt_. A list of integers indicating the column position of an entry in the
   * select list in the target table is written to the out parameter referencedColumns.
   *
   * In addition to the expressions that are generated for each assignment, the
   * expression list contains an expression for each key column. The key columns
   * are always prepended to the list of expression representing the assignments.
   */
  private void buildAndValidateAssignmentExprs(Analyzer analyzer,
      List<SelectListItem> selectList)
      throws AnalysisException {
    // The order of the referenced columns equals the order of the result expressions
    Set<SlotId> uniqueSlots = new HashSet<>();
    Set<SlotId> keySlots = new HashSet<>();

    // Mapping from column name to index
    List<Column> cols = table_.getColumnsInHiveOrder();
    Map<String, Integer> colIndexMap = new HashMap<>();
    for (int i = 0; i < cols.size(); i++) {
      colIndexMap.put(cols.get(i).getName(), i);
    }

    modifyImpl_.addKeyColumns(analyzer, selectList, referencedColumns_, uniqueSlots,
        keySlots, colIndexMap);

    // Assignments are only used in the context of updates.
    for (Pair<SlotRef, Expr> valueAssignment : assignments_) {
      SlotRef lhsSlotRef = valueAssignment.first;
      lhsSlotRef.analyze(analyzer);

      Expr rhsExpr = valueAssignment.second;
      // No subqueries for rhs expression
      if (rhsExpr.contains(Subquery.class)) {
        throw new AnalysisException(
            format("Subqueries are not supported as update expressions for column '%s'",
                lhsSlotRef.toSql()));
      }
      rhsExpr.analyze(analyzer);

      // Correct target table
      if (!lhsSlotRef.isBoundByTupleIds(targetTableRef_.getId().asList())) {
        throw new AnalysisException(
            format("Left-hand side column '%s' in assignment expression '%s=%s' does not "
                + "belong to target table '%s'", lhsSlotRef.toSql(), lhsSlotRef.toSql(),
                rhsExpr.toSql(), targetTableRef_.getDesc().getTable().getFullName()));
      }

      Column c = lhsSlotRef.getResolvedPath().destColumn();
      // TODO(Kudu) Add test for this code-path when Kudu supports nested types
      if (c == null) {
        throw new AnalysisException(
            format("Left-hand side in assignment expression '%s=%s' must be a column " +
                "reference", lhsSlotRef.toSql(), rhsExpr.toSql()));
      }

      if (keySlots.contains(lhsSlotRef.getSlotId())) {
        boolean isSystemGeneratedColumn =
            c instanceof KuduColumn && ((KuduColumn)c).isAutoIncrementing();
        throw new AnalysisException(format("%s column '%s' cannot be updated.",
            isSystemGeneratedColumn ? "System generated key" : "Key",
            lhsSlotRef.toSql()));
      }

      if (uniqueSlots.contains(lhsSlotRef.getSlotId())) {
        throw new AnalysisException(
            format("Duplicate value assignment to column: '%s'", lhsSlotRef.toSql()));
      }

      rhsExpr = checkTypeCompatibility(targetTableRef_.getDesc().getTable().getFullName(),
          c, rhsExpr, analyzer, null /*widestTypeSrcExpr*/);
      uniqueSlots.add(lhsSlotRef.getSlotId());
      selectList.add(new SelectListItem(rhsExpr, null));
      referencedColumns_.add(colIndexMap.get(c.getName()));
    }
  }

  @Override
  public List<Expr> getResultExprs() { return sourceStmt_.getResultExprs(); }

  @Override
  public void castResultExprs(List<Type> types) throws AnalysisException {
    sourceStmt_.castResultExprs(types);
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    Preconditions.checkState(isAnalyzed());
    for (Pair<SlotRef, Expr> assignment: assignments_) {
      assignment.second = rewriter.rewrite(assignment.second, analyzer_);
    }
    sourceStmt_.rewriteExprs(rewriter);
  }

  public QueryStmt getQueryStmt() { return sourceStmt_; }

  /**
   * Return true if the target table is Kudu table.
   * Since only Kudu tables can be updated, it must be true.
   */
  public boolean isTargetTableKuduTable() { return (table_ instanceof FeKuduTable); }

  private void addKeyColumn(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName, boolean isSortingColumn)
      throws AnalysisException {
    SlotRef ref = addSlotRef(analyzer, selectList, referencedColumns, uniqueSlots,
        keySlots, colIndexMap, colName);
    resultExprs_.add(ref);
    if (isSortingColumn) sortExprs_.add(ref);
  }

  private void addPartitioningColumn(Analyzer analyzer, List<SelectListItem> selectList,
  List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
  Map<String, Integer> colIndexMap, String colName) throws AnalysisException {
    SlotRef ref = addSlotRef(analyzer, selectList, referencedColumns, uniqueSlots,
        keySlots, colIndexMap, colName);
    partitionKeyExprs_.add(ref);
    sortExprs_.add(ref);
  }

  private SlotRef addSlotRef(Analyzer analyzer, List<SelectListItem> selectList,
      List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
      Map<String, Integer> colIndexMap, String colName) throws AnalysisException {
    List<String> path = Path.createRawPath(targetTableRef_.getUniqueAlias(), colName);
    SlotRef ref = new SlotRef(path);
    ref.analyze(analyzer);
    selectList.add(new SelectListItem(ref, null));
    uniqueSlots.add(ref.getSlotId());
    keySlots.add(ref.getSlotId());
    referencedColumns.add(colIndexMap.get(colName));
    return ref;
  }

  @Override
  public abstract String toSql(ToSqlOptions options);

  private interface ModifyImpl {
    void analyze(Analyzer analyzer) throws AnalysisException;

    void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException;

    void addKeyColumns(Analyzer analyzer,
        List<SelectListItem> selectList, List<Integer> referencedColumns,
        Set<SlotId> uniqueSlots, Set<SlotId> keySlots, Map<String, Integer> colIndexMap)
        throws AnalysisException;
  }

  private class ModifyKudu implements ModifyImpl {
    // Target Kudu table. Result of analysis.
    FeKuduTable kuduTable_ = (FeKuduTable)table_;

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {}

    @Override
    public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
        throws AnalysisException {
      // cast result expressions to the correct type of the referenced slot of the
      // target table
      int keyColumnsOffset = kuduTable_.getPrimaryKeyColumnNames().size();
      for (int i = keyColumnsOffset; i < sourceStmt_.resultExprs_.size(); ++i) {
        sourceStmt_.resultExprs_.set(i, sourceStmt_.resultExprs_.get(i).castTo(
            assignments_.get(i - keyColumnsOffset).first.getType()));
      }
    }

    @Override
    public void addKeyColumns(Analyzer analyzer, List<SelectListItem> selectList,
        List<Integer> referencedColumns, Set<SlotId> uniqueSlots, Set<SlotId> keySlots,
        Map<String, Integer> colIndexMap) throws AnalysisException {
      // Add the key columns as slot refs
      for (String k : kuduTable_.getPrimaryKeyColumnNames()) {
        addKeyColumn(analyzer, selectList, referencedColumns, uniqueSlots, keySlots,
            colIndexMap, k, false);
      }
    }
  }

  private class ModifyIceberg implements ModifyImpl {
    FeIcebergTable originalTargetTable_;
    IcebergPositionDeleteTable icePosDelTable_;

    public ModifyIceberg() {
      originalTargetTable_ = (FeIcebergTable)table_;
      icePosDelTable_ = new IcebergPositionDeleteTable((FeIcebergTable)table_);
      // Make the virtual position delete table the new target table.
      table_ = icePosDelTable_;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
      setMaxTableSinks(analyzer_.getQueryOptions().getMax_fs_writers());
      if (ModifyStmt.this instanceof UpdateStmt) {
        throw new AnalysisException("UPDATE is not supported for Iceberg table " +
            originalTargetTable_.getFullName());
      }

      if (icePosDelTable_.getFormatVersion() == 1) {
        throw new AnalysisException("Iceberg V1 table do not support DELETE/UPDATE " +
            "operations: " + originalTargetTable_.getFullName());
      }

      String deleteMode = originalTargetTable_.getIcebergApiTable().properties().get(
          org.apache.iceberg.TableProperties.DELETE_MODE);
      if (deleteMode != null && !deleteMode.equals("merge-on-read")) {
        throw new AnalysisException(String.format("Unsupported delete mode: '%s' for " +
            "Iceberg table: %s", deleteMode, originalTargetTable_.getFullName()));
      }

      if (originalTargetTable_.getDeleteFileFormat() != TIcebergFileFormat.PARQUET) {
        throw new AnalysisException("Impala can only write delete files in PARQUET, " +
            "but the given table uses a different file format: " +
            originalTargetTable_.getFullName());
      }

      if (wherePredicate_ == null ||
          org.apache.impala.analysis.Expr.IS_TRUE_LITERAL.apply(wherePredicate_)) {
        // TODO (IMPALA-12136): rewrite DELETE FROM t; statements to TRUNCATE TABLE t;
        throw new AnalysisException("For deleting every row, please use TRUNCATE.");
      }
    }

    @Override
    public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
        throws AnalysisException {
    }

    @Override
    public void addKeyColumns(Analyzer analyzer, List<SelectListItem> selectList,
        List<Integer> referencedColumns,
        Set<SlotId> uniqueSlots, Set<SlotId> keySlots, Map<String, Integer> colIndexMap)
        throws AnalysisException {
      if (originalTargetTable_.isPartitioned()) {
        String[] partitionCols;
        partitionCols = new String[] {"PARTITION__SPEC__ID",
            "ICEBERG__PARTITION__SERIALIZED"};
        for (String k : partitionCols) {
          addPartitioningColumn(analyzer, selectList, referencedColumns, uniqueSlots,
              keySlots, colIndexMap, k);
        }
      }
      String[] deleteCols;
      deleteCols = new String[] {"INPUT__FILE__NAME", "FILE__POSITION"};
      // Add the key columns as slot refs
      for (String k : deleteCols) {
        addKeyColumn(analyzer, selectList, referencedColumns, uniqueSlots, keySlots,
            colIndexMap, k, true);
      }
    }
  }
}
