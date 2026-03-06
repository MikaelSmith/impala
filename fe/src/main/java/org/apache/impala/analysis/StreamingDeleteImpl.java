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
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.KuduTableSink;
import org.apache.impala.planner.MultiDataSink;
import org.apache.impala.planner.TableSink;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.KuduUtil;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamingDeleteImpl extends ModifyImpl {
  private FeTable baseTable_;
  private FeKuduTable deleteTable_ = null;
  private int deleteTableId_ = -1;

  /////////////////////////////////////////
  // START: Members that are set in buildAndValidateSelectExprs().

  // Output expressions that produce the final results to write to the target table. May
  // include casts.
  //
  // In case of DELETE statements it contains the columns that identify the deleted
  // rows (Kudu primary keys, Iceberg file_path / position).
  protected List<Expr> resultExprs_ = new ArrayList<>();

  // Position mapping of output expressions of the sourceStmt_ to column indices in the
  // target table. The i'th position in this list maps to the referencedColumns_[i]'th
  // position in the target table.
  protected List<Integer> referencedColumns_ = new ArrayList<>();
  protected List<Integer> deleteTableColumns_ = new ArrayList<>();

  // END: Members that are set in buildAndValidateSelectExprs().
  /////////////////////////////////////////

  public StreamingDeleteImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    baseTable_ = modifyStmt.table_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    deleteTable_ = KuduUtil.getKuduTable(analyzer, baseTable_.getDb().getName(),
        baseTable_.getParameter(FeTable.STREAMING_DELS));
    deleteTableId_ = analyzer.getDescTbl().addTargetTable(deleteTable_);
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
    Map<String, Integer> deleteTableColIndexMap = indexMap(
        deleteTable_.getColumnsInHiveOrder());

    // The order of the referenced columns equals the order of the result expressions
    for (String colName : getKuduTable().getPrimaryKeyColumnNames()) {
      Expr ref = makeSlotRef(analyzer, colName);
      selectList.add(new SelectListItem(ref, null));
      resultExprs_.add(ref);
      referencedColumns_.add(colIndexMap.get(colName));
      deleteTableColumns_.add(deleteTableColIndexMap.get(colName));
    }

    if (!modifyStmt_.assignments_.isEmpty()) {
      throw new AnalysisException("UPDATE not yet supported for streaming tables.");
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

  private FeKuduTable getKuduTable() { return (FeKuduTable)modifyStmt_.table_; }

  @Override
  public void addCastsToAssignmentsInSourceStmt(Analyzer analyzer)
      throws AnalysisException {
    if (!modifyStmt_.assignments_.isEmpty()) {
      throw new AnalysisException("UPDATE not yet supported for streaming tables.");
    }
  }

  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeKuduTable);
    TableSink deleteSink = new KuduTableSink(deleteTable_, TableSink.Op.INSERT,
        deleteTableColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken(),
        deleteTableId_);
    TableSink tableSink = new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
        referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken());

    MultiDataSink ret = new MultiDataSink();
    ret.addDataSink(deleteSink);
    ret.addDataSink(tableSink);
    return ret;
  }
}
