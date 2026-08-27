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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TSortingOrder;

import com.google.common.base.Preconditions;

/**
 * Represents a CREATE TABLE ... STORED AS STREAMING statement.
 *
 * <p>A streaming table is an Iceberg table (the unified read/query path) backed by
 * two automatically-generated tables:
 * <ul>
 *   <li>A Kudu table ({@code <name>_kudu}) used for low-latency upserts / deletes.
 *   <li>An Iceberg table ({@code <name>_iceberg}) holding compacted historical data.
 * </ul>
 *
 * <p>The tblproperties {@code impala.streaming.kudu} and
 * {@code impala.streaming.iceberg} are set on the streaming table to point at the
 * backing tables; {@code impala.streaming.pit} and {@code impala.streaming.dels}
 * are pre-populated with empty strings and filled in later by MIGRATE and related
 * DDL operations.
 *
 * <p>The primary-key column information supplied by the user is forwarded to the
 * Kudu backing table only; the streaming table and the Iceberg backing table are
 * created without primary-key constraints.
 *
 * <p>Example:
 * <pre>
 *   CREATE TABLE orders (
 *     id INT,
 *     cust_id INT,
 *     amount DECIMAL(10,2),
 *     PRIMARY KEY (id)
 *   ) STORED AS STREAMING;
 * </pre>
 *
 * <p>This extends {@link CreateTableStmt} so that the standard DDL execution path
 * in the frontend can be reused for the streaming table itself.  Callers must
 * execute the two backing {@link CreateTableStmt} instances (available via
 * {@link #getKuduStmt()} and {@link #getIcebergStmt()}) <em>before</em> executing
 * this statement.
 */
public class CreateStreamingTableStmt extends CreateTableStmt {

  // Original streaming-table definition, preserved for Kudu-backed tables.
  private final TableDef kuduTableDef_;

  // Primary-key metadata captured from the original table definition.
  private final List<String> streamingPrimaryKeyColNames_;

  // Backing-table CreateTableStmt instances, populated during analyze().
  private CreateTableStmt kuduStmt_;
  private CreateTableStmt icebergStmt_;
  private CreateTableStmt delsStmt_;

  /**
   * Constructs a streaming-table statement from a {@link TableDef} whose file format
   * is {@link THdfsFileFormat#STREAMING} (as set by the parser via {@code tbl_options}).
   *
   * <p>The file format is remapped to {@link THdfsFileFormat#ICEBERG} because the
   * streaming table is itself stored as Iceberg on the read/query path.  The primary-key
   * information is extracted and stored separately before the standard analysis (which
   * would reject a unique primary key on an Iceberg table).
   */
  public CreateStreamingTableStmt(TableDef tableDef) {
    super(remapToIceberg(tableDef));
    kuduTableDef_ = tableDef;

    LinkedHashSet<String> pkColNames = new LinkedHashSet<>(
        tableDef.getPrimaryKeyColumnNames());
    for (ColumnDef colDef : tableDef.getColumnDefs()) {
      if (!colDef.isPrimaryKey()) continue;
      pkColNames.add(colDef.getColName());
    }
    streamingPrimaryKeyColNames_ = new ArrayList<>(pkColNames);
  }

  /**
   * Creates a new Iceberg-v3 {@link TableDef} derived from the streaming-table definition.
   *
   * <p>Only properties relevant to Iceberg are copied. Kudu-only metadata, such as
   * Kudu partition parameters and primary-key definitions, is intentionally omitted.
   * Column definitions are converted with {@link ColumnDef#copyForIceberg(ColumnDef)}
   * so incompatible Kudu options are removed and unsupported narrow integer types are
   * widened where needed.
   */
  private static TableDef remapToIceberg(TableDef tableDef) {
    TableDef icebergDef = new TableDef(
        tableDef.getTblName(), tableDef.isExternal(), tableDef.getIfNotExists());

    for (ColumnDef col : tableDef.getColumnDefs()) {
      icebergDef.getColumnDefs().add(ColumnDef.copyForIceberg(col));
    }

    icebergDef.getIcebergPartitionSpecs().addAll(tableDef.getIcebergPartitionSpecs());

    TableDef.Options old = tableDef.getOptions();
    Map<String, String> tblProps = old.tblProperties != null ?
        new HashMap<>(old.tblProperties) : new HashMap<>();
    // Set format-version 3 to support column DEFAULT values in Iceberg
    tblProps.put("format-version", "3");
    TableDef.Options opts = new TableDef.Options(
        tableDef.geTBucketInfo(),
        new Pair<>(old.sortCols, old.sortingOrder),
        old.comment,
        old.rowFormat,
        old.serdeProperties != null ? old.serdeProperties : new HashMap<>(),
        THdfsFileFormat.ICEBERG,
        old.location,
        old.cachingOp,
        tblProps,
        new TQueryOptions());
    icebergDef.setOptions(opts);
    return icebergDef;
  }

  // ---------------------------------------------------------------------------
  // Accessors
  // ---------------------------------------------------------------------------

  /**
   * Returns the {@link CreateTableStmt} for the backing Kudu table.  Only valid
   * after {@link #analyze(Analyzer)} has been called.
   */
  public CreateTableStmt getKuduStmt() {
    Preconditions.checkState(isAnalyzed(), "getKuduStmt() called before analyze()");
    return kuduStmt_;
  }

  /**
   * Returns the {@link CreateTableStmt} for the backing Iceberg table.  Only valid
   * after {@link #analyze(Analyzer)} has been called.
   */
  public CreateTableStmt getIcebergStmt() {
    Preconditions.checkState(isAnalyzed(), "getIcebergStmt() called before analyze()");
    return icebergStmt_;
  }

  /**
   * Returns the {@link CreateTableStmt} for the backing deletes (dels) Kudu table.
   * Only valid after {@link #analyze(Analyzer)} has been called.
   */
  public CreateTableStmt getDelsStmt() {
    Preconditions.checkState(isAnalyzed(), "getDelsStmt() called before analyze()");
    return delsStmt_;
  }

  // ---------------------------------------------------------------------------
  // Analysis
  // ---------------------------------------------------------------------------

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (streamingPrimaryKeyColNames_.isEmpty()) {
      throw new AnalysisException(
          "STORED AS STREAMING requires at least one PRIMARY KEY column.");
    }

    // Analyze the streaming table itself as a plain Iceberg table (no primary keys).
    super.analyze(analyzer);

    String db = getDb();
    String baseName = getTbl();
    boolean ifNotExists = getIfNotExists();

    // Derive backing table names.
    String kuduName    = baseName + "_kudu";
    String icebergName = baseName + "_iceberg";
    String delsName    = baseName + "_dels";

    // Build and analyze the Kudu backing table.
    kuduStmt_ = buildKuduStmt(db, kuduName, ifNotExists, analyzer.getQueryOptions());
    kuduStmt_.analyze(analyzer);

    // Copy Kudu master addresses onto the user-visible streaming table definition so
    // execution paths (e.g. PIT create/drop) don't need to resolve them from backings.
    getTblProperties().putIfAbsent(KuduTable.KEY_MASTER_HOSTS,
        BackendConfig.INSTANCE.getBackendCfg().kudu_master_hosts);

    // Build and analyze the Iceberg backing table.
    icebergStmt_ = buildIcebergStmt(
        db, icebergName, ifNotExists, analyzer.getQueryOptions());
    icebergStmt_.analyze(analyzer);

    // Build and analyze the dels (deletes) Kudu backing table.
    delsStmt_ = buildDelsStmt(db, delsName, ifNotExists, analyzer.getQueryOptions());
    delsStmt_.analyze(analyzer);

    // Inject streaming tblproperties so FeTable.isStreaming() recognises this table.
    getTblProperties().put(FeTable.STREAMING_KUDU,    kuduName);
    getTblProperties().put(FeTable.STREAMING_ICEBERG, icebergName);
    getTblProperties().put(FeTable.STREAMING_DELS,    delsName);
    // PIT is managed by MIGRATE; pre-populate with an empty string so the property
    // is present in the metastore even before the first migration.
    getTblProperties().putIfAbsent(FeTable.STREAMING_PIT, "");
  }

  /**
   * Returns a {@link TCreateTableParams} for the streaming table that also embeds the
   * two backing-table creates in {@code backing_creates}.  {@link CatalogOpExecutor}
   * processes those first before creating the streaming table itself.
   */
  @Override
  public TCreateTableParams toThrift() {
    TCreateTableParams params = super.toThrift();
    List<TCreateTableParams> backingCreates = new ArrayList<>();
    backingCreates.add(kuduStmt_.toThrift());
    backingCreates.add(icebergStmt_.toThrift());
    backingCreates.add(delsStmt_.toThrift());
    params.setBacking_creates(backingCreates);
    return params;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds the {@link CreateTableStmt} for the backing Kudu table.
   * All user-supplied columns are included; the primary-key column list that was
   * captured in the constructor is applied here. Kudu partition parameters are
   * forwarded from the streaming table definition.
   */
  private CreateTableStmt buildKuduStmt(
      String db, String tblName, boolean ifNotExists, TQueryOptions queryOptions) {
    TableName kuduTableName = new TableName(db, tblName);
    TableDef kuduDef = new TableDef(kuduTableName, false, ifNotExists);

    // Preserve original Kudu-relevant column options and PK declarations.
    kuduDef.getColumnDefs().addAll(kuduTableDef_.getColumnDefs());
    kuduDef.getPrimaryKeyColumnNames().addAll(kuduTableDef_.getPrimaryKeyColumnNames());
    kuduDef.setPrimaryKeyUnique(kuduTableDef_.isPrimaryKeyUnique());
    kuduDef.getKuduPartitionParams().addAll(kuduTableDef_.getKuduPartitionParams());

    Map<String, String> props = new HashMap<>();
    TableDef.Options opts = new TableDef.Options(
        kuduTableDef_.geTBucketInfo(),
        new Pair<>(kuduTableDef_.getSortColumns(), kuduTableDef_.getSortingOrder()),
        null,  // Omit comment on the Kudu table since it's not user-facing.
        kuduTableDef_.getRowFormat(),
        new HashMap<>(),
        THdfsFileFormat.KUDU,
        null, null, props, queryOptions);
    kuduDef.setOptions(opts);
    return new CreateTableStmt(kuduDef);
  }

  /**
   * Builds the {@link CreateTableStmt} for the backing dels (deletes) Kudu table.
   * The dels table stores Iceberg row positions and optional logical delete predicates.
   * Predicate rows use a reserved _row_id value and are distinguished by the implicit
   * auto_incrementing_id column added by the non-unique Kudu primary key.
   */
  private CreateTableStmt buildDelsStmt(
      String db, String tblName, boolean ifNotExists, TQueryOptions queryOptions) {
    TableName delsTableName = new TableName(db, tblName);
    TableDef delsDef = new TableDef(delsTableName, false, ifNotExists);

    ColumnDef rowIdCol = new ColumnDef(
        FeTable.STREAMING_DELS_ROW_ID, new TypeDef(Type.BIGINT));
    rowIdCol.setNullable(false);
    delsDef.getColumnDefs().add(rowIdCol);
    ColumnDef predicateCol = new ColumnDef(
        FeTable.STREAMING_DELS_PREDICATE, new TypeDef(Type.STRING));
    predicateCol.setNullable(true);
    delsDef.getColumnDefs().add(predicateCol);
    delsDef.getPrimaryKeyColumnNames().add(FeTable.STREAMING_DELS_ROW_ID);
    delsDef.setPrimaryKeyUnique(false); // _dels always uses non-unique PKs

    Map<String, String> props = new HashMap<>();
    TableDef.Options opts = new TableDef.Options(
        null,
        new Pair<>(null, TSortingOrder.LEXICAL),
        null,
        null,
        new HashMap<>(),
        THdfsFileFormat.KUDU,
        null, null, props, queryOptions);
    delsDef.setOptions(opts);
    return new CreateTableStmt(delsDef);
  }

  /**
   * Builds the {@link CreateTableStmt} for the backing Iceberg table.
   * Kudu-specific options and partitioning are omitted.
   */
  private CreateTableStmt buildIcebergStmt(
      String db, String tblName, boolean ifNotExists, TQueryOptions queryOptions) {
    TableName icebergTableName = new TableName(db, tblName);
    TableDef icebergDef = new TableDef(icebergTableName, false, ifNotExists);

    // Preserve original Iceberg-relevant column options.
    icebergDef.getColumnDefs().addAll(getColumnDefs());
    icebergDef.getIcebergPartitionSpecs().addAll(getIcebergPartitionSpecs());

    Map<String, String> props = new HashMap<>();
    // Set format-version 3 to support _row_id in Iceberg.
    props.put("format-version", "3");
    TableDef.Options opts = new TableDef.Options(
        geTBucketInfo(),
        new Pair<>(getSortColumns(), getSortingOrder()),
        null,  // Omit comment on the Iceberg table since it's not user-facing.
        getRowFormat(),
        new HashMap<>(),
        THdfsFileFormat.ICEBERG,
        null, null, props, queryOptions);
    icebergDef.setOptions(opts);
    return new CreateTableStmt(icebergDef);
  }
}
