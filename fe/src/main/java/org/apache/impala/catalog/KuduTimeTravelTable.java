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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;

public class KuduTimeTravelTable extends ForwardingFeKuduTable {

  // The Time Travel parameters that control the schema for the table.
  private final TimeTravelSpec timeTravelSpec_;

  // colsByPos[i] refers to the ith column in the table.
  protected final ArrayList<Column> colsByPos_ = new ArrayList<>();

  // map from lowercase column name to Column object.
  protected final Map<String, Column> colsByName_ = new HashMap<>();

  // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
  protected final ArrayType type_ = new ArrayType(new StructType());

  public KuduTimeTravelTable(FeKuduTable base, TimeTravelSpec timeTravelSpec) {
    super(base);
    timeTravelSpec_ = timeTravelSpec;
    // TODO: initialize columns from schema corresponding to time travel
    // specification.
    for (Column c : base.getColumns()) {
      addColumn((KuduColumn) c);
    }
    if (timeTravelSpec_.isDiffScan()) {
      // For diff scan, we need to add the additional columns for the diff metadata.
      addColumn(KuduColumn.createIsDeletedColumn(colsByPos_.size()));
    }
  }

  public TimeTravelSpec getTimeTravelSpec() {
    return timeTravelSpec_;
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    Preconditions.checkState(getNumClusteringCols() == 0);
    return colsByPos_;
  }

  @Override
  public List<String> getColumnNames() {
    return Column.toColumnNames(colsByPos_);
  }

  @Override
  public List<Column> getColumns() {
    return colsByPos_;
  }

  @Override
  public List<Column> getClusteringColumns() {
    return Collections.emptyList();
  }

  @Override
  public Column getColumn(String name) {
    return colsByName_.get(name.toLowerCase());
  }

  @Override // FeTable
  public List<Column> getNonClusteringColumns() {
    return colsByPos_;
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    Preconditions.checkArgument(colsByPos_.get(c.getPosition()) == c);
    return false;
  }

  @Override
  public ArrayType getType() { return type_; }

  public void addColumn(KuduColumn col) {
    colsByPos_.add(col);
    colsByName_.put(col.getName().toLowerCase(), col);

    ((StructType) type_.getItemType())
        .addField(new StructField(col.getName(), col.getType(), col.getComment()));
  }
}

class ForwardingFeKuduTable extends ForwardingFeTable implements FeKuduTable {
  private final FeKuduTable base;

  public ForwardingFeKuduTable(FeKuduTable base) {
    super(base);
    this.base = base;
  }

  @Override
  public String getKuduMasterHosts() {
    return base.getKuduMasterHosts();
  }

  @Override
  public String getKuduTableName() {
    return base.getKuduTableName();
  }

  @Override
  public boolean isPrimaryKeyUnique() {
    return base.isPrimaryKeyUnique();
  }

  @Override
  public boolean hasAutoIncrementingColumn() {
    return base.hasAutoIncrementingColumn();
  }

  @Override
  public List<String> getPrimaryKeyColumnNames() {
    return base.getPrimaryKeyColumnNames();
  }

  @Override
  public List<KuduPartitionParam> getPartitionBy() {
    return base.getPartitionBy();
  }
}
