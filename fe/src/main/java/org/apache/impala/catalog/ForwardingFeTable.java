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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;

public class ForwardingFeTable implements FeTable {
  private final FeTable base;

  public ForwardingFeTable(FeTable base) {
    this.base = base;
  }

  public FeTable getBase() {
    return base;
  }

  @Override
  public boolean isLoaded() {
    return base.isLoaded();
  }

  @Override
  public Table getMetaStoreTable() {
    return base.getMetaStoreTable();
  }

  @Override
  public String getStorageHandlerClassName() {
    return base.getStorageHandlerClassName();
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return base.getCatalogObjectType();
  }

  @Override
  public String getName() {
    return base.getName();
  }

  @Override
  public String getFullName() {
    return base.getFullName();
  }

  @Override
  public TableName getTableName() {
    return base.getTableName();
  }

  @Override
  public TImpalaTableType getTableType() {
    return base.getTableType();
  }

  @Override
  public String getTableComment() {
    return base.getTableComment();
  }

  @Override
  public List<Column> getColumns() {
    return base.getColumns();
  }

  @Override
  public List<VirtualColumn> getVirtualColumns() {
    return base.getVirtualColumns();
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    return base.getColumnsInHiveOrder();
  }

  @Override
  public List<String> getColumnNames() {
    return base.getColumnNames();
  }

  @Override
  public List<Column> getClusteringColumns() {
    return base.getClusteringColumns();
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return base.getNonClusteringColumns();
  }

  @Override
  public int getNumClusteringCols() {
    return base.getNumClusteringCols();
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    return base.isClusteringColumn(c);
  }

  @Override
  public Column getColumn(String name) {
    return base.getColumn(name);
  }

  @Override
  public ArrayType getType() {
    return base.getType();
  }

  @Override
  public FeDb getDb() {
    return base.getDb();
  }

  @Override
  public long getNumRows() {
    return base.getNumRows();
  }

  @Override
  public TTableStats getTTableStats() {
    return base.getTTableStats();
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    return base.toThriftDescriptor(tableId, referencedPartitions);
  }

  @Override
  public long getWriteId() {
    return base.getWriteId();
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    return base.getValidWriteIds();
  }

  @Override
  public String getOwnerUser() {
    return base.getOwnerUser();
  }

  @Override
  public long getCatalogVersion() { return 0; }

  @Override
  public long getLastLoadedTimeMs() { return 0; }
}
