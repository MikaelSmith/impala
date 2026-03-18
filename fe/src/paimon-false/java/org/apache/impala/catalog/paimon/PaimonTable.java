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

package org.apache.impala.catalog.paimon;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;

import java.util.Set;

/**
 * Shim table implementation compiled when Paimon jars are excluded.
 */
public class PaimonTable extends Table implements FePaimonTable {
  public PaimonTable(org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    super(msTable, db, name, owner);
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> refParts) {
    return new TTableDescriptor(tableId, TTableType.PAIMON_TABLE, getTColumnDescriptors(),
            numClusteringCols_, name_, db_.getName());
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    throw new TableLoadingException("Paimon support is disabled for this build.");
  }
}
