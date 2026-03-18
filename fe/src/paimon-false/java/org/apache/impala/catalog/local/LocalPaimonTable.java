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

package org.apache.impala.catalog.local;

import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.thrift.TTableDescriptor;

import java.util.Collections;
import java.util.Set;

/**
 * Local catalog shim for Paimon tables when Paimon jars are excluded.
 */
public class LocalPaimonTable extends LocalTable implements FePaimonTable {
  public static LocalPaimonTable load(LocalDb db,
      org.apache.hadoop.hive.metastore.api.Table msTbl, MetaProvider.TableMetaRef ref)
      throws TableLoadingException {
    throw new TableLoadingException("Paimon support is disabled for this build.");
  }

  protected LocalPaimonTable(LocalDb db, org.apache.hadoop.hive.metastore.api.Table msTbl,
      MetaProvider.TableMetaRef ref) {
    super(db, msTbl, ref, new ColumnMap(Collections.<Column>emptyList(),
        0, db.getName() + "." + msTbl.getTableName(), false));
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    throw new UnsupportedOperationException("Paimon support is disabled for this build.");
  }
}
