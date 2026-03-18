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

import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.util.EventSequence;

/**
 * No-op shim for Paimon catalog operations when Paimon jars are excluded.
 */
public class PaimonCatalogOpExecutor {
  public static boolean createTable(MetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table newTable, EventSequence catalogTimeline,
      TCreateTableParams params) throws ImpalaRuntimeException {
    throw new ImpalaRuntimeException("Paimon support is disabled for this build.");
  }

  public static boolean dropTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      org.apache.impala.catalog.Table existingTbl, EventSequence catalogTimeline,
      TDropTableOrViewParams param) throws ImpalaRuntimeException {
    throw new ImpalaRuntimeException("Paimon support is disabled for this build.");
  }
}
