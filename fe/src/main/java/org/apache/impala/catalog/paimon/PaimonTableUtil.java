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

import static org.apache.impala.catalog.Table.isExternalPurgeTable;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.impala.catalog.HdfsFileFormat;

/**
 * No-op shim for Paimon utility methods when Paimon jars are excluded.
 */
public class PaimonTableUtil {
  public static final String PAIMON_STORAGE_HANDLER =
      "org.apache.paimon.hive.PaimonStorageHandler";
  public static final String STORAGE_HANDLER = "storage_handler";

  public static boolean isPaimonTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    if (msTbl.getParameters() != null
        && PAIMON_STORAGE_HANDLER.equals(
            msTbl.getParameters().getOrDefault(STORAGE_HANDLER, ""))) {
      return true;
    }
    if (msTbl.getSd() == null) return false;
    String inputFormat = msTbl.getSd().getInputFormat();
    if (inputFormat != null
        && inputFormat.equals(HdfsFileFormat.PAIMON.inputFormat())) {
      return true;
    }
    return msTbl.getSd().getSerdeInfo() != null
        && msTbl.getSd().getSerdeInfo().getSerializationLib() != null
        && msTbl.getSd().getSerdeInfo().getSerializationLib().equals(
            HdfsFileFormat.PAIMON.serializationLib());
  }

  public static boolean isSynchronizedTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return isPaimonTable(msTbl) && (isManagedTable(msTbl) || isExternalPurgeTable(msTbl));
  }

  public static boolean isManagedTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString());
  }
}
