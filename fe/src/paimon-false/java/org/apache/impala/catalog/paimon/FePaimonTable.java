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

import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TShowStatsOp;

/**
 * Frontend interface shim for Paimon-backed tables when Paimon jars are excluded.
 */
public interface FePaimonTable extends FeTable, FeShowFileStmtSupport {
  JobConf jobConf = new JobConf();

  default TResultSet getTableStats(TShowStatsOp op) {
    throw new UnsupportedOperationException("Paimon support is disabled for this build.");
  }

  @Override
  default TResultSet doGetTableFiles(TShowFilesParams request) {
    throw new UnsupportedOperationException("Paimon support is disabled for this build.");
  }

  @Override
  default HdfsFileFormat getTableFormat() {
    return HdfsFileFormat.PAIMON;
  }

  @Override
  default boolean supportPartitionFilter() {
    throw new UnsupportedOperationException("Paimon support is disabled for this build.");
  }

  default List<String> getPrimaryKeys() {
    throw new UnsupportedOperationException("Paimon support is disabled for this build.");
  }
}
