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

package org.apache.impala.analysis.paimon;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.ShowStatsStmt;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.common.AnalysisException;

/**
 * No-op shim when the no-paimon Maven profile is active.
 */
public class PaimonAnalyzer {
  private static AnalysisException unsupported() {
    return new AnalysisException("Paimon support is disabled for this build.");
  }

  public static void analyzeCreateTableStmt(CreateTableStmt stmt, Analyzer analyzer)
      throws AnalysisException {
    throw unsupported();
  }

  public static void analyzeShowStatStmt(ShowStatsStmt statsOp, FePaimonTable table,
      Analyzer analyzer) throws AnalysisException {
    throw unsupported();
  }
}
