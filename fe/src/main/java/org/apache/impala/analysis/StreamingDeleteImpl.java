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

import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.planner.DataSink;
import org.apache.impala.planner.KuduTableSink;
import org.apache.impala.planner.TableSink;

import com.google.common.base.Preconditions;

public class StreamingDeleteImpl extends StreamingModifyImpl {
  public StreamingDeleteImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
    Preconditions.checkState(modifyStmt.assignments_.isEmpty(),
        "DELETE should not have any assignments.");
  }

  @Override
  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeKuduTable);
    if (isKuduOnly_) {
      // For Kudu-only deletes we can skip the dels table.
      return new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
          referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken());
    }
    return new KuduTableSink(modifyStmt_.table_, TableSink.Op.DELETE,
        referencedColumns_, resultExprs_, modifyStmt_.getKuduTransactionToken(),
        deleteTableId_, deleteRowIdColIdx_);
  }
}
