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

public class StreamingUpdateImpl extends StreamingModifyImpl {
  public StreamingUpdateImpl(ModifyStmt modifyStmt) {
    super(modifyStmt);
  }

  @Override
  public DataSink createDataSink() {
    // UPDATE -> select all matching primary keys and missing rows, delete rows, then
    // upsert new rows into Kudu.
    Preconditions.checkState(modifyStmt_.table_ instanceof FeKuduTable);
    TableSink.Op op = isKuduOnly_ ? TableSink.Op.UPDATE : TableSink.Op.UPSERT;
    if (getKuduTable().isPrimaryKeyUnique()) {
      // For tables with unique primary keys we can directly upsert the modified rows
      // without deleting first.
      return new KuduTableSink(modifyStmt_.table_, op, referencedColumns_,
          sourceStmt_.getResultExprs(), modifyStmt_.getKuduTransactionToken());
    }
    return new KuduTableSink(modifyStmt_.table_, op, referencedColumns_,
        sourceStmt_.getResultExprs(), modifyStmt_.getKuduTransactionToken(),
        deleteTableId_, deleteTableColumns_);
  }
}
