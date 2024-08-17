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

package org.apache.impala.util;

import org.apache.impala.planner.DataSourceScanNode;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.ScanNode;

import com.google.common.base.Preconditions;

/**
 * Returns the maximum number of rows processed by any node in a given plan tree
 */
public class MaxRowsProcessedVisitor implements Visitor<PlanNode> {

  // True if we should abort because we don't have valid estimates
  // for a plan node.
  private boolean valid_ = true;

  // Max number of rows processed across all instances of a plan node.
  private long maxRowsProcessed_ = 0;

  @Override
  public void visit(PlanNode caller) {
    if (!valid_) return;

    long numRows = caller.getRowsProcessed();
    if (numRows < 0) {
      valid_ = false;
      return;
    }
    maxRowsProcessed_ = Math.max(maxRowsProcessed_, numRows);
    Preconditions.checkState(maxRowsProcessed_ >= 0);
  }

  public boolean valid() { return valid_; }

  public long getMaxRowsProcessed() {
    Preconditions.checkState(valid_);
    return maxRowsProcessed_;
  }

}
