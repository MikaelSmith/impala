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

import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Disable codegen on plan nodes with less than N rows per compute node. Enable codegen
 * on any fragments that have codegened plan nodes.
 */
public class PlanNodeCodegenVisitor implements Visitor<PlanNode> {
  private final static Logger LOG = LoggerFactory.getLogger(PlanNodeCodegenVisitor.class);

  public PlanNodeCodegenVisitor(int rowsThreshold) {
    rowsThreshold_ = rowsThreshold;
  }

  private int rowsThreshold_;

  @Override
  public void visit(PlanNode caller) {
    PlanFragment fragment = caller.getFragment();
    Preconditions.checkState(fragment != null, "PlanNode has no fragment: {}", caller);

    long numRows = caller.getRowsProcessed();
    if (numRows < 0) {
      // Row count is unknown, so assume large and leave codegen enabled.
      LOG.trace("Number of rows unknown for {}, enabling codegen for {}",
          caller, fragment);
      fragment.markHasCodegen();
      return;
    }

    int numNodes = caller.getNumNodes();
    Preconditions.checkState(numNodes > 0);

    long numRowsPerInstance = (long)Math.ceil(numRows / (double)numNodes);
    if (numRowsPerInstance < rowsThreshold_) {
      LOG.trace("Number of rows {} < {}, disabling codegen for {} in {}",
          numRowsPerInstance, rowsThreshold_, caller, fragment);
      caller.setDisableCodegen(true);
    } else {
      LOG.trace("Number of rows {} >= {}, enabling codegen for {} in {}",
          numRowsPerInstance, rowsThreshold_, caller, fragment);
      fragment.markHasCodegen();
    }
  }

}
