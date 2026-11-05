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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.thrift.TCTEProducer;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanNode;
import org.apache.impala.thrift.TPlanNodeType;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Predicate;

/**
 * Evaluation of a Common Table Expression for distribution to CTEConsumerNodes.
 */
public class CTEProducerNode extends PlanNode {
  private final String cteName_;

  protected final static long DEFAULT_PER_INSTANCE_MEM = 128L * 1024L * 1024L;

  // Number of consumers assumed if they cannot be determined from the plan.
  private final static int DEFAULT_NUM_CONSUMERS = 2;

  public CTEProducerNode(PlanNodeId id, PlanNode ctePlan, String cteName) {
    // Descriptor for the target view, including id
    super(id, ctePlan.getTupleIds(), "CTE PRODUCER");
    children_.add(ctePlan);
    cteName_ = cteName;
  }

  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);
    cardinality_ = capCardinalityAtLimit(getChild(0).getCardinality());
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    processingCost_ = computeDefaultProcessingCost();
  }

  /**
   * Returns the number of CTEConsumerNodes reading from each instance of this producer.
   * Usually the consumers are the destinations of the LocalMultiSink of this producer's
   * fragment. In single-fragment plans the consumers are in the same fragment.
   */
  private int getNumConsumers() {
    if (fragment_ == null) return DEFAULT_NUM_CONSUMERS;
    if (fragment_.getSink() instanceof LocalMultiSink) {
      return Math.max(1, ((LocalMultiSink) fragment_.getSink()).getNumConsumers());
    }
    Predicate<PlanNode> isConsumer = n -> n instanceof CTEConsumerNode
        && cteName_.equals(((CTEConsumerNode) n).getCteName());
    List<PlanNode> consumers = new ArrayList<>();
    fragment_.getPlanRoot().collectAll(isConsumer, consumers);
    return consumers.isEmpty() ? DEFAULT_NUM_CONSUMERS
        : (int) consumers.stream().map(PlanNode::getId).distinct().count();
  }

  @Override
  public void computeNodeResourceProfile(TQueryOptions queryOptions) {
    // CTEProducer uses a single buffer size - either the default spillable buffer size or
    // the smallest buffer size required to fit the maximum row size.
    long bufferSize = computeMaxSpillableBufferSize(
      queryOptions.getDefault_spillable_buffer_size(), queryOptions.getMax_row_size());
    // CTEProducer may need to buffer the entire input from its child.
    double fullInputSize = getChild(0).cardinality_ * avgRowSize_;
    long perInstanceMemEstimate = fullInputSize < 0 ? DEFAULT_PER_INSTANCE_MEM
        : (long) Math.ceil(fullInputSize / fragment_.getNumInstances());
    // The backend LocalExchanger buffers rows in single-page segments backed by this
    // node's reservation, which is shared with all consumers. To make progress it needs
    // one page being written plus one pinned page for each consumer reading a different
    // segment. Beyond that it increases its reservation as needed, and spills segments
    // that no consumer is reading if it can't.
    long perInstanceMinMemReservation = (getNumConsumers() + 1) * bufferSize;

    nodeResourceProfile_ = new ResourceProfileBuilder()
        .setMemEstimateBytes(perInstanceMemEstimate)
        .setMinMemReservationBytes(perInstanceMinMemReservation)
        .setSpillableBufferBytes(bufferSize)
        .setMaxRowBufferBytes(bufferSize).build();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.CTE_PRODUCER_NODE;
    msg.cte_producer = new TCTEProducer(cteName_);
  }

  @Override
  protected String getDisplayLabelDetail() {
    return cteName_;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    return String.format("%s%s [%s]\n", prefix, getDisplayLabel(), cteName_);
  }
}
