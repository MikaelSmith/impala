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

package org.apache.impala.calcite.rel.node;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.calcite.rel.util.TupleDescriptorFactory;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.CTEProducerNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;

public class ImpalaCTEProducer extends Spool implements ImpalaPlanRel {
  private final String name_;

  public ImpalaCTEProducer(RelNode cte, String name) {
    this(cte.getCluster(), cte.getTraitSet(), cte, name, Type.EAGER, Type.EAGER);
  }

  private ImpalaCTEProducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode cte,
      String name, Type readType, Type writeType) {
    super(cluster, traitSet, cte, readType, writeType);
    name_ = name;
  }

  @Override
  protected ImpalaCTEProducer copy(
      RelTraitSet traitSet, RelNode input, Type readType, Type writeType) {
    return new ImpalaCTEProducer(
        getCluster(), traitSet, input, name_, readType, writeType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("cteName", name_);
  }

  @Override
  public NodeWithExprs getPlanNode(ParentPlanRelContext context) throws ImpalaException {
    PlanNodeId nodeId = context.ctx_.getNextNodeId();

    ImpalaPlanRel relInput = (ImpalaPlanRel) getInput();
    ParentPlanRelContext.Builder builder =
        new ParentPlanRelContext.Builder(context, this);
    NodeWithExprs child = relInput.getPlanNode(builder.build());

    Analyzer analyzer = context.ctx_.getRootAnalyzer();
    List<String> fieldNames = getRowType().getFieldNames();
    PlanNode physicalNode;
    List<Expr> outputExprs;
    if (!canMaterialize(child.outputExprs_)
        || hasConsumerLayout(analyzer, child, getRowType())) {
      // Buffer the child's rows as-is.
      physicalNode = new CTEProducerNode(nodeId, child.planNode_, name_);
      outputExprs = child.outputExprs_;
    } else {
      // Materialize exactly the CTE's output columns into a tuple with the same layout
      // as the tuples of CTE consumers (see NodeCreationUtils.createCTEConsumerPlanNode).
      // This avoids buffering columns only used inside the CTE, and lets consumers return
      // the buffered rows without materializing them again.
      TupleDescriptor outputTuple =
          new TupleDescriptorFactory(name_, getRowType()).create(analyzer);
      for (int i = 0; i < outputTuple.getSlots().size(); ++i) {
        SlotDescriptor slot = outputTuple.getSlots().get(i);
        Expr expr = child.outputExprs_.get(i);
        slot.setSourceExpr(expr);
        slot.setStats(ColumnStats.fromExpr(expr));
      }
      physicalNode = new CTEProducerNode(
          nodeId, child.planNode_, name_, outputTuple, child.outputExprs_);
      outputExprs = NodeCreationUtils.createOutputExprs(outputTuple.getSlots());
    }
    physicalNode.init(analyzer);
    return new NodeWithExprs(physicalNode, outputExprs, fieldNames, child.tblRefs_);
  }

  /** Returns true if 'exprs' can be materialized into a tuple by the producer. */
  private static boolean canMaterialize(List<Expr> exprs) {
    if (exprs.isEmpty()) return false;
    for (Expr expr : exprs) {
      if (!expr.getType().isScalarType() || expr.getType().isNull()) return false;
    }
    return true;
  }

  /**
   * Returns true if the rows of 'child' already have the layout of a CTE consumer's
   * tuple, which is created from 'rowType'. That's the case if the child outputs the
   * slots of a single tuple in order, with the types and nullability that the consumer
   * tuple will have, in which case the tuples have the same layout.
   */
  private static boolean hasConsumerLayout(
      Analyzer analyzer, NodeWithExprs child, RelDataType rowType)
      throws ImpalaException {
    List<TupleId> tupleIds = child.planNode_.getTupleIds();
    if (tupleIds.size() != 1) return false;
    TupleDescriptor tuple = analyzer.getDescTbl().getTupleDesc(tupleIds.get(0));
    List<SlotDescriptor> slots = tuple.getSlots();
    List<RelDataTypeField> fields = rowType.getFieldList();
    if (slots.size() != fields.size() || child.outputExprs_.size() != fields.size()) {
      return false;
    }
    for (int i = 0; i < slots.size(); ++i) {
      SlotDescriptor slot = slots.get(i);
      Expr expr = child.outputExprs_.get(i);
      if (!(expr instanceof SlotRef) || ((SlotRef) expr).getDesc() != slot) return false;
      if (!slot.isMaterialized()) return false;
      RelDataType fieldType = fields.get(i).getType();
      if (!slot.getType().equals(ImpalaTypeConverter.createImpalaType(fieldType))) {
        return false;
      }
      if (slot.getIsNullable() != fieldType.isNullable()) return false;
    }
    return true;
  }

  @Override
  public RelNodeType relNodeType() {
    return RelNodeType.CTEPRODUCER;
  }

  public String getName() {
    return name_;
  }
}
