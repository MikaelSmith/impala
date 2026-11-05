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

#pragma once

#include "codegen/codegen-fn-ptr.h"
#include "exec/exec-node.h"
#include "runtime/local-exchanger.h"

namespace impala {

class CTEProducerNode;
class LocalExchanger;
class ScalarExpr;
class ScalarExprEvaluator;

class CTEProducerPlanNode : public PlanNode {
 public:
  Status Init(const TPlanNode& tnode, FragmentState* state) override;
  void Close() override;
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  void Codegen(FragmentState* state) override;

  /// Codegens CTEProducerNode::MaterializeBatch() for 'output_exprs_'.
  Status CodegenMaterializeBatch(LlvmCodeGen* codegen);
  ~CTEProducerPlanNode() override = default;

  string GetCTEName(int32_t index) const {
    return tnode_->cte_producer.name + "_" + std::to_string(index);
  }

  std::map<int32_t, std::unique_ptr<LocalExchanger>> exchangers_;

  /// Codegen'd LocalExchanger::AddRows(), specialized for this node's row layout.
  CodegenFnPtr<LocalExchanger::AddRowsFn> codegend_add_rows_fn_;

  /// Exprs materialized into the output tuple, evaluated over the child's rows. Empty if
  /// the child's rows are buffered as-is.
  std::vector<ScalarExpr*> output_exprs_;

  typedef void (*MaterializeBatchFn)(CTEProducerNode*, RowBatch*, RowBatch*, uint8_t**);
  /// Codegen'd CTEProducerNode::MaterializeBatch(). Only used if 'output_exprs_' is set.
  CodegenFnPtr<MaterializeBatchFn> codegend_materialize_batch_fn_;
};

/// Node that buffers results produced by a Common Table Expression into a
/// LocalExchanger. It's expected to be the root of a fragment; on Open it
/// accumulates all results from its children; GetNext() waits for all consumers
/// (executing in other fragments) to consume the results, then returns eos.
///
/// The LocalExchanger copies rows into buffer pool pages using this node's buffer pool
/// client and reservation, and spills them to scratch when the reservation cannot be
/// increased. Because rows are copied, the exchanger never references memory of the
/// child subtree (e.g. hash tables of a separate join build fragment).
class CTEProducerNode : public ExecNode {
 public:
  CTEProducerNode(
      ObjectPool* pool, const CTEProducerPlanNode& pnode, RuntimeState* state);

  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;
  void DebugString(int indentation_level, std::stringstream* out) const override;

  /// Evaluates 'output_exprs_' over the rows of 'input_batch' and materializes the
  /// results into 'tuple_buf', which is attached to 'output_batch'. Var-len data is not
  /// copied: it references 'input_batch' or expr results, which remain valid until the
  /// rows are pushed into the exchanger. Cross-compiled.
  void MaterializeBatch(RowBatch* input_batch, RowBatch* output_batch,
      uint8_t** tuple_buf);

 private:
  /// Pushes all rows from the child into 'exchanger_'.
  Status OpenInternal(RuntimeState* state);

  /// Materializes the rows of 'child_batch' into 'output_batch_' if needed and pushes
  /// them into 'exchanger_'.
  Status PushBatch(RuntimeState* state, RowBatch* child_batch);

  string name_;
  LocalExchanger* exchanger_;
  const TupleDescriptor* output_tuple_desc_;
  const std::vector<ScalarExpr*>& output_exprs_;
  std::vector<ScalarExprEvaluator*> output_expr_evals_;
  const CodegenFnPtr<CTEProducerPlanNode::MaterializeBatchFn>&
      codegend_materialize_batch_fn_;
  /// Batch the projected rows are materialized into. Only used if 'output_exprs_' is
  /// set.
  std::unique_ptr<RowBatch> output_batch_;
  const CodegenFnPtr<LocalExchanger::AddRowsFn>& codegend_add_rows_fn_;
};

}
