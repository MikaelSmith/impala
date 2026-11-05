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

#include <utility>
#include <vector>

#include "codegen/codegen-fn-ptr.h"
#include "exec/exec-node.h"
#include "runtime/descriptors.h"
#include "runtime/local-exchanger.h"
#include "runtime/string-value.h"
#include "runtime/tuple.h"

namespace impala {

class BufferedTupleStream;
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

  /// Codegens CTEProducerNode::MaterializeRows() for 'output_exprs_'.
  Status CodegenMaterializeRows(LlvmCodeGen* codegen);

  /// Codegens versions of CTEProducerNode::VarlenSize() and CopyVarlen() specialized for
  /// the string slots of 'tuple_desc'.
  static Status CodegenVarlenFns(LlvmCodeGen* codegen, const TupleDescriptor& tuple_desc,
      llvm::Function** varlen_size_fn, llvm::Function** copy_varlen_fn);
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

  /// Codegen'd CTEProducerNode::MaterializeRows(). Only used if 'output_exprs_' is set.
  CodegenFnPtr<LocalExchanger::AddRowsFn> codegend_materialize_rows_fn_;
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

  /// LocalExchanger::AddRowsFn used if 'output_exprs_' is set. 'ctx' is the
  /// CTEProducerNode. Evaluates 'output_exprs_' over the rows of 'batch', the child's
  /// rows, and materializes the results directly into the write page of 'stream'
  /// followed by their string data, laid out like BufferedTupleStream::AddRow() does.
  /// Cross-compiled, so that the materialization is specialized for 'output_exprs_'.
  static int MaterializeRows(void* ctx, BufferedTupleStream* stream, RowBatch* batch,
      int start_row, Status* status);

 private:
  /// Slow path of MaterializeRows() if 'row' may not fit in the write page of 'stream':
  /// materializes it into 'pending_tuple_' and calls AddPendingRow().
  bool IR_NO_INLINE AddRowSlow(
      BufferedTupleStream* stream, TupleRow* row, Status* status) noexcept;

  /// Saves the materialized 'tuple', whose string data was not copied yet, as the
  /// pending row.
  void IR_NO_INLINE SavePendingRow(const Tuple* tuple) noexcept;

  /// Adds the pending row to 'stream' with AddRowCustomBegin(). Returns false, keeping
  /// the row pending, if it cannot be added to the stream, e.g. because the page is full
  /// or there's no reservation. The row is then added by the next MaterializeRows() call
  /// instead of evaluating the exprs again.
  bool IR_NO_INLINE AddPendingRow(BufferedTupleStream* stream, Status* status) noexcept;

  /// Returns the length of the string data of 'tuple' that is stored after the tuple in
  /// the stream: the non-NULL strings that are not small strings. Replaced by a version
  /// with constant slot offsets when MaterializeRows() is codegen'd.
  int64_t IR_NO_INLINE VarlenSize(const Tuple* tuple) const noexcept;

  /// Copies the string data of 'tuple' to 'dst', in slot order, and points the slots at
  /// the copies, as BufferedTupleStream::AddRow() does. Replaced by a version with
  /// constant slot offsets when MaterializeRows() is codegen'd.
  void IR_NO_INLINE CopyVarlen(Tuple* tuple, uint8_t* dst) const noexcept;

 public:
  /// Helpers for VarlenSize() and CopyVarlen() for one string slot, called with constant
  /// arguments by their codegen'd versions.
  static int64_t IR_ALWAYS_INLINE StringSlotVarlenSize(const Tuple* tuple,
      int null_byte_offset, uint8_t null_bit_mask, int tuple_offset);
  static void IR_ALWAYS_INLINE CopyStringSlot(Tuple* tuple, int null_byte_offset,
      uint8_t null_bit_mask, int tuple_offset, uint8_t** dst);

 private:
  /// Pushes all rows from the child into 'exchanger_'.
  Status OpenInternal(RuntimeState* state);

  /// Pushes the rows of 'child_batch' into 'exchanger_', projecting them if needed.
  Status PushBatch(RuntimeState* state, RowBatch* child_batch);

  string name_;
  LocalExchanger* exchanger_;
  const TupleDescriptor* output_tuple_desc_;
  const std::vector<ScalarExpr*>& output_exprs_;
  std::vector<ScalarExprEvaluator*> output_expr_evals_;
  const CodegenFnPtr<LocalExchanger::AddRowsFn>& codegend_materialize_rows_fn_;
  /// Null indicator and tuple offset of each string slot of the output tuple, in slot
  /// order.
  std::vector<std::pair<NullIndicatorOffset, int>> string_slots_;
  /// A materialized output tuple that could not be added to the stream yet. Its strings
  /// reference the child's batch or expr results, which remain valid while the batch is
  /// pushed.
  std::vector<uint8_t> pending_tuple_;
  bool has_pending_row_ = false;
  const CodegenFnPtr<LocalExchanger::AddRowsFn>& codegend_add_rows_fn_;
};

}
