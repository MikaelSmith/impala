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

#include "exec/cte-producer-node.h"
#include "codegen/llvm-codegen.h"
#include "exec/exec-node-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/local-exchanger.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple.h"
#include "util/runtime-profile-counters.h"
#include "util/runtime-profile.h"

#include "common/names.h"

namespace impala {

Status CTEProducerPlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  RETURN_IF_ERROR(PlanNode::Init(tnode, state));
  DCHECK(tnode.__isset.cte_producer);
  // Register the exchanger here while plan setup is single-threaded.
  for (const TPlanFragmentInstanceCtx* instance_ctx : state->instance_ctxs()) {
    DCHECK_GT(instance_ctx->num_cte_consumers, 0);
    unique_ptr<LocalExchanger> exchanger(
        new LocalExchanger(instance_ctx->num_cte_consumers));
    string exchange_instance = GetCTEName(instance_ctx->per_fragment_instance_idx);
    VLOG_QUERY << "Registering CTE exchange " << exchange_instance << " with "
              << instance_ctx->num_cte_consumers << " consumers.";
    state->query_state()->RegisterExchanger(exchange_instance, exchanger.get());
    exchangers_.emplace(instance_ctx->per_fragment_instance_idx, std::move(exchanger));
  }
  const TCTEProducer& cte_node = tnode.cte_producer;
  if (cte_node.__isset.output_exprs) {
    DCHECK_EQ(row_descriptor_->tuple_descriptors().size(), 1);
    RETURN_IF_ERROR(ScalarExpr::Create(cte_node.output_exprs,
        *children_[0]->row_descriptor_, state, &output_exprs_));
    DCHECK_EQ(output_exprs_.size(),
        row_descriptor_->tuple_descriptors()[0]->slots().size());
  }
  return Status::OK();
}

void CTEProducerPlanNode::Close() {
  ScalarExpr::Close(output_exprs_);
  PlanNode::Close();
}

void CTEProducerPlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  PlanNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != nullptr);
  Status status;
  if (output_exprs_.empty()) {
    llvm::Function* add_rows_fn = nullptr;
    status = LocalExchanger::CodegenAddRows(codegen, row_descriptor_, &add_rows_fn);
    if (status.ok()) codegen->AddFunctionToJit(add_rows_fn, &codegend_add_rows_fn_);
  } else {
    // Rows are written by MaterializeRows(), not LocalExchanger::AddRows().
    status = CodegenMaterializeRows(codegen);
  }
  AddCodegenStatus(status);
}

Status CTEProducerPlanNode::CodegenMaterializeRows(LlvmCodeGen* codegen) {
  const TupleDescriptor& tuple_desc = *row_descriptor_->tuple_descriptors()[0];
  llvm::Function* materialize_exprs_fn;
  RETURN_IF_ERROR(Tuple::CodegenMaterializeExprs(codegen, false, tuple_desc,
      output_exprs_, /*use_mem_pool=*/false, &materialize_exprs_fn));
  llvm::Function* materialize_rows_fn =
      codegen->GetFunction(IRFunction::CTE_PRODUCER_MATERIALIZE_ROWS, true);
  DCHECK(materialize_rows_fn != nullptr);
  int replaced = codegen->ReplaceCallSites(materialize_rows_fn, materialize_exprs_fn,
      Tuple::MATERIALIZE_EXPRS_NULL_POOL_SYMBOL);
  DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(materialize_rows_fn);

  llvm::Function* varlen_size_fn;
  llvm::Function* copy_varlen_fn;
  RETURN_IF_ERROR(CodegenVarlenFns(codegen, tuple_desc, &varlen_size_fn,
      &copy_varlen_fn));
  replaced = codegen->ReplaceCallSites(
      materialize_rows_fn, varlen_size_fn, "CTEProducerNode10VarlenSize");
  DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(materialize_rows_fn);
  replaced = codegen->ReplaceCallSites(
      materialize_rows_fn, copy_varlen_fn, "CTEProducerNode10CopyVarlen");
  DCHECK_REPLACE_COUNT(replaced, 1) << LlvmCodeGen::Print(materialize_rows_fn);
  materialize_rows_fn = codegen->FinalizeFunction(materialize_rows_fn);
  if (materialize_rows_fn == nullptr) {
    return Status("Codegen'd CTEProducerNode::MaterializeRows() failed verification");
  }
  codegen->AddFunctionToJit(materialize_rows_fn, &codegend_materialize_rows_fn_);
  return Status::OK();
}

// Codegens versions of CTEProducerNode::VarlenSize() and CopyVarlen() that call
// StringSlotVarlenSize() and CopyStringSlot() with constant arguments for each string
// slot of 'tuple_desc'. For a tuple with one string slot:
//
// define i64 @VarlenSize(ptr %this_ptr, ptr %tuple) {
//   %size0 = call i64 @StringSlotVarlenSize(ptr %tuple, i32 16, i8 1, i32 0)
//   %size = add i64 0, %size0
//   ret i64 %size
// }
//
// define void @CopyVarlen(ptr %this_ptr, ptr %tuple, ptr %dst) {
//   %dst_ptr = alloca ptr
//   store ptr %dst, ptr %dst_ptr
//   call void @CopyStringSlot(ptr %tuple, i32 16, i8 1, i32 0, ptr %dst_ptr)
//   ret void
// }
Status CTEProducerPlanNode::CodegenVarlenFns(LlvmCodeGen* codegen,
    const TupleDescriptor& tuple_desc, llvm::Function** varlen_size_fn,
    llvm::Function** copy_varlen_fn) {
  llvm::LLVMContext& context = codegen->context();
  {
    LlvmCodeGen::FnPrototype prototype(codegen, "VarlenSize", codegen->i64_type());
    prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", codegen->ptr_type()));
    prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", codegen->ptr_type()));
    LlvmBuilder builder(context);
    llvm::Value* args[2];
    *varlen_size_fn = prototype.GeneratePrototype(&builder, args);
    llvm::Value* size = codegen->GetI64Constant(0);
    for (const SlotDescriptor* slot : tuple_desc.string_slots()) {
      llvm::Value* slot_size = codegen->CodegenCallFunction(&builder,
          IRFunction::CTE_PRODUCER_STRING_SLOT_VARLEN_SIZE,
          {args[1], codegen->GetI32Constant(slot->null_indicator_offset().byte_offset),
              codegen->GetI8Constant(slot->null_indicator_offset().bit_mask),
              codegen->GetI32Constant(slot->tuple_offset())},
          "slot_size");
      size = builder.CreateAdd(size, slot_size, "size");
    }
    builder.CreateRet(size);
    *varlen_size_fn = codegen->FinalizeFunction(*varlen_size_fn);
    if (*varlen_size_fn == nullptr) {
      return Status("Codegen'd CTEProducerNode::VarlenSize() failed verification");
    }
  }
  {
    LlvmCodeGen::FnPrototype prototype(codegen, "CopyVarlen", codegen->void_type());
    prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", codegen->ptr_type()));
    prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", codegen->ptr_type()));
    prototype.AddArgument(LlvmCodeGen::NamedVariable("dst", codegen->ptr_type()));
    LlvmBuilder builder(context);
    llvm::Value* args[3];
    *copy_varlen_fn = prototype.GeneratePrototype(&builder, args);
    llvm::Value* dst_ptr = codegen->CreateEntryBlockAlloca(
        *copy_varlen_fn, LlvmCodeGen::NamedVariable("dst_ptr", codegen->ptr_type()));
    builder.CreateStore(args[2], dst_ptr);
    for (const SlotDescriptor* slot : tuple_desc.string_slots()) {
      codegen->CodegenCallFunction(&builder, IRFunction::CTE_PRODUCER_COPY_STRING_SLOT,
          {args[1], codegen->GetI32Constant(slot->null_indicator_offset().byte_offset),
              codegen->GetI8Constant(slot->null_indicator_offset().bit_mask),
              codegen->GetI32Constant(slot->tuple_offset()), dst_ptr},
          "");
    }
    builder.CreateRetVoid();
    *copy_varlen_fn = codegen->FinalizeFunction(*copy_varlen_fn);
    if (*copy_varlen_fn == nullptr) {
      return Status("Codegen'd CTEProducerNode::CopyVarlen() failed verification");
    }
  }
  return Status::OK();
}

Status CTEProducerPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CTEProducerNode(pool, *this, state));
  return Status::OK();
}

CTEProducerNode::CTEProducerNode(
    ObjectPool* pool, const CTEProducerPlanNode& pnode, RuntimeState* state)
  : ExecNode(pool, pnode, state->desc_tbl()),
    output_tuple_desc_(pnode.row_descriptor_->tuple_descriptors()[0]),
    output_exprs_(pnode.output_exprs_),
    codegend_materialize_rows_fn_(pnode.codegend_materialize_rows_fn_),
    codegend_add_rows_fn_(pnode.codegend_add_rows_fn_) {
  int32_t idx = state->instance_ctx().per_fragment_instance_idx;
  exchanger_ = pnode.exchangers_.at(idx).get();
  name_ = pnode.GetCTEName(idx);
}

Status CTEProducerNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  if (!output_exprs_.empty()) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Create(output_exprs_, state, pool_,
        expr_perm_pool(), expr_results_pool(), &output_expr_evals_));
    // Rows are written in the layout of BufferedTupleStream::AddRow(), which has no
    // null indicators for a single non-nullable tuple.
    DCHECK_EQ(row_desc()->tuple_descriptors().size(), 1);
    DCHECK(!row_desc()->IsAnyTupleNullable());
    for (const SlotDescriptor* slot : output_tuple_desc_->string_slots()) {
      string_slots_.emplace_back(slot->null_indicator_offset(), slot->tuple_offset());
    }
    pending_tuple_.resize(output_tuple_desc_->byte_size());
  }
  DCHECK(resource_profile_.__isset.spillable_buffer_size);
  DCHECK(resource_profile_.__isset.max_row_buffer_size);
  RETURN_IF_ERROR(exchanger_->Init(state, runtime_profile(), mem_tracker(),
      buffer_pool_client(), row_desc(), resource_profile_.spillable_buffer_size,
      resource_profile_.max_row_buffer_size, name_));
  return Status::OK();
}

Status CTEProducerNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile()->total_time_counter());
  ScopedOpenEventAdder ea(this);
  Status status = OpenInternal(state);
  VLOG_QUERY << "Closing producer of CTE exchange " << name_;
  // On error, consumers return 'status' instead of eos after draining pushed rows.
  exchanger_->CloseProducer(status);
  return status;
}

Status CTEProducerNode::OpenInternal(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(output_expr_evals_, state));
  RETURN_IF_ERROR(child(0)->Open(state));
  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement. The exchanger uses it to buffer rows.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  DCHECK(!output_exprs_.empty() || row_desc()->Equals(*child(0)->row_desc()));
  RowBatch child_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &child_batch, &eos));
    VLOG_PROGRESS << "Adding " << child_batch.num_rows()
                  << " rows to CTE exchange " << name_ << " in " << label();
    RETURN_IF_ERROR(PushBatch(state, &child_batch));
    IncrementNumRowsReturned(child_batch.num_rows());
    child_batch.Reset();
  } while (!eos);
  return Status::OK();
}

Status CTEProducerNode::PushBatch(RuntimeState* state, RowBatch* child_batch) {
  // The exchanger copies the rows, so the batch can be reused afterwards.
  if (output_exprs_.empty()) {
    return exchanger_->Push(child_batch, codegend_add_rows_fn_.load());
  }
  DCHECK(!has_pending_row_);
  LocalExchanger::AddRowsFn fn = codegend_materialize_rows_fn_.load();
  if (fn == nullptr) fn = &CTEProducerNode::MaterializeRows;
  RETURN_IF_ERROR(exchanger_->Push(child_batch, fn, this));
  DCHECK(!has_pending_row_);
  return Status::OK();
}

bool CTEProducerNode::AddRowSlow(
    BufferedTupleStream* stream, TupleRow* row, Status* status) noexcept {
  Tuple* tuple = reinterpret_cast<Tuple*>(pending_tuple_.data());
  memset(tuple, 0, pending_tuple_.size());
  tuple->MaterializeExprs<false, true>(row, *output_tuple_desc_, output_expr_evals_,
      nullptr);
  has_pending_row_ = true;
  return AddPendingRow(stream, status);
}

void CTEProducerNode::SavePendingRow(const Tuple* tuple) noexcept {
  DCHECK(!has_pending_row_);
  memcpy(pending_tuple_.data(), tuple, pending_tuple_.size());
  has_pending_row_ = true;
}

bool CTEProducerNode::AddPendingRow(
    BufferedTupleStream* stream, Status* status) noexcept {
  DCHECK(has_pending_row_);
  const int64_t fixed_size = pending_tuple_.size();
  Tuple* pending = reinterpret_cast<Tuple*>(pending_tuple_.data());
  const int64_t size = fixed_size + VarlenSize(pending);
  // Allocates a new (possibly large) page if needed and possible.
  uint8_t* data = stream->AddRowCustomBegin(size, status);
  if (data == nullptr) return false;
  memcpy(data, pending, fixed_size);
  CopyVarlen(reinterpret_cast<Tuple*>(data), data + fixed_size);
  stream->AddRowCustomEnd(size);
  has_pending_row_ = false;
  return true;
}

Status CTEProducerNode::GetNext(
    RuntimeState* state, RowBatch* output_row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  while (!exchanger_->ReadFinished(/*timeout_ms=*/ 10)) {
    // Wait for exchanger to finish, handling cancellation and query maintenance.
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
  }
  *eos = true;
  return Status::OK();
}

Status CTEProducerNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  // Reset() is not supported.
  const char* msg = "Internal error: CTE producer nodes should not appear in subplans.";
  DCHECK(false) << msg;
  return Status(msg);
}

void CTEProducerNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  VLOG_QUERY << "Releasing CTE exchange " << name_;
  exchanger_->Release();
  ScalarExprEvaluator::Close(output_expr_evals_, state);
  ExecNode::Close(state);
}

void CTEProducerNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ') << "CTEProducerNode(" << name_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
