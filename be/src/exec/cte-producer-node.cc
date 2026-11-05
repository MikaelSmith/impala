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
#include "exec/exec-node-util.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/local-exchanger.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
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
  return Status::OK();
}

Status CTEProducerPlanNode::CreateExecNode(RuntimeState* state, ExecNode** node) const {
  ObjectPool* pool = state->obj_pool();
  *node = pool->Add(new CTEProducerNode(pool, *this, state));
  return Status::OK();
}

CTEProducerNode::CTEProducerNode(
    ObjectPool* pool, const CTEProducerPlanNode& pnode, RuntimeState* state)
  : ExecNode(pool, pnode, state->desc_tbl()) {
  int32_t idx = state->instance_ctx().per_fragment_instance_idx;
  exchanger_ = pnode.exchangers_.at(idx).get();
  name_ = pnode.GetCTEName(idx);
}

Status CTEProducerNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
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
  RETURN_IF_ERROR(child(0)->Open(state));
  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement. The exchanger uses it to buffer rows.
  if (!buffer_pool_client()->is_registered()) {
    RETURN_IF_ERROR(ClaimBufferReservation(state));
  }

  DCHECK(row_desc()->Equals(*child(0)->row_desc()));
  RowBatch child_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  do {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &child_batch, &eos));
    VLOG_PROGRESS << "Adding " << child_batch.num_rows()
                  << " rows to CTE exchange " << name_ << " in " << label();
    // The exchanger copies the rows, so the batch can be reused.
    RETURN_IF_ERROR(exchanger_->Push(&child_batch));
    IncrementNumRowsReturned(child_batch.num_rows());
    child_batch.Reset();
  } while (!eos);
  return Status::OK();
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
  ExecNode::Close(state);
}

void CTEProducerNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ') << "CTEProducerNode(" << name_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

}
