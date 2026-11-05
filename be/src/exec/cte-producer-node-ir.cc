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
#include "runtime/buffered-tuple-stream.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"

using namespace impala;

int64_t CTEProducerNode::StringSlotVarlenSize(const Tuple* tuple, int null_byte_offset,
    uint8_t null_bit_mask, int tuple_offset) {
  NullIndicatorOffset null_indicator_offset;
  null_indicator_offset.byte_offset = null_byte_offset;
  null_indicator_offset.bit_mask = null_bit_mask;
  if (tuple->IsNull(null_indicator_offset)) return 0;
  const StringValue* sv = tuple->GetStringSlot(tuple_offset);
  return sv->IsSmall() ? 0 : sv->Len();
}

void CTEProducerNode::CopyStringSlot(Tuple* tuple, int null_byte_offset,
    uint8_t null_bit_mask, int tuple_offset, uint8_t** dst) {
  NullIndicatorOffset null_indicator_offset;
  null_indicator_offset.byte_offset = null_byte_offset;
  null_indicator_offset.bit_mask = null_bit_mask;
  if (tuple->IsNull(null_indicator_offset)) return;
  StringValue* sv = tuple->GetStringSlot(tuple_offset);
  if (sv->IsSmall()) return;
  const int len = sv->Len();
  if (len > 0) memcpy(*dst, sv->Ptr(), len);
  // Also for empty strings, so that the row is identical to one added with AddRow().
  sv->SetPtr(reinterpret_cast<char*>(*dst));
  *dst += len;
}

int64_t CTEProducerNode::VarlenSize(const Tuple* tuple) const noexcept {
  int64_t size = 0;
  for (const std::pair<NullIndicatorOffset, int>& slot : string_slots_) {
    size += StringSlotVarlenSize(
        tuple, slot.first.byte_offset, slot.first.bit_mask, slot.second);
  }
  return size;
}

void CTEProducerNode::CopyVarlen(Tuple* tuple, uint8_t* dst) const noexcept {
  for (const std::pair<NullIndicatorOffset, int>& slot : string_slots_) {
    CopyStringSlot(tuple, slot.first.byte_offset, slot.first.bit_mask, slot.second, &dst);
  }
}

int CTEProducerNode::MaterializeRows(void* ctx, BufferedTupleStream* stream,
    RowBatch* batch, int start_row, Status* status) {
  CTEProducerNode* node = reinterpret_cast<CTEProducerNode*>(ctx);
  const TupleDescriptor& tuple_desc = *node->output_tuple_desc_;
  const int64_t fixed_size = tuple_desc.byte_size();
  const int num_rows = batch->num_rows();
  int i = start_row;
  // A row that didn't fit last time was already materialized.
  if (node->has_pending_row_ && i < num_rows) {
    if (!node->AddPendingRow(stream, status)) return i;
    ++i;
  }
  for (; i < num_rows; ++i) {
    TupleRow* row = batch->GetRow(i);
    int64_t buf_len;
    uint8_t* buf = stream->GetWriteBuffer(&buf_len);
    if (UNLIKELY(buf_len < fixed_size)) {
      // No write page, or the fixed-length part doesn't fit.
      if (!node->AddRowSlow(stream, row, status)) return i;
      continue;
    }
    // Materialize the tuple directly into the page. Var-len data still references the
    // child's batch or expr results; it is copied after the tuple below.
    Tuple* tuple = reinterpret_cast<Tuple*>(buf);
    memset(tuple, 0, fixed_size);
    tuple->MaterializeExprs<false, true>(row, tuple_desc, node->output_expr_evals_,
        nullptr);
    const int64_t size = fixed_size + node->VarlenSize(tuple);
    if (UNLIKELY(size > buf_len)) {
      // The var-len data doesn't fit in the page. Add the row to another page without
      // evaluating the exprs again.
      node->SavePendingRow(tuple);
      if (!node->AddPendingRow(stream, status)) return i;
      continue;
    }
    node->CopyVarlen(tuple, buf + fixed_size);
    stream->AddInPlaceRow(size);
  }
  return i;
}
