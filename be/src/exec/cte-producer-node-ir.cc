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
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"

using namespace impala;

void CTEProducerNode::MaterializeBatch(
    RowBatch* input_batch, RowBatch* output_batch, uint8_t** tuple_buf) {
  // Take all references to member variables out of the loop to reduce the number of
  // loads and stores.
  const TupleDescriptor& tuple_desc = *output_tuple_desc_;
  const int tuple_byte_size = tuple_desc.byte_size();
  uint8_t* cur_tuple = *tuple_buf;
  const int num_rows = input_batch->num_rows();
  DCHECK_LE(num_rows, output_batch->capacity() - output_batch->num_rows());
  FOREACH_ROW_LIMIT(input_batch, 0, num_rows, batch_iter) {
    Tuple* dst_tuple = reinterpret_cast<Tuple*>(cur_tuple);
    TupleRow* dst_row = output_batch->GetRow(output_batch->AddRow());
    // Var-len data is not copied: LocalExchanger::Push() copies it into the exchanger.
    dst_tuple->MaterializeExprs<false, true>(
        batch_iter.Get(), tuple_desc, output_expr_evals_, nullptr);
    dst_row->SetTuple(0, dst_tuple);
    output_batch->CommitLastRow();
    cur_tuple += tuple_byte_size;
  }
  *tuple_buf = cur_tuple;
}
