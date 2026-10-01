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

#include "exec/vectorized-comparison.h"

#include <cstring>
#include <functional>

#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "udf/udf.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

namespace {

bool OpFromName(const string& name, VectorizedComparison::Op* op) {
  if (name == "eq") {
    *op = VectorizedComparison::EQ;
  } else if (name == "ne") {
    *op = VectorizedComparison::NE;
  } else if (name == "lt") {
    *op = VectorizedComparison::LT;
  } else if (name == "le") {
    *op = VectorizedComparison::LE;
  } else if (name == "gt") {
    *op = VectorizedComparison::GT;
  } else if (name == "ge") {
    *op = VectorizedComparison::GE;
  } else {
    return false;
  }
  return true;
}

/// Returns the op with swapped operands, i.e. 'a op b' == 'b Mirror(op) a'.
VectorizedComparison::Op Mirror(VectorizedComparison::Op op) {
  switch (op) {
    case VectorizedComparison::LT: return VectorizedComparison::GT;
    case VectorizedComparison::LE: return VectorizedComparison::GE;
    case VectorizedComparison::GT: return VectorizedComparison::LT;
    case VectorizedComparison::GE: return VectorizedComparison::LE;
    default: return op;
  }
}

bool IsSupportedType(PrimitiveType type) {
  switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DATE:
      return true;
    default:
      return false;
  }
}

/// Same operators as the builtin comparison functions in operators-ir.cc so that
/// results, including for NaN, are identical.
template <typename T, typename Cmp, bool NULLABLE>
void EvalKernel(const uint8_t* __restrict__ tuple_mem, int tuple_size, int num_tuples,
    int slot_offset, NullIndicatorOffset null_offset, T constant,
    bool* __restrict__ selected) {
  Cmp cmp;
  const uint8_t* slot = tuple_mem + slot_offset;
  const uint8_t* null_byte = tuple_mem + null_offset.byte_offset;
  for (int i = 0; i < num_tuples; ++i) {
    T val;
    memcpy(&val, slot, sizeof(T));
    bool pass = cmp(val, constant);
    if (NULLABLE) pass &= (*null_byte & null_offset.bit_mask) == 0;
    selected[i] &= pass;
    slot += tuple_size;
    null_byte += tuple_size;
  }
}

template <typename T, typename Cmp>
void EvalKernel(const uint8_t* tuple_mem, int tuple_size, int num_tuples,
    int slot_offset, NullIndicatorOffset null_offset, T constant, bool* selected) {
  if (null_offset.bit_mask == 0) {
    EvalKernel<T, Cmp, false>(
        tuple_mem, tuple_size, num_tuples, slot_offset, null_offset, constant, selected);
  } else {
    EvalKernel<T, Cmp, true>(
        tuple_mem, tuple_size, num_tuples, slot_offset, null_offset, constant, selected);
  }
}

} // anonymous namespace

VectorizedComparison::VectorizedComparison(PrimitiveType type, Op op, int slot_offset,
    NullIndicatorOffset null_offset, int64_t int_constant, double float_constant)
  : type_(type),
    op_(op),
    slot_offset_(slot_offset),
    null_offset_(null_offset),
    int_constant_(int_constant),
    float_constant_(float_constant) {
  DCHECK(IsSupportedType(type));
}

bool VectorizedComparison::IsSupported(const ScalarExpr& conjunct) {
  const ScalarFnCall* fn_call = dynamic_cast<const ScalarFnCall*>(&conjunct);
  if (fn_call == nullptr || !fn_call->IsBuiltin()) return false;
  Op op = EQ;
  if (!OpFromName(conjunct.function_name(), &op)) return false;
  if (conjunct.GetNumChildren() != 2) return false;
  const ScalarExpr* left = conjunct.GetChild(0);
  const ScalarExpr* right = conjunct.GetChild(1);
  if (left->IsLiteral()) std::swap(left, right);
  if (!left->IsSlotRef() || !right->IsLiteral()) return false;
  if (static_cast<const SlotRef*>(left)->GetTupleIdx() != 0) return false;
  return left->type() == right->type() && IsSupportedType(left->type().type);
}

Status VectorizedComparison::Create(RuntimeState* state, ScalarExprEvaluator* eval,
    vector<VectorizedComparison>* comparisons) {
  const ScalarExpr& conjunct = eval->root();
  DCHECK(IsSupported(conjunct));
  Op op = EQ;
  OpFromName(conjunct.function_name(), &op);
  const ScalarExpr* left = conjunct.GetChild(0);
  const ScalarExpr* right = conjunct.GetChild(1);
  if (left->IsLiteral()) {
    std::swap(left, right);
    op = Mirror(op);
  }
  const SlotRef* slot_ref = static_cast<const SlotRef*>(left);

  AnyVal* const_val;
  RETURN_IF_ERROR(eval->GetConstValue(state, *right, &const_val));
  DCHECK(const_val != nullptr);
  DCHECK(!const_val->is_null);
  int64_t int_constant = 0;
  double float_constant = 0;
  PrimitiveType type = slot_ref->type().type;
  switch (type) {
    case TYPE_TINYINT:
      int_constant = static_cast<TinyIntVal*>(const_val)->val;
      break;
    case TYPE_SMALLINT:
      int_constant = static_cast<SmallIntVal*>(const_val)->val;
      break;
    case TYPE_INT:
      int_constant = static_cast<IntVal*>(const_val)->val;
      break;
    case TYPE_BIGINT:
      int_constant = static_cast<BigIntVal*>(const_val)->val;
      break;
    case TYPE_DATE:
      int_constant = static_cast<DateVal*>(const_val)->val;
      break;
    case TYPE_FLOAT:
      float_constant = static_cast<FloatVal*>(const_val)->val;
      break;
    case TYPE_DOUBLE:
      float_constant = static_cast<DoubleVal*>(const_val)->val;
      break;
    default:
      DCHECK(false) << "Unsupported type " << slot_ref->type();
  }
  comparisons->emplace_back(type, op, slot_ref->GetSlotOffset(),
      slot_ref->GetNullIndicatorOffset(), int_constant, float_constant);
  return Status::OK();
}

template <typename T>
void VectorizedComparison::EvalForType(const uint8_t* tuple_mem, int tuple_size,
    int num_tuples, bool* selected, T constant) const {
  switch (op_) {
    case EQ:
      EvalKernel<T, std::equal_to<T>>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected);
      break;
    case NE:
      EvalKernel<T, std::not_equal_to<T>>(tuple_mem, tuple_size, num_tuples,
          slot_offset_, null_offset_, constant, selected);
      break;
    case LT:
      EvalKernel<T, std::less<T>>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected);
      break;
    case LE:
      EvalKernel<T, std::less_equal<T>>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected);
      break;
    case GT:
      EvalKernel<T, std::greater<T>>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected);
      break;
    case GE:
      EvalKernel<T, std::greater_equal<T>>(tuple_mem, tuple_size, num_tuples,
          slot_offset_, null_offset_, constant, selected);
      break;
  }
}

void VectorizedComparison::Eval(const uint8_t* tuple_mem, int tuple_size,
    int num_tuples, bool* selected) const {
  switch (type_) {
    case TYPE_TINYINT:
      EvalForType<int8_t>(tuple_mem, tuple_size, num_tuples, selected, int_constant_);
      break;
    case TYPE_SMALLINT:
      EvalForType<int16_t>(tuple_mem, tuple_size, num_tuples, selected, int_constant_);
      break;
    case TYPE_INT:
    case TYPE_DATE:
      EvalForType<int32_t>(tuple_mem, tuple_size, num_tuples, selected, int_constant_);
      break;
    case TYPE_BIGINT:
      EvalForType<int64_t>(tuple_mem, tuple_size, num_tuples, selected, int_constant_);
      break;
    case TYPE_FLOAT:
      EvalForType<float>(tuple_mem, tuple_size, num_tuples, selected, float_constant_);
      break;
    case TYPE_DOUBLE:
      EvalForType<double>(tuple_mem, tuple_size, num_tuples, selected, float_constant_);
      break;
    default:
      DCHECK(false) << "Unsupported type " << TypeToString(type_);
  }
}

}
