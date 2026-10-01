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

// The baseline x86-64 target has no 64-bit vector compare and only 128-bit vectors.
#if defined(__x86_64__)
#define VECTORIZED_KERNEL __attribute__((target_clones("avx2", "default")))
#else
#define VECTORIZED_KERNEL
#endif

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
/// results, including for NaN, are identical. The kernels compute with uint8_t rather
/// than bool because GCC does not vectorize the bool version.
template <typename T, typename Cmp, bool NULLABLE>
VECTORIZED_KERNEL void EvalKernel(const uint8_t* __restrict__ tuple_mem, int tuple_size, int num_tuples,
    int slot_offset, NullIndicatorOffset null_offset, T constant,
    uint8_t* __restrict__ selected) {
  Cmp cmp;
  const uint8_t* slot = tuple_mem + slot_offset;
  const uint8_t* null_byte = tuple_mem + null_offset.byte_offset;
  for (int i = 0; i < num_tuples; ++i) {
    T val;
    memcpy(&val, slot, sizeof(T));
    uint8_t pass = static_cast<uint8_t>(cmp(val, constant));
    if (NULLABLE) pass &= static_cast<uint8_t>((*null_byte & null_offset.bit_mask) == 0);
    selected[i] &= pass;
    slot += tuple_size;
    null_byte += tuple_size;
  }
}

template <typename T, typename Cmp, bool NULLABLE>
VECTORIZED_KERNEL void EvalColumnKernel(const T* __restrict__ values, const uint8_t* __restrict__ is_null,
    int num_values, T constant, uint8_t* __restrict__ selected) {
  Cmp cmp;
  for (int i = 0; i < num_values; ++i) {
    uint8_t pass = static_cast<uint8_t>(cmp(values[i], constant));
    if (NULLABLE) pass &= static_cast<uint8_t>(is_null[i] == 0);
    selected[i] &= pass;
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

template <typename T, typename Fn>
void VectorizedComparison::DispatchOp(T constant, Fn&& fn) const {
  switch (op_) {
    case EQ: fn(constant, std::equal_to<T>()); break;
    case NE: fn(constant, std::not_equal_to<T>()); break;
    case LT: fn(constant, std::less<T>()); break;
    case LE: fn(constant, std::less_equal<T>()); break;
    case GT: fn(constant, std::greater<T>()); break;
    case GE: fn(constant, std::greater_equal<T>()); break;
  }
}

template <typename Fn>
void VectorizedComparison::Dispatch(Fn&& fn) const {
  switch (type_) {
    case TYPE_TINYINT: DispatchOp(static_cast<int8_t>(int_constant_), fn); break;
    case TYPE_SMALLINT: DispatchOp(static_cast<int16_t>(int_constant_), fn); break;
    case TYPE_INT:
    case TYPE_DATE: DispatchOp(static_cast<int32_t>(int_constant_), fn); break;
    case TYPE_BIGINT: DispatchOp(int_constant_, fn); break;
    case TYPE_FLOAT: DispatchOp(static_cast<float>(float_constant_), fn); break;
    case TYPE_DOUBLE: DispatchOp(float_constant_, fn); break;
    default: DCHECK(false) << "Unsupported type " << TypeToString(type_);
  }
}

void VectorizedComparison::Eval(const uint8_t* tuple_mem, int tuple_size,
    int num_tuples, bool* selected) const {
  static_assert(sizeof(bool) == sizeof(uint8_t));
  uint8_t* selected_bytes = reinterpret_cast<uint8_t*>(selected);
  Dispatch([&](auto constant, auto cmp) {
    using T = decltype(constant);
    using Cmp = decltype(cmp);
    if (null_offset_.bit_mask == 0) {
      EvalKernel<T, Cmp, false>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected_bytes);
    } else {
      EvalKernel<T, Cmp, true>(tuple_mem, tuple_size, num_tuples, slot_offset_,
          null_offset_, constant, selected_bytes);
    }
  });
}

void VectorizedComparison::EvalColumn(const uint8_t* values, const uint8_t* is_null,
    int num_values, bool* selected) const {
  uint8_t* selected_bytes = reinterpret_cast<uint8_t*>(selected);
  Dispatch([&](auto constant, auto cmp) {
    using T = decltype(constant);
    using Cmp = decltype(cmp);
    const T* typed_values = reinterpret_cast<const T*>(values);
    DCHECK_EQ(reinterpret_cast<uintptr_t>(values) % alignof(T), 0);
    if (is_null == nullptr) {
      EvalColumnKernel<T, Cmp, false>(
          typed_values, is_null, num_values, constant, selected_bytes);
    } else {
      EvalColumnKernel<T, Cmp, true>(
          typed_values, is_null, num_values, constant, selected_bytes);
    }
  });
}

}
