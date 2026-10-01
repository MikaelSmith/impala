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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace impala {

class RuntimeState;
class ScalarExpr;
class ScalarExprEvaluator;

/// A conjunct of the form '<slot> <op> <literal>' on a numeric or DATE slot of the
/// first tuple in a row. It is evaluated over a batch of tuples with a branch-free loop
/// instead of row-at-a-time expression evaluation.
class VectorizedComparison {
 public:
  enum Op { EQ, NE, LT, LE, GT, GE };

  VectorizedComparison(PrimitiveType type, Op op, int slot_offset,
      NullIndicatorOffset null_offset, int64_t int_constant, double float_constant);

  /// Returns true if 'conjunct' has a shape supported by this class. Depends only on
  /// the expr tree, so codegen and the interpreted path make the same decision.
  static bool IsSupported(const ScalarExpr& conjunct);

  /// Creates a comparison from the open evaluator 'eval', whose root expr must satisfy
  /// IsSupported(), and appends it to 'comparisons'.
  static Status Create(RuntimeState* state, ScalarExprEvaluator* eval,
      std::vector<VectorizedComparison>* comparisons) WARN_UNUSED_RESULT;

  /// For each of the 'num_tuples' tuples of 'tuple_size' bytes starting at 'tuple_mem',
  /// clears 'selected[i]' if the comparison is false or NULL for tuple i.
  void Eval(const uint8_t* tuple_mem, int tuple_size, int num_tuples,
      bool* selected) const;

  /// Same as Eval() for 'num_values' contiguous values of the slot type at 'values',
  /// which must be aligned to the type. 'is_null' has one byte per value, or is
  /// nullptr if no value is NULL.
  void EvalColumn(const uint8_t* values, const uint8_t* is_null, int num_values,
      bool* selected) const;

  int slot_offset() const { return slot_offset_; }

 private:
  /// Calls 'fn(constant, cmp)' with the constant converted to the slot's C++ type and
  /// the std comparison functor for 'op_'.
  template <typename Fn>
  void Dispatch(Fn&& fn) const;
  template <typename T, typename Fn>
  void DispatchOp(T constant, Fn&& fn) const;

  PrimitiveType type_;
  Op op_;
  int slot_offset_;
  NullIndicatorOffset null_offset_;
  int64_t int_constant_;
  double float_constant_;
};

}
