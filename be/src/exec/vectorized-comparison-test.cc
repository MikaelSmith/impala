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

#include <cmath>
#include <cstring>
#include <limits>

#include "exec/vectorized-comparison.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

// Tuples have a null indicator byte at offset 0 and the slot at offset 8.
constexpr int TUPLE_SIZE = 16;
constexpr int SLOT_OFFSET = 8;
constexpr int NULL_BIT = 3;

template <typename T>
bool Reference(VectorizedComparison::Op op, T v, T c) {
  switch (op) {
    case VectorizedComparison::EQ: return v == c;
    case VectorizedComparison::NE: return v != c;
    case VectorizedComparison::LT: return v < c;
    case VectorizedComparison::LE: return v <= c;
    case VectorizedComparison::GT: return v > c;
    case VectorizedComparison::GE: return v >= c;
  }
  return false;
}

template <typename T>
void TestType(PrimitiveType type, const vector<T>& values, T constant) {
  const int n = values.size();
  vector<uint8_t> tuples(n * TUPLE_SIZE, 0);
  for (int i = 0; i < n; ++i) {
    memcpy(&tuples[i * TUPLE_SIZE + SLOT_OFFSET], &values[i], sizeof(T));
    // Every third tuple is NULL.
    if (i % 3 == 2) tuples[i * TUPLE_SIZE] = 1 << NULL_BIT;
  }
  int64_t int_constant = std::is_integral<T>::value ? static_cast<int64_t>(constant) : 0;
  double float_constant = std::is_integral<T>::value ? 0 : static_cast<double>(constant);
  for (int op_idx = VectorizedComparison::EQ; op_idx <= VectorizedComparison::GE;
       ++op_idx) {
    auto op = static_cast<VectorizedComparison::Op>(op_idx);
    for (bool nullable : {false, true}) {
      NullIndicatorOffset null_offset = nullable ? NullIndicatorOffset(0, NULL_BIT)
                                                 : NullIndicatorOffset();
      VectorizedComparison cmp(
          type, op, SLOT_OFFSET, null_offset, int_constant, float_constant);
      // Start with some rows deselected to verify results are ANDed.
      std::unique_ptr<bool[]> selected(new bool[n]);
      for (int i = 0; i < n; ++i) selected[i] = i % 5 != 4;
      cmp.Eval(tuples.data(), TUPLE_SIZE, n, selected.get());
      for (int i = 0; i < n; ++i) {
        bool expected = i % 5 != 4 && Reference(op, values[i], constant)
            && !(nullable && i % 3 == 2);
        EXPECT_EQ(expected, selected[i]) << "type=" << TypeToString(type)
            << " op=" << op_idx << " nullable=" << nullable << " row=" << i;
      }
    }
  }
}

TEST(VectorizedComparisonTest, Integers) {
  TestType<int8_t>(TYPE_TINYINT, {-128, -1, 0, 1, 5, 6, 127, 5, 4, 5}, 5);
  TestType<int16_t>(TYPE_SMALLINT, {-32768, -7, 0, 7, 32767, -7, 8, 6}, -7);
  TestType<int32_t>(TYPE_INT,
      {std::numeric_limits<int32_t>::min(), -1, 0, 100, 99, 101, 100,
          std::numeric_limits<int32_t>::max()}, 100);
  TestType<int32_t>(TYPE_DATE, {-719162, 0, 18000, 18001, 17999, 18000, 2932896}, 18000);
  TestType<int64_t>(TYPE_BIGINT,
      {std::numeric_limits<int64_t>::min(), -1, 0, 1L << 40, (1L << 40) + 1,
          (1L << 40) - 1, std::numeric_limits<int64_t>::max()}, 1L << 40);
}

TEST(VectorizedComparisonTest, FloatingPoint) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  TestType<double>(TYPE_DOUBLE, {-inf, -1.5, 0.0, -0.0, 2.5, nan, 2.5, inf, 2.4}, 2.5);
  TestType<double>(TYPE_DOUBLE, {-inf, -1.5, 0.0, nan, 2.5, inf}, nan);
  TestType<float>(TYPE_FLOAT, {-1.5f, 0.0f, 2.5f, std::nanf(""), 2.5f, 3.0f}, 2.5f);
}

// Exercises batch sizes that are not a multiple of typical vector widths.
TEST(VectorizedComparisonTest, LargeBatch) {
  vector<int64_t> values;
  for (int i = 0; i < 1027; ++i) values.push_back((i * 7919) % 1000 - 500);
  TestType<int64_t>(TYPE_BIGINT, values, 17);
}

}
