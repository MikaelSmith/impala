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

#include <cstring>
#include <iostream>
#include <random>
#include <vector>

#include "exec/vectorized-comparison.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

// Models dictionary-decoding a 1024-row batch of a nullable INT or BIGINT column and
// filtering it with 'col < c' (and 'col >= lo AND col < c' for two conjuncts):
//  - row-at-a-time: decode into the tuples, then the codegen'd ProcessScratchBatch
//    loop before vectorization, with the comparison inlined and a branch per row.
//  - vectorized: decode into the tuples, VectorizedComparison::Eval() on the tuples
//    per conjunct, then the row loop that only consumes 'selected_rows'.
//  - columnar, scatter all: decode into a contiguous column buffer,
//    VectorizedComparison::EvalColumn(), copy all values and null bits into the
//    tuples, then the row loop.
//  - columnar, scatter selected: as above, but the row loop only copies the values of
//    selected rows into their tuples.
//  - columnar, selection vector: as above, but builds a vector of selected row indexes
//    without branches and then only touches the selected rows.
// All include the output-row compaction done by ProcessScratchBatch.
//
// Release build, aarch64 VM, GCC 15, throughput relative to row-at-a-time (INT/BIGINT):
// conjuncts  sel   vectorized  scatter all  scatter selected  selection vector
//         1  0.01  0.75/0.75   0.71/0.67    0.98/0.94         1.33/1.26
//         1  0.50  1.03/1.00   0.98/0.89    1.16/1.07         1.32/1.20
//         1  0.99  0.91/0.90   0.85/0.81    1.20/1.15         0.93/0.87
//         2  0.01  0.65/0.66   0.69/0.64    0.94/0.88         1.26/1.15
//         2  0.50  0.89/0.87   0.95/0.85    1.11/1.01         1.27/1.12
//         2  0.99  0.78/0.79   0.83/0.77    1.15/1.07         0.90/0.83

constexpr int BATCH_SIZE = 1024;
constexpr int DICT_SIZE = 1000;
constexpr int TUPLE_SIZE = 24;
constexpr int SLOT_OFFSET = 8;
constexpr int NULL_BIT = 0;

template <typename T>
struct TestData {
  vector<T> dict;
  vector<uint32_t> dict_idx;
  vector<uint8_t> tuples;
  vector<T> column;
  vector<uint8_t> is_null;
  vector<VectorizedComparison> comparisons;
  T lo;
  T hi;
  int num_conjuncts;
  bool selected[BATCH_SIZE];
  uint8_t* output[BATCH_SIZE];
  int64_t sink = 0;

  TestData(PrimitiveType type, double selectivity, int num_conjuncts)
    : num_conjuncts(num_conjuncts) {
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<T> dist(0, 999999);
    for (int i = 0; i < DICT_SIZE; ++i) dict.push_back(dist(rng));
    tuples.resize(BATCH_SIZE * TUPLE_SIZE, 0);
    column.resize(BATCH_SIZE);
    for (int i = 0; i < BATCH_SIZE; ++i) {
      dict_idx.push_back(rng() % DICT_SIZE);
      // 1% NULLs.
      is_null.push_back(rng() % 100 == 0);
    }
    lo = num_conjuncts == 1 ? 0 : 1;
    hi = lo + static_cast<T>(selectivity * 1000000);
    NullIndicatorOffset null_offset(0, NULL_BIT);
    comparisons.emplace_back(
        type, VectorizedComparison::LT, SLOT_OFFSET, null_offset, hi, 0);
    if (num_conjuncts == 2) {
      comparisons.emplace_back(
          type, VectorizedComparison::GE, SLOT_OFFSET, null_offset, lo, 0);
    }
  }

  void DecodeIntoTuples() {
    uint8_t* tuple = tuples.data();
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      memcpy(tuple + SLOT_OFFSET, &dict[dict_idx[i]], sizeof(T));
      tuple[0] = is_null[i] << NULL_BIT;
    }
  }

  void DecodeIntoColumn() {
    for (int i = 0; i < BATCH_SIZE; ++i) column[i] = dict[dict_idx[i]];
  }

  void CompactSelected() {
    uint8_t* tuple = tuples.data();
    uint8_t** out = output;
    bool* is_selected = selected;
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      *out = tuple;
      if (!*is_selected++) continue;
      ++out;
    }
    sink += out - output;
  }
};

template <typename T>
void RowAtATime(int iters, void* data) {
  TestData<T>* d = reinterpret_cast<TestData<T>*>(data);
  const T lo = d->lo;
  const T hi = d->hi;
  const bool two_conjuncts = d->num_conjuncts == 2;
  for (int it = 0; it < iters; ++it) {
    d->DecodeIntoTuples();
    uint8_t* tuple = d->tuples.data();
    uint8_t** out = d->output;
    bool* is_selected = d->selected;
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      *out = tuple;
      T v;
      memcpy(&v, tuple + SLOT_OFFSET, sizeof(v));
      bool is_null = (tuple[0] & (1 << NULL_BIT)) != 0;
      if (is_null || !(v < hi) || (two_conjuncts && !(v >= lo))) {
        *is_selected++ = false;
        continue;
      }
      *is_selected++ = true;
      ++out;
    }
    d->sink += out - d->output;
  }
}

template <typename T>
void Vectorized(int iters, void* data) {
  TestData<T>* d = reinterpret_cast<TestData<T>*>(data);
  for (int it = 0; it < iters; ++it) {
    d->DecodeIntoTuples();
    memset(d->selected, true, BATCH_SIZE);
    for (const VectorizedComparison& cmp : d->comparisons) {
      cmp.Eval(d->tuples.data(), TUPLE_SIZE, BATCH_SIZE, d->selected);
    }
    d->CompactSelected();
  }
}

template <typename T>
void EvalColumn(TestData<T>* d) {
  d->DecodeIntoColumn();
  memset(d->selected, true, BATCH_SIZE);
  for (const VectorizedComparison& cmp : d->comparisons) {
    cmp.EvalColumn(reinterpret_cast<const uint8_t*>(d->column.data()),
        d->is_null.data(), BATCH_SIZE, d->selected);
  }
}

template <typename T>
void ColumnarScatterAll(int iters, void* data) {
  TestData<T>* d = reinterpret_cast<TestData<T>*>(data);
  for (int it = 0; it < iters; ++it) {
    EvalColumn(d);
    uint8_t* tuple = d->tuples.data();
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      memcpy(tuple + SLOT_OFFSET, &d->column[i], sizeof(T));
      tuple[0] = d->is_null[i] << NULL_BIT;
    }
    d->CompactSelected();
  }
}

template <typename T>
void ColumnarScatterSelected(int iters, void* data) {
  TestData<T>* d = reinterpret_cast<TestData<T>*>(data);
  for (int it = 0; it < iters; ++it) {
    EvalColumn(d);
    uint8_t* tuple = d->tuples.data();
    uint8_t** out = d->output;
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      *out = tuple;
      if (!d->selected[i]) continue;
      // A selected row passed a comparison on this slot, so it is not NULL.
      memcpy(tuple + SLOT_OFFSET, &d->column[i], sizeof(T));
      tuple[0] = 0;
      ++out;
    }
    d->sink += out - d->output;
  }
}

template <typename T>
void ColumnarSelectionVector(int iters, void* data) {
  TestData<T>* d = reinterpret_cast<TestData<T>*>(data);
  uint16_t sel_vector[BATCH_SIZE];
  for (int it = 0; it < iters; ++it) {
    EvalColumn(d);
    int num_selected = 0;
    for (int i = 0; i < BATCH_SIZE; ++i) {
      sel_vector[num_selected] = i;
      num_selected += d->selected[i];
    }
    for (int k = 0; k < num_selected; ++k) {
      int i = sel_vector[k];
      uint8_t* tuple = d->tuples.data() + i * TUPLE_SIZE;
      memcpy(tuple + SLOT_OFFSET, &d->column[i], sizeof(T));
      tuple[0] = 0;
      d->output[k] = tuple;
    }
    d->sink += num_selected;
  }
}

template <typename T>
void RunSuites(PrimitiveType type) {
  char name[120];
  for (int num_conjuncts : {1, 2}) {
    for (double selectivity : {0.01, 0.5, 0.99}) {
      snprintf(name, sizeof(name), "%s %d conjunct(s), sel %.2f",
          TypeToString(type).c_str(), num_conjuncts, selectivity);
      Benchmark suite(name);
      TestData<T>* d = new TestData<T>(type, selectivity, num_conjuncts);
      suite.AddBenchmark("row-at-a-time", RowAtATime<T>, d);
      suite.AddBenchmark("vectorized", Vectorized<T>, d);
      suite.AddBenchmark("columnar, scatter all", ColumnarScatterAll<T>, d);
      suite.AddBenchmark("columnar, scatter selected", ColumnarScatterSelected<T>, d);
      suite.AddBenchmark("columnar, selection vector", ColumnarSelectionVector<T>, d);
      cout << suite.Measure() << endl;
    }
  }
}

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;
  RunSuites<int32_t>(TYPE_INT);
  RunSuites<int64_t>(TYPE_BIGINT);
  return 0;
}
