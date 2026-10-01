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

// Compares filtering a 1024-tuple scratch batch with 'col < c' (and 'col >= lo AND
// col < c' for two conjuncts) on a nullable BIGINT slot:
//  - row-at-a-time: models codegen'd ProcessScratchBatch before vectorization, with the
//    comparison inlined and an early-exit branch per row.
//  - vectorized: VectorizedComparison::Eval() per conjunct, then the row loop that only
//    consumes 'selected_rows'.
// Both include the output-row compaction done by ProcessScratchBatch.
//
// Release build, aarch64 VM (14 cores), median iters/ms:
// conjuncts  selectivity  row-at-a-time  vectorized  relative
//         1         0.01           2640        1300    0.492X
//         1         0.50           1610        1300    0.812X
//         1         0.99           1820        1300    0.715X
//         2         0.01           2660         940    0.354X
//         2         0.50           1540         949    0.617X
//         2         0.99           1820         940    0.516X

constexpr int BATCH_SIZE = 1024;
constexpr int TUPLE_SIZE = 24;
constexpr int SLOT_OFFSET = 8;
constexpr int NULL_BIT = 0;

struct TestData {
  vector<uint8_t> tuples;
  vector<VectorizedComparison> comparisons;
  int64_t lo;
  int64_t hi;
  int num_conjuncts;
  bool selected[BATCH_SIZE];
  uint8_t* output[BATCH_SIZE];
  int64_t sink = 0;

  TestData(double selectivity, int num_conjuncts) : num_conjuncts(num_conjuncts) {
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(0, 999999);
    tuples.resize(BATCH_SIZE * TUPLE_SIZE, 0);
    for (int i = 0; i < BATCH_SIZE; ++i) {
      int64_t v = dist(rng);
      memcpy(&tuples[i * TUPLE_SIZE + SLOT_OFFSET], &v, sizeof(v));
      // 1% NULLs.
      if (rng() % 100 == 0) tuples[i * TUPLE_SIZE] = 1 << NULL_BIT;
    }
    lo = num_conjuncts == 1 ? 0 : 1;
    hi = lo + static_cast<int64_t>(selectivity * 1000000);
    NullIndicatorOffset null_offset(0, NULL_BIT);
    comparisons.emplace_back(
        TYPE_BIGINT, VectorizedComparison::LT, SLOT_OFFSET, null_offset, hi, 0);
    if (num_conjuncts == 2) {
      comparisons.emplace_back(
          TYPE_BIGINT, VectorizedComparison::GE, SLOT_OFFSET, null_offset, lo, 0);
    }
  }
};

void RowAtATime(int iters, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  const int64_t lo = d->lo;
  const int64_t hi = d->hi;
  const bool two_conjuncts = d->num_conjuncts == 2;
  for (int it = 0; it < iters; ++it) {
    uint8_t* tuple = d->tuples.data();
    uint8_t** out = d->output;
    bool* is_selected = d->selected;
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      *out = tuple;
      int64_t v;
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

void Vectorized(int iters, void* data) {
  TestData* d = reinterpret_cast<TestData*>(data);
  for (int it = 0; it < iters; ++it) {
    memset(d->selected, true, BATCH_SIZE);
    for (const VectorizedComparison& cmp : d->comparisons) {
      cmp.Eval(d->tuples.data(), TUPLE_SIZE, BATCH_SIZE, d->selected);
    }
    uint8_t* tuple = d->tuples.data();
    uint8_t** out = d->output;
    bool* is_selected = d->selected;
    for (int i = 0; i < BATCH_SIZE; ++i, tuple += TUPLE_SIZE) {
      *out = tuple;
      if (!*is_selected++) continue;
      ++out;
    }
    d->sink += out - d->output;
  }
}

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;
  char name[120];
  for (int num_conjuncts : {1, 2}) {
    for (double selectivity : {0.01, 0.5, 0.99}) {
      snprintf(name, sizeof(name), "%d conjunct(s), selectivity %.2f", num_conjuncts,
          selectivity);
      Benchmark suite(name);
      TestData* d = new TestData(selectivity, num_conjuncts);
      suite.AddBenchmark("row-at-a-time", RowAtATime, d);
      suite.AddBenchmark("vectorized", Vectorized, d);
      cout << suite.Measure() << endl;
    }
  }
  return 0;
}
