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

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/local-exchanger.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/test-env.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"
#include "util/runtime-profile-counters.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace strings;

namespace impala {

static const int64_t BUFFER_POOL_LIMIT = 256L * 1024L * 1024L;
static const int64_t MIN_BUFFER_LEN = 1024;
static const int64_t PAGE_LEN = 64 * 1024;
static const int BATCH_SIZE = 256;
static const char* EXCHANGE_NAME = "test_cte_0";

/// Tests for LocalExchanger, focused on multiple consumers reading at different speeds
/// with a reservation small enough to force spilling.
class LocalExchangerTest : public testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {
    if (client_.is_registered()) {
      EXPECT_EQ(0, client_.GetUsedReservation()) << "Exchanger leaked pages";
      test_env_->exec_env()->buffer_pool()->DeregisterClient(&client_);
    }
    pool_.Clear();
    test_env_.reset();
  }

  /// Sets up the test environment. The client may use at most 'reservation_limit'
  /// bytes of reservation, which is claimed upfront to mimic an exec node claiming its
  /// minimum reservation.
  void Init(int64_t reservation_limit, const TQueryOptions* query_options = nullptr) {
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(MIN_BUFFER_LEN, BUFFER_POOL_LIMIT);
    ASSERT_OK(test_env_->Init());

    DescriptorTblBuilder builder(test_env_->exec_env()->frontend(), &pool_);
    builder.DeclareTuple() << TYPE_INT << TYPE_STRING;
    row_desc_ = pool_.Add(new RowDescriptor(*builder.Build(),
        vector<TTupleId>(1, 0), vector<bool>(1, false)));

    ASSERT_OK(test_env_->CreateQueryState(0, query_options, &runtime_state_));
    profile_ = RuntimeProfile::Create(&pool_, "producer");
    MemTracker* client_tracker =
        pool_.Add(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    tracker_ = client_tracker;
    ASSERT_OK(test_env_->exec_env()->buffer_pool()->RegisterClient("producer",
        runtime_state_->query_state()->file_group(),
        runtime_state_->instance_buffer_reservation(), client_tracker,
        reservation_limit, profile_, &client_));
    ASSERT_TRUE(client_.IncreaseReservation(reservation_limit));
  }

  unique_ptr<LocalExchanger> CreateExchanger(int num_consumers) {
    unique_ptr<LocalExchanger> exchanger(new LocalExchanger(num_consumers));
    EXPECT_OK(exchanger->Init(runtime_state_, profile_, tracker_, &client_, row_desc_,
        PAGE_LEN, PAGE_LEN, EXCHANGE_NAME));
    return exchanger;
  }

  /// Value of the string slot for row 'i'. Long enough to not be inlined.
  static string GenString(int64_t i) {
    return Substitute("row-$0-padding-to-make-this-string-long", i);
  }

  /// Fills 'batch' with rows [start, start + num_rows).
  void FillBatch(RowBatch* batch, int64_t start, int num_rows) {
    batch->Reset();
    const TupleDescriptor* tuple_desc = row_desc_->tuple_descriptors()[0];
    const SlotDescriptor* int_slot = tuple_desc->slots()[0];
    const SlotDescriptor* str_slot = tuple_desc->slots()[1];
    for (int r = 0; r < num_rows; ++r) {
      int64_t value = start + r;
      Tuple* tuple = Tuple::Create(tuple_desc->byte_size(), batch->tuple_data_pool());
      *reinterpret_cast<int32_t*>(tuple->GetSlot(int_slot->tuple_offset())) = value;
      string s = GenString(value);
      char* buf = reinterpret_cast<char*>(batch->tuple_data_pool()->Allocate(s.size()));
      memcpy(buf, s.data(), s.size());
      tuple->GetStringSlot(str_slot->tuple_offset())->Assign(buf, s.size());
      TupleRow* row = batch->GetRow(batch->AddRow());
      row->SetTuple(0, tuple);
      batch->CommitLastRow();
    }
  }

  /// Pushes 'num_rows' rows into 'exchanger' and closes the producer.
  void Produce(LocalExchanger* exchanger, int64_t num_rows, Status* status) {
    RowBatch batch(row_desc_, BATCH_SIZE, tracker_);
    for (int64_t start = 0; start < num_rows; start += BATCH_SIZE) {
      int n = std::min<int64_t>(BATCH_SIZE, num_rows - start);
      FillBatch(&batch, start, n);
      *status = exchanger->Push(&batch);
      if (!status->ok()) break;
    }
    batch.Reset();
    exchanger->CloseProducer(*status);
  }

  /// Reads rows from 'exchanger', verifying that they arrive in order. Stops after
  /// 'max_rows' rows (if >= 0). Sleeps 'sleep_us' after each batch.
  void Consume(LocalExchanger* exchanger, int64_t max_rows, int sleep_us,
      int64_t* rows_read, Status* status) {
    int32_t idx = exchanger->Open();
    RowBatch batch(row_desc_, BATCH_SIZE, tracker_);
    const TupleDescriptor* tuple_desc = row_desc_->tuple_descriptors()[0];
    const SlotDescriptor* int_slot = tuple_desc->slots()[0];
    const SlotDescriptor* str_slot = tuple_desc->slots()[1];
    *rows_read = 0;
    bool eos = false;
    while (!eos && (max_rows < 0 || *rows_read < max_rows)) {
      batch.Reset();
      *status = exchanger->GetNext(runtime_state_, idx, &batch, &eos);
      if (!status->ok()) break;
      for (int r = 0; r < batch.num_rows(); ++r) {
        Tuple* tuple = batch.GetRow(r)->GetTuple(0);
        int32_t value = *reinterpret_cast<int32_t*>(
            tuple->GetSlot(int_slot->tuple_offset()));
        ASSERT_EQ(*rows_read, value);
        StringValue* sv = tuple->GetStringSlot(str_slot->tuple_offset());
        ASSERT_EQ(GenString(value), string(sv->Ptr(), sv->Len()));
        ++*rows_read;
      }
      if (sleep_us > 0) std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
    batch.Reset();
    exchanger->CloseConsumer(idx);
  }

  int64_t GetCounter(const string& name) {
    RuntimeProfileBase* child = profile_->GetChildByPrefix(
        RuntimeProfile::PREFIX_LOCAL_EXCHANGER + EXCHANGE_NAME);
    EXPECT_TRUE(child != nullptr);
    RuntimeProfile::Counter* counter = child->GetCounter(name);
    EXPECT_TRUE(counter != nullptr) << name;
    return counter == nullptr ? -1 : counter->value();
  }

  /// Runs a producer and consumers concurrently. 'sleep_us[i]' is the per-batch delay
  /// of consumer i; a negative value means the consumer only starts reading once the
  /// producer has finished, forcing the exchanger to buffer everything.
  void RunConsumers(int64_t num_rows, const vector<int>& sleep_us) {
    int num_consumers = sleep_us.size();
    unique_ptr<LocalExchanger> exchanger = CreateExchanger(num_consumers);
    Status producer_status;
    vector<Status> statuses(num_consumers);
    vector<int64_t> rows_read(num_consumers, 0);
    std::atomic<bool> producer_done{false};
    vector<std::thread> threads;
    for (int i = 0; i < num_consumers; ++i) {
      threads.emplace_back([&, i]() {
        if (sleep_us[i] < 0) {
          while (!producer_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
        }
        Consume(exchanger.get(), -1, std::max(0, sleep_us[i]), &rows_read[i],
            &statuses[i]);
      });
    }
    Produce(exchanger.get(), num_rows, &producer_status);
    producer_done = true;
    for (std::thread& t : threads) t.join();
    exchanger->Release();
    ASSERT_OK(producer_status);
    for (int i = 0; i < num_consumers; ++i) {
      ASSERT_OK(statuses[i]);
      EXPECT_EQ(num_rows, rows_read[i]) << "consumer " << i;
    }
    EXPECT_EQ(num_rows, GetCounter("RowsPushed"));
  }

  ObjectPool pool_;
  unique_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_ = nullptr;
  RuntimeProfile* profile_ = nullptr;
  MemTracker* tracker_ = nullptr;
  const RowDescriptor* row_desc_ = nullptr;
  BufferPool::ClientHandle client_;
};

/// A single consumer with plenty of reservation never spills.
TEST_F(LocalExchangerTest, SingleConsumerNoSpill) {
  Init(64 * PAGE_LEN);
  RunConsumers(20000, {0});
  EXPECT_EQ(0, GetCounter("SegmentsSpilled"));
}

/// Two consumers, one of which only starts after all rows are pushed. With only
/// (num_consumers + 1) pages of reservation, the exchanger must spill.
TEST_F(LocalExchangerTest, StalledConsumerSpills) {
  Init(3 * PAGE_LEN);
  RunConsumers(50000, {0, -1});
  EXPECT_GT(GetCounter("SegmentsSpilled"), 0);
  EXPECT_GT(GetCounter("SegmentsRepinned"), 0);
}

/// All consumers stalled: everything is buffered, mostly on disk.
TEST_F(LocalExchangerTest, AllConsumersStalled) {
  Init(3 * PAGE_LEN);
  RunConsumers(50000, {-1, -1});
  EXPECT_GT(GetCounter("SegmentsSpilled"), 0);
}

/// Four consumers at different speeds with minimal reservation.
TEST_F(LocalExchangerTest, FourConsumersVaryingSpeeds) {
  Init(5 * PAGE_LEN);
  RunConsumers(50000, {0, 50, 500, -1});
  EXPECT_GT(GetCounter("SegmentsSpilled"), 0);
}

/// With extra reservation available, a lagging consumer causes the exchanger to grow
/// its reservation instead of spilling.
TEST_F(LocalExchangerTest, GrowsReservationBeforeSpilling) {
  Init(256 * PAGE_LEN);
  RunConsumers(20000, {0, -1});
  EXPECT_EQ(0, GetCounter("SegmentsSpilled"));
}

/// A consumer that closes early must not block release of segments.
TEST_F(LocalExchangerTest, EarlyCloseConsumer) {
  Init(3 * PAGE_LEN);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(2);
  Status producer_status, s0, s1;
  int64_t rows0 = 0, rows1 = 0;
  std::thread early([&]() { Consume(exchanger.get(), 1000, 0, &rows0, &s0); });
  std::thread full([&]() { Consume(exchanger.get(), -1, 0, &rows1, &s1); });
  Produce(exchanger.get(), 30000, &producer_status);
  early.join();
  full.join();
  exchanger->Release();
  ASSERT_OK(producer_status);
  ASSERT_OK(s0);
  ASSERT_OK(s1);
  EXPECT_GE(rows0, 1000);
  EXPECT_EQ(30000, rows1);
}

/// Rows pushed after all consumers closed are dropped.
TEST_F(LocalExchangerTest, AllConsumersClosedEarly) {
  Init(3 * PAGE_LEN);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(1);
  Status producer_status, s0;
  int64_t rows0 = 0;
  Consume(exchanger.get(), 0, 0, &rows0, &s0);
  Produce(exchanger.get(), 30000, &producer_status);
  exchanger->Release();
  ASSERT_OK(producer_status);
  ASSERT_OK(s0);
  EXPECT_EQ(0, rows0);
}

/// An error from the producer is returned to consumers after the pushed rows.
TEST_F(LocalExchangerTest, ProducerError) {
  Init(64 * PAGE_LEN);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(1);
  RowBatch batch(row_desc_, BATCH_SIZE, tracker_);
  FillBatch(&batch, 0, BATCH_SIZE);
  ASSERT_OK(exchanger->Push(&batch));
  batch.Reset();
  exchanger->CloseProducer(Status("producer failed"));
  int64_t rows0 = 0;
  Status s0;
  Consume(exchanger.get(), -1, 0, &rows0, &s0);
  exchanger->Release();
  EXPECT_EQ(BATCH_SIZE, rows0);
  ASSERT_FALSE(s0.ok());
  EXPECT_STR_CONTAINS(s0.GetDetail(), "producer failed");
}

/// If the producer is released without being closed, e.g. because it was never opened,
/// consumers return an error after the pushed rows instead of reporting eos.
TEST_F(LocalExchangerTest, ReleaseWithoutCloseProducer) {
  Init(64 * PAGE_LEN);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(1);
  RowBatch batch(row_desc_, BATCH_SIZE, tracker_);
  FillBatch(&batch, 0, BATCH_SIZE);
  ASSERT_OK(exchanger->Push(&batch));
  batch.Reset();
  int64_t rows0 = 0;
  Status s0;
  std::thread consumer([&]() { Consume(exchanger.get(), -1, 0, &rows0, &s0); });
  // Release() publishes the pushed rows and waits for the consumer to close.
  exchanger->Release();
  consumer.join();
  EXPECT_EQ(BATCH_SIZE, rows0);
  EXPECT_TRUE(s0.IsCancelled()) << s0.GetDetail();
}

/// A consumer waiting for data notices cancellation.
TEST_F(LocalExchangerTest, CancelWaitingConsumer) {
  Init(3 * PAGE_LEN);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(1);
  Status s0;
  int64_t rows0 = 0;
  std::thread consumer([&]() { Consume(exchanger.get(), -1, 0, &rows0, &s0); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  runtime_state_->Cancel();
  consumer.join();
  EXPECT_TRUE(s0.IsCancelled()) << s0.GetDetail();
  exchanger->CloseProducer();
  exchanger->Release();
}

/// With scratch disabled, the exchanger fails with a clear error instead of spilling.
TEST_F(LocalExchangerTest, SpillingDisabled) {
  TQueryOptions query_options;
  query_options.__set_scratch_limit(0);
  Init(3 * PAGE_LEN, &query_options);
  unique_ptr<LocalExchanger> exchanger = CreateExchanger(1);
  Status producer_status;
  // No consumer reads, so the exchanger must buffer everything.
  Produce(exchanger.get(), 50000, &producer_status);
  ASSERT_FALSE(producer_status.ok());
  EXPECT_STR_CONTAINS(producer_status.GetDetail(), "scratch_limit is 0");
  int32_t idx = exchanger->Open();
  exchanger->CloseConsumer(idx);
  exchanger->Release();
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  auto common_runtime_ctx =
      impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
