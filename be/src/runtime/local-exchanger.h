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

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/buffered-tuple-stream.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "util/runtime-profile.h"

namespace llvm {
class Function;
}

namespace impala {

class LlvmCodeGen;
class MemTracker;
class RowBatch;
class RowDescriptor;
class RuntimeState;

/// Spillable host-local multicast exchange. One producer pushes RowBatches which are
/// read, in order, by multiple consumers running in other threads. Inspired by
/// StarRocks' multi_cast_local_exchange.
///
/// Storage:
/// Rows are copied into a sequence of segments. Each segment is a BufferedTupleStream
/// holding exactly one page (a default-sized page, or a single large page for a row
/// that does not fit in a default-sized page). All segments share the producer's
/// BufferPool client, so all exchanged data is backed by buffer pool reservation.
/// The producer writes the "open" segment; once it is full (or a consumer is waiting
/// for data, or the producer is done) it is finalized and published to consumers.
///
/// A BufferedTupleStream only supports multiple concurrent readers while it is pinned,
/// so a finalized segment is always either pinned (readable by any number of consumers
/// concurrently via their own ReadIterator) or unpinned (spilled). When the producer
/// cannot get reservation for a new page, it unpins the newest finalized segment that
/// no consumer is currently reading, letting the buffer pool write it to scratch.
/// When a consumer reaches an unpinned segment it re-pins it. A segment is destroyed
/// once every consumer has read past it.
///
/// Reservation:
/// At any time at most one pinned segment per consumer is being read plus the open
/// segment is being written. The producer's minimum reservation must therefore be
/// at least (num_consumers + 1) * max_page_len to guarantee progress. Beyond that the
/// exchanger grows its reservation until the buffer pool denies it, then spills.
///
/// Thread safety:
/// All methods are thread-safe. A BufferPool client may only be used by one thread at a
/// time, so every operation that may touch the client (writing, pinning, unpinning,
/// closing streams) is done while holding 'mutex_'. Consumers read from pinned
/// segments without holding the lock, which is safe because a segment with active
/// readers is never unpinned or closed, and each consumer owns its ReadIterator.
class LocalExchanger {
 public:
  explicit LocalExchanger(int32_t num_consumers);
  ~LocalExchanger();

  /// Initializes the exchanger. Must be called before Push() or GetNext().
  /// 'state' is the producer's RuntimeState and 'mem_tracker' the producer's
  /// MemTracker, both used when spilling. 'client' is the producer's BufferPool
  /// client; it must be registered before the first Push() and must remain valid until
  /// Release() returns. 'row_desc' describes the rows pushed. 'default_page_len' and
  /// 'max_page_len' are the stream page sizes.
  Status Init(RuntimeState* state, RuntimeProfile* profile, MemTracker* mem_tracker,
      BufferPool::ClientHandle* client, const RowDescriptor* row_desc,
      int64_t default_page_len, int64_t max_page_len, const std::string& name);

  /// Called by each consumer to get its unique consumer index.
  int32_t Open();

  /// Signature of functions that append the rows of 'batch' starting at 'start_row' to
  /// a segment's stream until a row cannot be added, e.g. because the segment's page is
  /// full or there's no reservation for it. Returns the index of the first row not added,
  /// or batch->num_rows() if all were added. Sets 'status' if an error occurred. Push()
  /// then finalizes the segment or spills and calls the function again for the remaining
  /// rows. 'ctx' is passed through from Push().
  typedef int (*AddRowsFn)(void* ctx, BufferedTupleStream* stream, RowBatch* batch,
      int start_row, Status* status);

  /// AddRowsFn that copies the rows of 'batch' with BufferedTupleStream::AddRow().
  /// 'ctx' is unused. Cross-compiled so the AddRow() call can be replaced with a version
  /// specialized for the row layout.
  static int AddRows(void* ctx, BufferedTupleStream* stream, RowBatch* batch,
      int start_row, Status* status);

  /// Codegens AddRows() for rows described by 'row_desc', with
  /// BufferedTupleStream::AddRow() replaced by its codegen'd version.
  static Status CodegenAddRows(LlvmCodeGen* codegen, const RowDescriptor* row_desc,
      llvm::Function** fn);

  /// Copies all rows in 'batch' into the exchanger, spilling if needed. 'batch' may be
  /// reused or destroyed after this returns. Returns an error if the rows could not be
  /// stored, e.g. because spilling is disabled or a scratch I/O error occurred.
  /// 'add_rows_fn' writes the rows into the exchanger's streams; if nullptr, the
  /// interpreted AddRows() is used, and 'batch' must have the row layout passed to
  /// Init(). 'ctx' is passed to 'add_rows_fn'.
  Status Push(RowBatch* batch, AddRowsFn add_rows_fn = nullptr, void* ctx = nullptr);

  /// Reads the next rows for consumer 'consumer_index' into 'batch', which must be
  /// empty and have the row layout of the pushed rows. Blocks until rows are available
  /// or the producer is closed. Sets 'eos' once all rows have been returned.
  /// Rows in 'batch' reference exchanger memory that remains valid until the next call
  /// to GetNext() or CloseConsumer() for this consumer. 'consumer_state' is used to
  /// check for cancellation of the consumer while waiting.
  Status GetNext(RuntimeState* consumer_state, int32_t consumer_index, RowBatch* batch,
      bool* eos);

  /// Closes exchanger for producer and signals eos. After this call, no more Push()
  /// calls are allowed. If 'status' is an error, consumers will return it after
  /// reading all rows pushed so far instead of reporting eos.
  void CloseProducer(const Status& status = Status::OK());

  /// Closes exchanger for consumer_index. After this call, no more GetNext() calls
  /// are allowed for this consumer.
  void CloseConsumer(int32_t consumer_index);

  /// Blocks until all consumers have closed or timeout_ms elapses.
  /// Returns true if all consumers are done, false if timed out.
  bool ReadFinished(int timeout_ms) const;

  /// Waits for all consumers to close, then frees all resources. Must be called by the
  /// producer before its BufferPool client is closed.
  void Release();

 private:
  struct Segment {
    std::unique_ptr<BufferedTupleStream> stream;
    /// Number of consumers currently reading from this segment.
    int32_t active_readers = 0;
    /// Number of consumers that have not yet read past this segment.
    int32_t remaining_consumers = 0;
    /// True if this segment's var-len pointers must be resolved (see
    /// BufferedTupleStream::ResolvePointers()) before consumers read it. Resolving once
    /// lets all consumers read the page without writing to it.
    bool needs_resolve = false;
    /// True while a consumer is resolving the segment's pointers without holding
    /// 'mutex_'. Other consumers wait for it to finish.
    bool resolving = false;
  };

  struct ConsumerState {
    /// Absolute index of the next segment to read, or of the segment being read.
    int64_t segment_idx = 0;
    /// True if the consumer has an active ReadIterator over segment 'segment_idx'.
    bool in_segment = false;
    /// True if the consumer has read all rows of segment 'segment_idx' but may still
    /// reference its memory from the last returned batch.
    bool segment_done = false;
    bool closed = false;
    /// The stream of segment 'segment_idx' while 'in_segment' is true. The active reader
    /// keeps the segment alive, so the consumer can read it without holding 'mutex_'.
    BufferedTupleStream* stream = nullptr;
    BufferedTupleStream::ReadIterator iter;
  };

  /// Returns the segment with absolute index 'idx'. Must be called with 'mutex_' held.
  Segment* GetSegment(int64_t idx) {
    DCHECK_GE(idx, first_segment_idx_);
    DCHECK_LT(idx - first_segment_idx_, segments_.size());
    return segments_[idx - first_segment_idx_].get();
  }

  /// Creates a new open segment. 'size_hint' is an estimate of the bytes that will be
  /// written before it is finalized; it's used to pick a smaller page when consumers are
  /// waiting for data. Must be called with 'mutex_' held.
  Status CreateOpenSegment(int64_t size_hint);

  /// Finalizes the open segment (if any rows were written) and makes it available to
  /// consumers. Must be called with 'mutex_' held.
  void FinalizeOpenSegment();

  /// Frees up reservation by unpinning the newest pinned finalized segment that has no
  /// active readers (other than 'exclude'). Returns an error if spilling is not allowed
  /// or there is nothing to unpin. Must be called with 'mutex_' held.
  Status Spill(const Segment* exclude, int64_t bytes_needed);

  /// Enters segment 'cs->segment_idx', pinning it if needed. Resolves its pointers
  /// (releasing 'lock' while doing so) or waits for another consumer to do it.
  /// 'lock' must hold 'mutex_'.
  Status EnterSegment(ConsumerState* cs, std::unique_lock<std::mutex>* lock);

  /// Leaves the consumer's current segment. Must be called with 'mutex_' held.
  void LeaveSegment(ConsumerState* cs);

  /// Destroys fully-consumed segments at the front of 'segments_'. Must be called with
  /// 'mutex_' held.
  void ReleaseConsumedSegments();

  /// Number of consumers not yet closed. Must be called with 'mutex_' held.
  int32_t NumActiveConsumers() const { return consumer_count_ - consumers_done_; }

  /// Number of consumers waiting for data in GetNext(). Must be called with 'mutex_'
  /// held.
  bool HasWaitingConsumers() const { return waiting_consumers_ > 0; }

  const int32_t consumer_count_;
  RuntimeState* state_ = nullptr;
  MemTracker* mem_tracker_ = nullptr;
  RuntimeProfile* runtime_profile_ = nullptr;
  BufferPool::ClientHandle* client_ = nullptr;
  const RowDescriptor* row_desc_ = nullptr;
  int64_t default_page_len_ = 0;
  int64_t max_page_len_ = 0;
  int64_t min_page_len_ = 0;
  std::string name_;

  /// Estimated bytes per row in a segment, used to size the page of segments created
  /// while consumers are waiting. Updated as segments fill up.
  int64_t avg_row_bytes_ = 0;

  RuntimeProfile::HighWaterMarkCounter* num_segments_counter_ = nullptr;
  RuntimeProfile::HighWaterMarkCounter* pinned_bytes_counter_ = nullptr;
  RuntimeProfile::Counter* rows_pushed_counter_ = nullptr;
  RuntimeProfile::Counter* segments_spilled_counter_ = nullptr;
  RuntimeProfile::Counter* segments_repinned_counter_ = nullptr;

  mutable std::mutex mutex_;
  mutable std::condition_variable all_consumers_done_cv_;
  std::condition_variable batch_available_cv_;
  int32_t consumers_done_ = 0;
  int32_t next_consumer_index_ = 0;
  int32_t waiting_consumers_ = 0;

  /// Segment currently being written by the producer, not visible to consumers.
  std::unique_ptr<Segment> open_segment_;

  /// Finalized segments not yet consumed by all consumers. segments_[0] has absolute
  /// index 'first_segment_idx_'.
  std::deque<std::unique_ptr<Segment>> segments_;
  int64_t first_segment_idx_ = 0;

  /// Bytes currently pinned by finalized segments and the open segment.
  int64_t pinned_bytes_ = 0;

  std::vector<ConsumerState> consumers_;
  bool eos_ = false;
  bool spilled_ = false;
  bool released_ = false;
  Status producer_status_;
};

}
