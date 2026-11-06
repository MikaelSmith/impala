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

#include "runtime/local-exchanger.h"

#include <algorithm>
#include <chrono>

#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/bit-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

using namespace strings;

namespace impala {

/// How often a waiting consumer wakes up to check for cancellation.
static constexpr int CONSUMER_WAIT_MS = 10;

LocalExchanger::LocalExchanger(int32_t num_consumers)
  : consumer_count_(num_consumers), consumers_(num_consumers) {
  DCHECK_GT(num_consumers, 0);
}

LocalExchanger::~LocalExchanger() {
  DCHECK(released_ || state_ == nullptr) << "Release() must be called to free resources.";
}

Status LocalExchanger::Init(RuntimeState* state, RuntimeProfile* profile,
    MemTracker* mem_tracker, BufferPool::ClientHandle* client,
    const RowDescriptor* row_desc, int64_t default_page_len, int64_t max_page_len,
    const string& name) {
  DCHECK(state_ == nullptr) << "Init() called twice";
  DCHECK(BitUtil::IsPowerOf2(default_page_len)) << default_page_len;
  DCHECK(BitUtil::IsPowerOf2(max_page_len)) << max_page_len;
  DCHECK_GE(max_page_len, default_page_len);
  state_ = state;
  mem_tracker_ = mem_tracker;
  client_ = client;
  row_desc_ = row_desc;
  default_page_len_ = default_page_len;
  max_page_len_ = max_page_len;
  min_page_len_ =
      std::min(default_page_len, ExecEnv::GetInstance()->buffer_pool()->min_buffer_len());
  name_ = name;
  runtime_profile_ = profile->CreateChild(Substitute(
      "$0$1", RuntimeProfile::PREFIX_LOCAL_EXCHANGER, name), true, true, false);
  num_segments_counter_ =
      runtime_profile_->AddHighWaterMarkCounter("MaxSegments", TUnit::UNIT);
  pinned_bytes_counter_ =
      runtime_profile_->AddHighWaterMarkCounter("PeakPinnedBytes", TUnit::BYTES);
  rows_pushed_counter_ = ADD_COUNTER(runtime_profile_, "RowsPushed", TUnit::UNIT);
  segments_spilled_counter_ =
      ADD_COUNTER(runtime_profile_, "SegmentsSpilled", TUnit::UNIT);
  segments_repinned_counter_ =
      ADD_COUNTER(runtime_profile_, "SegmentsRepinned", TUnit::UNIT);
  avg_row_bytes_ = std::max(1, row_desc->GetRowSize());
  return Status::OK();
}

int32_t LocalExchanger::Open() {
  lock_guard<mutex> l(mutex_);
  DCHECK_LT(next_consumer_index_, consumer_count_);
  return next_consumer_index_++;
}

Status LocalExchanger::CreateOpenSegment(int64_t size_hint) {
  DCHECK(open_segment_ == nullptr);
  int64_t page_len = default_page_len_;
  if (HasWaitingConsumers()) {
    // A consumer is waiting, so this segment will be finalized at the end of the
    // current Push(). Avoid wasting a mostly-empty default-sized page on it.
    page_len = std::max(min_page_len_, std::min(default_page_len_,
        static_cast<int64_t>(BitUtil::RoundUpToPowerOfTwo(std::max<int64_t>(
            size_hint, 1)))));
  }
  unique_ptr<Segment> segment = make_unique<Segment>();
  segment->stream.reset(
      new BufferedTupleStream(state_, row_desc_, client_, page_len, max_page_len_));
  RETURN_IF_ERROR(segment->stream->Init(name_, /*pinned=*/true));
  // Keep each segment to a single page so that a segment can always be pinned with a
  // single page worth of reservation.
  segment->stream->LimitToSinglePage();
  while (true) {
    bool got_reservation = false;
    Status status = segment->stream->PrepareForWrite(&got_reservation);
    if (status.ok() && got_reservation) break;
    if (status.ok()) status = Spill(nullptr, page_len);
    if (!status.ok()) {
      segment->stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
      return status;
    }
  }
  open_segment_ = std::move(segment);
  return Status::OK();
}

void LocalExchanger::FinalizeOpenSegment() {
  if (open_segment_ == nullptr) return;
  unique_ptr<Segment> segment = std::move(open_segment_);
  segment->stream->DoneWriting();
  segment->remaining_consumers = NumActiveConsumers();
  if (segment->stream->num_rows() == 0 || segment->remaining_consumers == 0) {
    pinned_bytes_ -= segment->stream->BytesPinned(false);
    segment->stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    return;
  }
  // Resolve pointers that were not resolved on write, e.g. of collections, before any
  // consumer can read the segment concurrently.
  segment->needs_resolve = segment->stream->HasUnresolvedPages();
  segments_.push_back(std::move(segment));
  num_segments_counter_->Set(static_cast<int64_t>(segments_.size()));
  batch_available_cv_.notify_all();
}

Status LocalExchanger::Spill(const Segment* exclude, int64_t bytes_needed) {
  // Unpin the newest idle segment: it is the one the slowest consumer reaches last.
  for (auto it = segments_.rbegin(); it != segments_.rend(); ++it) {
    Segment* segment = it->get();
    if (segment == exclude || segment->active_readers > 0) continue;
    if (!segment->stream->is_pinned()) continue;
    if (!spilled_) {
      // StartSpilling() returns an error if spilling is disabled for the query.
      RETURN_IF_ERROR(state_->StartSpilling(mem_tracker_));
      runtime_profile_->AppendExecOption("Spilled");
      spilled_ = true;
    }
    int64_t pinned = segment->stream->BytesPinned(false);
    RETURN_IF_ERROR(segment->stream->UnpinStream(BufferedTupleStream::UNPIN_ALL));
    pinned_bytes_ -= pinned - segment->stream->BytesPinned(false);
    COUNTER_ADD(segments_spilled_counter_, 1);
    return Status::OK();
  }
  return Status(TErrorCode::INTERNAL_ERROR, Substitute(
      "Could not free up $0 of reservation for CTE exchange $1: all $2 buffered "
      "segments are spilled or being read.\n$3",
      PrettyPrinter::PrintBytes(bytes_needed), name_, segments_.size(),
      client_->DebugString()));
}

Status LocalExchanger::CodegenAddRows(
    LlvmCodeGen* codegen, const RowDescriptor* row_desc, llvm::Function** fn) {
  llvm::Function* add_row_fn = nullptr;
  RETURN_IF_ERROR(BufferedTupleStream::CodegenAddRow(codegen, row_desc, &add_row_fn));
  llvm::Function* add_rows_fn =
      codegen->GetFunction(IRFunction::LOCAL_EXCHANGER_ADD_ROWS, true);
  DCHECK(add_rows_fn != nullptr);
  int replaced = codegen->ReplaceCallSites(
      add_rows_fn, add_row_fn, "BufferedTupleStream6AddRow");
  DCHECK_REPLACE_COUNT(replaced, 1);
  *fn = codegen->FinalizeFunction(add_rows_fn);
  if (*fn == nullptr) {
    return Status("Codegen'd LocalExchanger::AddRows() failed verification, see log");
  }
  return Status::OK();
}

Status LocalExchanger::Push(RowBatch* batch, AddRowsFn add_rows_fn) {
  DCHECK(state_ != nullptr) << "Init() must be called before Push()";
  DCHECK(client_->is_registered());
  DCHECK(batch->row_desc()->Equals(*row_desc_));
  if (add_rows_fn == nullptr) add_rows_fn = &LocalExchanger::AddRows;
  lock_guard<mutex> l(mutex_);
  DCHECK(!eos_) << "Push() after CloseProducer()";
  // Nobody will read the rows, drop them.
  if (NumActiveConsumers() == 0) return Status::OK();
  const int num_rows = batch->num_rows();
  int next_row = 0;
  while (next_row < num_rows) {
    if (open_segment_ == nullptr) {
      // Only used to size the page if a consumer is waiting.
      RETURN_IF_ERROR(CreateOpenSegment((num_rows - next_row) * avg_row_bytes_));
    }
    BufferedTupleStream* stream = open_segment_->stream.get();
    int64_t pinned_before = stream->BytesPinned(false);
    Status status;
    next_row = add_rows_fn(stream, batch, next_row, &status);
    pinned_bytes_ += stream->BytesPinned(false) - pinned_before;
    RETURN_IF_ERROR(status);
    if (next_row == num_rows) break;
    if (stream->num_rows() > 0) {
      // The segment's single page is full.
      avg_row_bytes_ = std::max<int64_t>(1, stream->byte_size() / stream->num_rows());
      FinalizeOpenSegment();
    } else {
      // Out of reservation for the segment's page: spill an idle segment and retry.
      RETURN_IF_ERROR(Spill(nullptr, default_page_len_));
    }
  }
  pinned_bytes_counter_->Set(pinned_bytes_);
  COUNTER_ADD(rows_pushed_counter_, num_rows);
  // Don't make consumers that have caught up wait for the page to fill.
  if (HasWaitingConsumers()) FinalizeOpenSegment();
  return Status::OK();
}

Status LocalExchanger::EnterSegment(
    ConsumerState* cs, unique_lock<mutex>* lock) {
  DCHECK(!cs->in_segment);
  DCHECK(lock->owns_lock());
  Segment* segment = GetSegment(cs->segment_idx);
  BufferedTupleStream* stream = segment->stream.get();
  if (!stream->is_pinned()) {
    // Read the segment back from scratch. PinStream() only starts the reads.
    while (true) {
      bool pinned = false;
      RETURN_IF_ERROR(stream->PinStream(&pinned));
      if (pinned) break;
      RETURN_IF_ERROR(Spill(segment, stream->byte_size()));
    }
    pinned_bytes_ += stream->BytesPinned(false);
    pinned_bytes_counter_->Set(pinned_bytes_);
    COUNTER_ADD(segments_repinned_counter_, 1);
    // Pages read back from scratch may be in different buffers.
    segment->needs_resolve = stream->HasUnresolvedPages();
  }
  // An active reader keeps the segment pinned while 'mutex_' is released below.
  ++segment->active_readers;
  while (segment->needs_resolve) {
    if (segment->resolving) {
      // Another consumer is resolving the segment.
      batch_available_cv_.wait(*lock);
      continue;
    }
    // Fix up the segment's pointers once so that no consumer writes to the page while
    // others read it. This waits for the page to be read back from scratch, so release
    // 'mutex_'. ResolvePointers() doesn't use the BufferPool client of a pinned stream.
    segment->resolving = true;
    lock->unlock();
    Status status = stream->ResolvePointers();
    lock->lock();
    segment->resolving = false;
    if (status.ok()) segment->needs_resolve = false;
    batch_available_cv_.notify_all();
    if (!status.ok()) {
      --segment->active_readers;
      return status;
    }
  }
  cs->iter = BufferedTupleStream::ReadIterator();
  Status status = stream->PrepareForPinnedRead(&cs->iter);
  if (!status.ok()) {
    --segment->active_readers;
    return status;
  }
  cs->stream = stream;
  cs->in_segment = true;
  cs->segment_done = false;
  return Status::OK();
}

void LocalExchanger::LeaveSegment(ConsumerState* cs) {
  DCHECK(cs->in_segment);
  Segment* segment = GetSegment(cs->segment_idx);
  --segment->active_readers;
  --segment->remaining_consumers;
  DCHECK_GE(segment->active_readers, 0);
  DCHECK_GE(segment->remaining_consumers, 0);
  cs->stream = nullptr;
  cs->in_segment = false;
  cs->segment_done = false;
  ++cs->segment_idx;
  ReleaseConsumedSegments();
}

void LocalExchanger::ReleaseConsumedSegments() {
  while (!segments_.empty() && segments_.front()->remaining_consumers == 0) {
    DCHECK_EQ(segments_.front()->active_readers, 0);
    BufferedTupleStream* stream = segments_.front()->stream.get();
    pinned_bytes_ -= stream->BytesPinned(false);
    stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
    segments_.pop_front();
    ++first_segment_idx_;
  }
  DCHECK_GE(pinned_bytes_, 0);
}

Status LocalExchanger::GetNext(
    RuntimeState* consumer_state, int32_t consumer_index, RowBatch* batch, bool* eos) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  DCHECK_EQ(batch->num_rows(), 0);
  ConsumerState* cs = &consumers_[consumer_index];
  DCHECK(!cs->closed);
  *eos = false;
  while (true) {
    if (!cs->in_segment || cs->segment_done) {
      unique_lock<mutex> l(mutex_);
      // The rows returned by the previous call are no longer referenced.
      if (cs->in_segment) LeaveSegment(cs);
      // Wait for the next segment to be published or for the producer to finish.
      ++waiting_consumers_;
      while (cs->segment_idx >= first_segment_idx_ + segments_.size() && !eos_) {
        if (consumer_state != nullptr && consumer_state->is_cancelled()) {
          --waiting_consumers_;
          return Status::CANCELLED;
        }
        batch_available_cv_.wait_for(l, std::chrono::milliseconds(CONSUMER_WAIT_MS));
      }
      --waiting_consumers_;
      if (cs->segment_idx >= first_segment_idx_ + segments_.size()) {
        DCHECK(eos_);
        RETURN_IF_ERROR(producer_status_);
        *eos = true;
        return Status::OK();
      }
      RETURN_IF_ERROR(EnterSegment(cs, &l));
    }
    // The segment is pinned and has an active reader, so it can be read without the
    // lock.
    DCHECK(cs->stream != nullptr);
    bool segment_eos = false;
    RETURN_IF_ERROR(cs->stream->GetNext(&cs->iter, batch, &segment_eos));
    cs->segment_done = segment_eos;
    // Segments are never empty, but the last batch of a segment may be.
    if (batch->num_rows() > 0) return Status::OK();
    DCHECK(segment_eos);
  }
}

void LocalExchanger::CloseProducer(const Status& status) {
  lock_guard<mutex> l(mutex_);
  if (eos_) return;
  FinalizeOpenSegment();
  producer_status_ = status;
  eos_ = true;
  batch_available_cv_.notify_all();
}

void LocalExchanger::CloseConsumer(int32_t consumer_index) {
  DCHECK(consumer_index >= 0 && consumer_index < consumer_count_);
  lock_guard<mutex> l(mutex_);
  ConsumerState* cs = &consumers_[consumer_index];
  DCHECK(!cs->closed);
  if (cs->in_segment) {
    --GetSegment(cs->segment_idx)->active_readers;
    cs->stream = nullptr;
    cs->in_segment = false;
  }
  // Drop this consumer from all published segments it has not read past.
  for (int64_t idx = std::max(cs->segment_idx, first_segment_idx_);
       idx < first_segment_idx_ + static_cast<int64_t>(segments_.size()); ++idx) {
    --GetSegment(idx)->remaining_consumers;
    DCHECK_GE(GetSegment(idx)->remaining_consumers, 0);
  }
  cs->closed = true;
  ReleaseConsumedSegments();
  ++consumers_done_;
  all_consumers_done_cv_.notify_all();
}

bool LocalExchanger::ReadFinished(int timeout_ms) const {
  unique_lock<mutex> l(mutex_);
  return all_consumers_done_cv_.wait_for(l, std::chrono::milliseconds(timeout_ms),
      [this]() { return consumers_done_ == consumer_count_; });
}

void LocalExchanger::Release() {
  unique_lock<mutex> l(mutex_);
  if (released_) return;
  // Ensure consumers can drain if CloseProducer() was not yet called (e.g. if Open()
  // never ran due to an early error). The producer did not finish, so consumers must
  // not report a successful eos.
  if (!eos_) {
    FinalizeOpenSegment();
    producer_status_ = Status::CANCELLED;
    eos_ = true;
    batch_available_cv_.notify_all();
  }
  // Wait for all consumers to have called CloseConsumer(). During normal execution
  // this condition is already satisfied before Release() is reached. During query
  // cancellation the producer's Close() may race ahead of consumer fragment instances
  // that are still draining the exchanger.
  all_consumers_done_cv_.wait(l, [&]() { return consumers_done_ == consumer_count_; });
  DCHECK(segments_.empty()) << "All segments must be consumed before Release()";
  DCHECK(open_segment_ == nullptr);
  for (auto& segment : segments_) {
    segment->stream->Close(nullptr, RowBatch::FlushMode::NO_FLUSH_RESOURCES);
  }
  segments_.clear();
  pinned_bytes_ = 0;
  released_ = true;
}

} // namespace impala
