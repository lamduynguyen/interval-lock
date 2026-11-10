#include "recovery/log_backend.h"
#include "common/constants.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "storage/aio.h"

#include "share_headers/time.h"

#include <unistd.h>
#include <algorithm>
#include <cerrno>

namespace leanstore::recovery {

// -------------------------------------------------------------------------------------
// LogIOSegment

LogIOSegment::LogIOSegment() {
  buffer = static_cast<u8 *>(
    aligned_alloc(BLK_BLOCK_SIZE, UpAlign(FLAGS_wal_batch_write_kb * KB + sizeof(LogMetaEntry), BLK_BLOCK_SIZE)));
}

LogIOSegment::~LogIOSegment() { free(buffer); }

/**
 * @brief LogIOSegment -- the write unit of autonomous commit
 *
 * Serialized format of LogIOSegment:
 *
 *  |-------------|-------------|------|-------------|---------------|----------------------|
 *  | Log entry 1 | Log entry 2 |......| Log entry N |..empty space..| "WRITE_METADATA" log |
 *  |-------------|-------------|------|-------------|---------------|----------------------|
 *
 * The "WRITE_METADATA" log entry contains information about this log flush:
 * - The written size of the log flush associated with this batch
 * - Total size of the actual logs
 * - LeanStore signature, used to differentiate between current session and previous session
 * For now, we put the "WRITE_METADATA" log at the end of the LogIOSegment for no particular reason
 *
 * `fill_fn` is responsible for filling/putting the dirty log entries into the to-write buffer (local variable ptr)
 * `write_fn` is used to trigger the write
 */
void LogIOSegment::Serialize(u64 log_size, const std::function<void(u8 *, u64 &)> &fill_fn,
                             const std::function<void(u8 *, u64)> &write_fn) {
  // It's possible, despite being unlikely, that we write more logs than the prepared buffer
  // In that case, we temporily allocate another buffer just for this operation
  u8 *ptr = buffer;
  if (log_size > FLAGS_wal_batch_write_kb * KB) [[unlikely]] {
    ptr = static_cast<u8 *>(aligned_alloc(BLK_BLOCK_SIZE, UpAlign(log_size, BLK_BLOCK_SIZE)));
  }

  // Metadata of this log block, used for log analysis during recovery phase
  const auto to_write_sz     = UpAlign(log_size, BLK_BLOCK_SIZE);
  auto padding_log           = new (&ptr[to_write_sz - sizeof(LogMetaEntry)]) LogMetaEntry();
  padding_log->type          = LogEntry::Type::WRITE_METADATA;
  padding_log->rc.w_id       = LeanStore::worker_thread_id;
  padding_log->size          = to_write_sz;
  padding_log->rc.chunk_size = log_size - sizeof(LogMetaEntry);
  padding_log->rc.signature  = LeanStore::signature;

  // Fill the write buffer, and then flush it to the non-volatile storage
  auto offset = 0UL;
  fill_fn(ptr, offset);
  Ensure(offset + sizeof(LogMetaEntry) <= to_write_sz);
  write_fn(ptr, to_write_sz);

  // Dirty works
  if (start_profiling) {
    statistics::recovery::real_log_bytes[LeanStore::worker_thread_id] += offset;
    statistics::recovery::written_log_bytes[LeanStore::worker_thread_id] += to_write_sz;
  }
  if (ptr != buffer) { free(ptr); };
}

/**
 * @brief We need to materialize the `WRITE_METADATA` log entry first, before accessing all other log entries
 * This requires us to read from the `&buffer[end_offset]` and the materialize backward
 */
auto LogIOSegment::Deserialize(u8 *buffer, u64 end_offset, u32 required_signature,
                               const std::function<void(u8 *, u64)> &analyze_fn) -> u64 {
  auto metadata = reinterpret_cast<LogMetaEntry *>(&buffer[end_offset - sizeof(LogMetaEntry)]);
  // Wrong signature, return
  if (required_signature != metadata->rc.signature) { return 0; }
  // Otherwise, deserialize the LogIOSegment
  analyze_fn(&buffer[end_offset - metadata->size], metadata->rc.chunk_size);
  return metadata->size;
}

// -------------------------------------------------------------------------------------

LogIOSegment::Buffer::Buffer(u64 size)
    : buffer((size == 0) ? nullptr : reinterpret_cast<u8 *>(malloc(size))), size(size) {}

LogIOSegment::Buffer::Buffer(u8 *stolen_buffer, u64 size) : buffer(stolen_buffer), size(size), stolen_buffer(true) {}

/* Required for std::vector<LogIOSegment::Buffer> */
LogIOSegment::Buffer::Buffer(LogIOSegment::Buffer &&obj) noexcept
    : buffer(obj.buffer), size(obj.size), stolen_buffer(obj.stolen_buffer) {
  obj.buffer        = nullptr;
  obj.size          = 0;
  obj.stolen_buffer = false;
}

LogIOSegment::Buffer::~Buffer() { Deallocate(); }

/**
 * We may call deallocate twice (if instant recovery is enabled).
 * This may cause double-free when compiling LeanStore with Release (-O3) mode.
 * Splitting destructor into two functions: normal destructor, ~LogIOSegment::Buffer() and Deallocate(),
 *  helps addressing this problem
 */
void LogIOSegment::Buffer::Deallocate() {
  if (buffer != nullptr && !stolen_buffer) {
    free(buffer);
    buffer = nullptr;
  }
}

void LogIOSegment::Buffer::ResizeBuffer(u64 new_size) {
  if (new_size > size) {
    buffer = reinterpret_cast<u8 *>(realloc(buffer, new_size));
    Ensure(buffer != nullptr);
  }
  size = new_size;
}

void LogIOSegment::Buffer::InsertNewLog(const u8 *log_buffer, u64 log_size) {
  auto prev_size = size;
  ResizeBuffer(size + log_size);
  std::memcpy(&buffer[prev_size], log_buffer, log_size);
}

auto LogIOSegment::Buffer::IsEmpty() -> bool { return size == 0; }

auto LogIOSegment::Buffer::Iterate(const std::function<bool(const LogEntry *)> &fn) const -> u64 {
  auto ptr = 0UL;
  auto ret = true;
  for (; (ptr < size) && ret;) {
    const auto log = reinterpret_cast<LogEntry *>(&buffer[ptr]);
    ret            = fn(log);
    ptr += log->size;
  }
  return ptr;
}

// -------------------------------------------------------------------------------------

LogIOSegment::LazyGenerator::Offset::Offset(LogIOSegment::Buffer *log, u64 offset) : log(log), offset(offset) {}

/* We're implementing min heap, so must reverse the order */
auto LogIOSegment::LazyGenerator::Offset::operator<=>(const Offset &other) const {
  auto llog = reinterpret_cast<const DataEntry *>(&(log->buffer[offset]));
  auto rlog = reinterpret_cast<const DataEntry *>(&(other.log->buffer[other.offset]));
  if (llog->rc.gsn < rlog->rc.gsn) { return 1; }
  if (llog->rc.gsn > rlog->rc.gsn) { return -1; }
  assert((llog->size == rlog->size) && (llog->chksum == rlog->chksum) && (std::memcmp(llog, rlog, llog->size) == 0));
  return 0;
}

void LogIOSegment::LazyGenerator::ReceiveInput(LogIOSegment::Buffer *w_segment) { tournament.emplace(w_segment); }

auto LogIOSegment::LazyGenerator::Iterate(const std::function<bool(const DataEntry *)> &fn) -> u64 {
  u64 count = 0;
  for (; !tournament.empty();) {
    Offset segment = tournament.top();  // Force copy
    tournament.pop();
    /* Eliminate duplicates */
    while (!tournament.empty()) {
      auto &other = tournament.top();
      if ((segment <=> other) != 0) { break; }
      /* Duplicate should only happen in other log segment */
      Offset new_off = other;
      tournament.pop();
      auto log = reinterpret_cast<DataEntry *>(&(new_off.log->buffer[new_off.offset]));
      if (new_off.log->size > new_off.offset + log->size) {
        tournament.emplace(new_off.log, new_off.offset + log->size);
      }
    }
    /* Process next log entry */
    count++;
    auto log = reinterpret_cast<DataEntry *>(&(segment.log->buffer[segment.offset]));
    auto ret = fn(log);
    if (!ret) { break; }
    if (segment.log->size > segment.offset + log->size) { tournament.emplace(segment.log, segment.offset + log->size); }
  }
  return count;
}

// -------------------------------------------------------------------------------------
// LogBackend

LogBackend::LogBackend(LogManager *log_manager, LogWorker *worker) : manager_(log_manager), worker_(worker) {}

void LogBackend::Connect() {
  int ret = io_uring_queue_init(LOGGING_AIO_QD, &ring_, storage::LibaioInterface::uring_flags);
  if (ret != 0) { throw ex::EnsureFailed("LogWorker: io_uring_queue_init error " + std::to_string(ret)); }
}

/**
 * @brief For autonomous commit protocol only.
 * Current worker synchronously write its local log buffer to the storage
 */
void LogBackend::SyncAppend(u8 *buffer, u64 size, const std::function<void()> &async_fn) {
  u64 start_time = 0;
  if (FLAGS_txn_debug && start_profiling_latency) { start_time = tsctime::ReadTSC(); }
  auto offset = LogFlushOffset(size);

  auto *sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    // The ring may still contain unsubmitted SQEs. Submit them and retry once; if the ring is
    // still full, the ring was not initialized/Connect() was not called.
    const int submitted = io_uring_submit(&ring_);
    if (submitted < 0) { throw ex::EnsureFailed("LogWorker: io_uring_submit error " + std::to_string(submitted)); }
    sqe = io_uring_get_sqe(&ring_);
    Ensure(sqe != nullptr);
  }
  io_uring_prep_write(sqe, manager_->wal_fd_, buffer, size, offset - size);
  UringSubmit(&ring_, 1, async_fn);

  if (FLAGS_txn_debug && start_profiling_latency && start_time > 0) {
    auto end_time = tsctime::ReadTSC();
    statistics::io_latency[LeanStore::worker_thread_id].emplace_back(tsctime::TscDifferenceNs(start_time, end_time));
  }
  if (FLAGS_txn_debug) { statistics::log_flush_cnt[LeanStore::worker_thread_id]++; }
}

auto LogBackend::LogFlushOffset(u64 chunk_size) -> u64 {
  assert((!cur_block_ || cur_block_->block_next_ >= cur_block_->block_base_) && (chunk_size % BLK_BLOCK_SIZE == 0));
  /* Every record in this chunk carries a GSN <= the writer's issuing GSN now. */
  auto chunk_gsn = worker_->GetCurrentGSN();

  if (!cur_block_ || cur_block_->block_next_ - cur_block_->block_base_ < chunk_size) {
    /* Current block is full: finalize it (publish its GSN watermark), then open
     * the next block from the ring registry. */
    if (cur_block_) {
      cur_block_->block_gsn = cur_block_max_gsn_;
      cur_block_->state.store(WALBlock::CLOSED, std::memory_order_release);
    }

    /* Move to the next WAL Block to append logs */
    cur_block_         = &manager_->WALBlocks().OpenNext();
    cur_block_max_gsn_ = 0;
  }
  cur_block_max_gsn_ = std::max(cur_block_max_gsn_, chunk_gsn);
  Ensure(cur_block_->block_next_ - cur_block_->block_base_ >= chunk_size);
  auto ret = cur_block_->block_next_;
  cur_block_->block_next_ -= chunk_size;  // advance descending write frontier
  return ret;
}

}  // namespace leanstore::recovery