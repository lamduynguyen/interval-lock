#include "recovery/recovery_backend.h"
#include "common/exceptions.h"
#include "leanstore/leanstore.h"
#include "recovery/log_backend.h"

namespace leanstore::recovery {

RecoveryBackend::RecoveryBackend(buffer::BufferManager *buffer)
    : buffer_(buffer),
      wal_block_size_(FLAGS_wal_block_size_mb * MB),
      retries_per_worker_((FLAGS_worker_count + FLAGS_wal_recovery_threads - 1) / FLAGS_wal_recovery_threads) {}

void RecoveryBackend::Connect() {
  wal_fd_ = open(FLAGS_db_path.c_str(), O_RDONLY | O_DIRECT, S_IRWXU);
  Ensure(wal_fd_ > 0);
  wal_offset_         = StorageCapacity(FLAGS_db_path.c_str());
  wal_offset_initial_ = wal_offset_.load();
}

void RecoveryBackend::RetrieveMasterRecord() {
  auto buffer = AlignedPtr<BLK_BLOCK_SIZE>(BLK_BLOCK_SIZE);

  auto cnt = pread(wal_fd_, buffer.get(), BLK_BLOCK_SIZE, wal_offset_ - BLK_BLOCK_SIZE);
  Ensure(cnt == BLK_BLOCK_SIZE);
  auto master = reinterpret_cast<LogMetaEntry *>(buffer.get());
  Ensure(master->type == LogEntry::Type::MASTER_RECORD);
  if (FLAGS_wal_debug) { master->ValidateChksum(); }
  signature_ = master->rc.signature;
  assert(signature_ > 0);
  wal_offset_ -= BLK_BLOCK_SIZE;

  /* Read the previous session's config from the master record. These let
   * recovery read the prior session's WAL regardless of the current FLAGS_wal_*
   * configuration. */
  auto master_full        = reinterpret_cast<MasterRecordEntry *>(buffer.get());
  prev_worker_count_      = master_full->num_workers;
  prev_wal_size_gb_       = master_full->wal_size_gb;
  prev_wal_block_size_mb_ = master_full->wal_block_size_mb;
  Ensure(prev_wal_size_gb_ > 0 && prev_wal_block_size_mb_ > 0);

  /* Use the previous session's block size when reading its WAL. */
  wal_block_size_ = prev_wal_block_size_mb_ * MB;
  if (prev_worker_count_ > 0 && prev_worker_count_ < MAX_NUMBER_OF_WORKER) {
    retries_per_worker_ = (prev_worker_count_ + FLAGS_wal_recovery_threads - 1) / FLAGS_wal_recovery_threads;
  }
}

/**
 * @brief For SSD backend, `offset` is the on-SSD offsets
 * For SLS, `offset` means the SLS's Log sequence numbers
 * For Bookkeeper, `offset` means log entry ID
 */
auto RecoveryBackend::ReadLogBlocks(
  const std::function<void(u8 *buf, u64 len, u32 signature, u64 offset)> &materialize_fn) -> u64 {
  /* Log entries should always be smaller than `wal_block_size_` with whatever log backend */
  auto buffer = AlignedPtr<BLK_BLOCK_SIZE>(wal_block_size_);
  Ensure(prev_wal_size_gb_ > 0);
  Ensure(wal_offset_initial_ >= static_cast<u64>(prev_wal_size_gb_) * GB);

  /* Lower bound: don't read below the start of the previous session's WAL region. */
  const u64 lower_bound = wal_offset_initial_ - static_cast<u64>(prev_wal_size_gb_) * GB;

  // Last SSD offset that refers to valid log block (according to signature)
  u64 last_valid_offset = std::numeric_limits<u64>::max();
  auto invalid_blocks   = 0U;
  while (invalid_blocks < retries_per_worker_) {
    auto rd_offset = wal_offset_.fetch_sub(wal_block_size_) - wal_block_size_;
    if (rd_offset < lower_bound) { break; }
    auto rd = pread(wal_fd_, buffer.get(), wal_block_size_, rd_offset);
    Ensure(rd == static_cast<i64>(wal_block_size_));
    auto block_metadata = reinterpret_cast<LogMetaEntry *>(&(buffer.get())[rd - sizeof(LogMetaEntry)]);
    LOG_DEBUG("Worker {} to analyze offset {} MB, required signature {}, found {}", LeanStore::worker_thread_id,
              rd_offset / MB, signature_, block_metadata->rc.signature);
    if (block_metadata->rc.signature != signature_) {
      LOG_DEBUG("Worker {} stops: required signature {}, found {}, retries_per_worker_{}", LeanStore::worker_thread_id,
                block_metadata->rc.signature, signature_, retries_per_worker_);
      invalid_blocks++;
      continue;
    }
    last_valid_offset = rd_offset;
    materialize_fn(buffer.get(), wal_block_size_, signature_, rd_offset);
  }

  return last_valid_offset;
}

}  // namespace leanstore::recovery