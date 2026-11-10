#include "recovery/log_manager.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "sync/hybrid_guard.h"

#include "share_headers/time.h"

#include <memory>
#include <new>

namespace leanstore::recovery {

LogManager::LogManager(std::atomic<bool> &is_running)
    : no_commit_executor_(FLAGS_worker_count / FLAGS_txn_commit_group_size),
      commit_latches_(no_commit_executor_),
      worker_write_batch_size_(FLAGS_wal_batch_write_kb * KB),
      wal_block_size_(FLAGS_wal_block_size_mb * MB) {
  /* Initialize log I/O */
  wal_fd_ = open(FLAGS_db_path.c_str(), O_WRONLY | O_DIRECT, S_IRWXU);
  Ensure(wal_fd_ > 0);
  w_offset_ = StorageCapacity(FLAGS_db_path.c_str()) - BLK_BLOCK_SIZE;

  /* Carve out the circular WAL region (DB file tail) into wal_block_size_ blocks.
   * File layout: | DB content | ... | WAL content (wal_size_gb) | 4KB master record |
   * The tail 4KB (i.e., BLK_BLOCK_SIZE) is the master record (see WriteMasterRecord).
   * It is written at the file tail before any WAL and w_offset_ is lowered past it, so
   * the region must sit below it or the last block's physical write would
   * underflow the region on ring wrap. */
  auto wal_region_end   = w_offset_.load() - BLK_BLOCK_SIZE;
  auto wal_region_start = wal_region_end - FLAGS_wal_size_gb * GB;
  auto num_wal_blocks   = static_cast<u32>((wal_region_end - wal_region_start) / wal_block_size_);
  Ensure(num_wal_blocks > 0);
  /* All blocks must fit inside the WAL region: every WAL entry lives within it. */
  Ensure(num_wal_blocks * wal_block_size_ <= wal_region_end - wal_region_start);
  wal_region_start_ = wal_region_start;
  /* Pass w_offset_ (not wal_region_end) as the block region end so that block 0
   * sits just below the master record, matching the recovery reader layout. */
  wal_blocks_ = std::make_unique<WALBlockManager>(num_wal_blocks, wal_block_size_, w_offset_.load());

  /* Initialize per-worker published state */
  w_last_gsn_ = new CacheLineAtomic[FLAGS_worker_count];

  /* Initialize local log workers */
  logger_ = static_cast<LogWorker *>(calloc(FLAGS_worker_count, sizeof(LogWorker)));
  for (auto idx = 0U; idx < FLAGS_worker_count; idx++) {
    new (&logger_[idx]) LogWorker(is_running, this);
    logger_[idx].w_last_gsn = &w_last_gsn_[idx].value;
  }

  /* Initialize group commit context */
  gc_ = static_cast<GroupCommitExecutor *>(calloc(no_commit_executor_, sizeof(GroupCommitExecutor)));
}

LogManager::~LogManager() {
  for (auto idx = 0U; idx < FLAGS_worker_count; idx++) { logger_[idx].~LogWorker(); }
  free(logger_);
  for (auto idx = 0U; idx < no_commit_executor_; idx++) { gc_[idx].~GroupCommitExecutor(); }
  free(gc_);
  delete[] w_last_gsn_;
}

auto LogManager::LocalLogWorker() -> LogWorker & {
  assert(LeanStore::worker_thread_id < FLAGS_worker_count);
  return logger_[LeanStore::worker_thread_id];
}

auto LogManager::WALOffset() -> std::atomic<u64> & { return w_offset_; }

auto LogManager::WALBlocks() -> WALBlockManager & { return *wal_blocks_; }

void LogManager::InitializeCommitExecutor(buffer::BufferManager *buffer) {
  for (auto idx = 0U; idx < no_commit_executor_; idx++) {
    auto start_wid = idx * FLAGS_txn_commit_group_size;
    auto end_wid   = std::min(FLAGS_worker_count, start_wid + FLAGS_txn_commit_group_size);
    LOG_INFO("Group commit {}: worker [{}..{}]", idx, start_wid, end_wid - 1);
    new (&gc_[idx]) GroupCommitExecutor(buffer, this, start_wid, end_wid);
  }
  buffer_manager_ = buffer;
  if (FLAGS_wal_enable_checkpoint) { checkpointer_ = std::make_unique<Checkpointer>(buffer, this); }
}

auto LogManager::CommitExecutor(u32 commit_idx) -> GroupCommitExecutor & { return gc_[commit_idx]; }

auto LogManager::NumberOfCommitExecutor() -> u32 { return no_commit_executor_; }

void LogManager::TriggerGroupCommit(u32 commit_idx) {
  if (commit_latches_[commit_idx].TryLockExclusive()) {
    sync::HybridGuard guard(&(commit_latches_[commit_idx]), sync::GuardMode::ADOPT_EXCLUSIVE);
    gc_[commit_idx].ExecuteOneRound();
  }
}

void LogManager::TriggerCheckpoint() {
  if (checkpointer_) { checkpointer_->TryCheckpoint(); }
}

void LogManager::BootstrapCheckpoint() {
  if (checkpointer_) {
    checkpointer_->Bootstrap();
    /* Done bootstraping, allowing checkpointer thread to run */
    checkpointer_->is_bootstraping_ = false;
  }
}

/* To be called from checkpointing thread: Whether the system is still doing recovery */
auto LogManager::CheckpointIsBootstraping() -> bool {
  if (checkpointer_) { return checkpointer_->is_bootstraping_.load(); }
  return false;
}

/**
 * @brief Write a dummy log entry (MasterRecord) here, which contains newest signature,
 *  used to differentiate between previous LeanStore session and current session.
 */
void LogManager::WriteMasterRecord() {
  /* Generate MASTER_RECORD log entry */
  auto start_tsctime           = tsctime::ReadTSC();
  LeanStore::signature         = ComputeCRC(reinterpret_cast<u8 *>(&start_tsctime), sizeof(u64));
  auto buffer                  = AlignedPtr<BLK_BLOCK_SIZE>(BLK_BLOCK_SIZE);
  auto master_record           = new (buffer.get()) MasterRecordEntry();
  master_record->type          = LogEntry::Type::MASTER_RECORD;
  master_record->size          = sizeof(MasterRecordEntry);
  master_record->rc.gsn        = 1;
  master_record->rc.chunk_size = BLK_BLOCK_SIZE;
  master_record->rc.signature  = LeanStore::signature;

  /* Persist session config so recovery can read the prior session's WAL. */
  master_record->num_workers       = FLAGS_worker_count;
  master_record->wal_size_gb       = FLAGS_wal_size_gb;
  master_record->wal_block_size_mb = FLAGS_wal_block_size_mb;

  if (FLAGS_wal_debug) { master_record->ComputeChksum(); }

  /* Persist master record */
  auto master_rec_offset = StorageCapacity(FLAGS_db_path.c_str());
  assert(master_rec_offset % BLK_BLOCK_SIZE == 0);
  [[maybe_unused]] auto cnt = pwrite(wal_fd_, buffer.get(), BLK_BLOCK_SIZE, master_rec_offset - BLK_BLOCK_SIZE);
  assert(cnt == BLK_BLOCK_SIZE);
  if (FLAGS_wal_fsync) { fdatasync(wal_fd_); }

  /* Done initializing log manager, starting checkpoint */
  if (checkpointer_) { checkpointer_->is_bootstraping_ = false; }
}

}  // namespace leanstore::recovery