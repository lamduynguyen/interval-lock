#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "recovery/checkpoint.h"
#include "recovery/group_commit.h"
#include "recovery/log_backend.h"
#include "recovery/log_entry.h"
#include "recovery/log_worker.h"
#include "recovery/recovery_manager.h"

#include "gtest/gtest_prod.h"

#include <atomic>
#include <memory>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::transaction {
class TransactionManager;
}

namespace leanstore::recovery {

class GroupCommitExecutor;

class LogManager {
 public:
  /* The minimum GSN up to which all logs from all workers have been flushed, used for RFA variant */
  inline static std::atomic<timestamp_t> global_min_gsn_flushed = 0;
  /* Increment the workers' GSN to this value periodically to prevent local GSN from skewing and undermining RFA */
  inline static std::atomic<timestamp_t> global_sync_to_this_gsn = 0;

  explicit LogManager(std::atomic<bool> &is_running);
  ~LogManager();

  /* Child components: Local logger and group commit*/
  auto NumberOfCommitExecutor() -> u32;
  auto CommitExecutor(u32 commit_idx) -> GroupCommitExecutor &;
  auto LocalLogWorker() -> LogWorker &;

  /* Utilities */
  void InitializeCommitExecutor(buffer::BufferManager *buffer);
  void TriggerGroupCommit(u32 commit_idx);
  auto WALOffset() -> std::atomic<u64> &;
  void WriteMasterRecord();

  /* Checkpointing utilities */
  void TriggerCheckpoint();
  void BootstrapCheckpoint();
  auto CheckpointIsBootstraping() -> bool;

  /* WAL block registry (checkpointing support) */
  auto WALBlocks() -> WALBlockManager &;

 private:
  friend class transaction::TransactionManager;
  friend class GroupCommitExecutor;
  friend struct LogWorker;
  friend struct LogBuffer;
  friend class LogBackend;
  friend class Checkpointer;
  friend class TestTreeRecovery;
  friend class TestBlobRecovery;
  friend class TestCheckpointer;
  FRIEND_TEST(TestLogManager, CentralizedLogging);
  FRIEND_TEST(TestRecovery, SmallChunks);
  FRIEND_TEST(TestRecovery, LargeChunk);
  FRIEND_TEST(TestTreeRecovery, BulkInsertRecovery);
  FRIEND_TEST(TestTreeRecovery, MixWorkloads);

  /* I/O for the log */
  int wal_fd_;
  std::atomic<u64> w_offset_;

  /* Circular WAL region bounds & block registry */
  std::unique_ptr<WALBlockManager> wal_blocks_;
  u64 wal_region_start_{0}; /* first byte of the WAL region; all WAL writes stay at/above it */

  /* Local loggers and the group commit executor */
  LogWorker *logger_;
  GroupCommitExecutor *gc_;
  u32 no_commit_executor_;

  /* Fuzzy checkpointer (created by InitializeCommitExecutor), driven by a dedicated
   * background thread owned by LeanStore (see LeanStore::StartCheckpointerThread). */
  buffer::BufferManager *buffer_manager_ = nullptr;
  std::unique_ptr<Checkpointer> checkpointer_;

  /* For baseline commit & WILO steal, whoever worker acquires X-latch can run group commit */
  std::vector<sync::HybridLatch> commit_latches_;

  /* Autonomous commit configuration */
  const u64 worker_write_batch_size_; /* Workers trigger log flush when its dirty logs reach this size */
  const u64 wal_block_size_; /* Workers flush log in blocks, i.e. fill block continuously then request a new block */

  /* Logger environments */
  CacheLineAtomic *w_last_gsn_ = nullptr;
};

}  // namespace leanstore::recovery
