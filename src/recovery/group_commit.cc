#include "recovery/group_commit.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "transaction/transaction_manager.h"

#include "share_headers/time.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <tuple>

namespace leanstore::recovery {

GroupCommitExecutor::GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager, u32 start_wid,
                                         u32 end_wid)
    : buffer_(buffer),
      log_manager_(log_manager),
      start_logger_id_(start_wid),
      end_logger_id_(end_wid),
      ready_to_commit_cut_(FLAGS_worker_count),
      last_gsn_snapshot_(FLAGS_worker_count) {
  int ret = io_uring_queue_init(GROUP_COMMIT_QD, &ring_, 0);
  if (ret != 0) { throw std::runtime_error("GroupCommit: io_uring_queue_init error"); }
}

/**
 * @brief Reset GroupCommitExecutor's state every round
 */
void GroupCommitExecutor::InitializeRound() {
  lp_req_.clear();
  already_prep_.clear();
  submitted_io_cnt_    = 0;
  completed_txn_       = 0;
  aborted_txn_         = 0;
  committed_txn_       = 0;
  min_all_workers_gsn_ = std::numeric_limits<timestamp_t>::max();
  max_all_workers_gsn_ = 0;
}

void GroupCommitExecutor::CompleteRound() {
  // Update RFA env
  UpdateMax(LogManager::global_min_gsn_flushed, min_all_workers_gsn_);
  UpdateMax(LogManager::global_sync_to_this_gsn, max_all_workers_gsn_);

  // Update statistics
  if (start_profiling) {
    assert(completed_txn_ == committed_txn_ + aborted_txn_);
    statistics::commit_rounds[LeanStore::worker_thread_id]++;
    statistics::txn_per_round[LeanStore::worker_thread_id].emplace_back(completed_txn_);
    statistics::aborted_txn[LeanStore::worker_thread_id] += aborted_txn_;
    statistics::committed_txn[LeanStore::worker_thread_id] += committed_txn_;
    if (FLAGS_txn_debug) {
      statistics::recovery::gct_time_ns[LeanStore::worker_thread_id] +=
        tsctime::TscDifferenceNs(round_begin_, round_end_);
    }
  }
}

/**
 * @brief Phase description:
 *
 * - Phase one: Write the current log buffer to the storage
 * - Phase two: Flush (fdatasync) all logs to disk and acknowledge all log entries are flushed to the storage
 * - Phase three: Write all BLOBs of transactions to the storage
 * - Phase four: Flush those BLOBs and mark them ok for evicted
 * - Phase five: Notify which transactions are committed according to RFA
 */
void GroupCommitExecutor::ExecuteOneRound() {
  /* This is also the shutdown drain path: workers still finishing their last commit may push
   * into a full precommit queue, so the consumer must keep running after is_running turns false. */
  InitializeRound();
  if (FLAGS_txn_debug) { round_begin_ = tsctime::ReadTSC(); }
  PhaseOne();
  if (FLAGS_blob_enable) { PhaseTwo(); }
  PhaseThree();
  if (FLAGS_txn_debug) { round_end_ = tsctime::ReadTSC(); }
  CompleteRound();
}

// -------------------------------------------------------------------------------------

/**
 * @brief Collect committed state of all workers.
 * If group commit/flush pipelining, write logs using async I/O
 */
void GroupCommitExecutor::PhaseOne() {
  for (auto w_i = start_logger_id_; w_i < end_logger_id_; w_i++) { CollectPrecommittedQueue(w_i); }
  for (auto w_i = 0UL; w_i < FLAGS_worker_count; w_i++) {
    auto last_gsn           = log_manager_->w_last_gsn_[w_i].value.load(std::memory_order_acquire);
    last_gsn_snapshot_[w_i] = last_gsn;
    min_all_workers_gsn_    = std::min<timestamp_t>(min_all_workers_gsn_, last_gsn);
    max_all_workers_gsn_    = std::max<timestamp_t>(max_all_workers_gsn_, last_gsn);
  }

  /* Fsync() if required */
  if (FLAGS_wal_fsync) { Fsync(); }
}

/**
 * @brief LargePage phase - writing and persisting all extents of transactions
 */
void GroupCommitExecutor::PhaseTwo() {
  Ensure(FLAGS_blob_enable);
  for (size_t w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
    auto &logger = log_manager_->logger_[w_i];
    /**
     * @brief Persist all BLOBs for all transactions
     */
    logger.precommitted_queue.LoopElements(ready_to_commit_cut_[w_i], [&](auto &txn) {
      if (SatisfyCommitConditions(w_i, txn)) {
        PrepareLargePageWrite(txn);
        return true;
      }
      return false;
    });
  }

  if (!lp_req_.empty()) {
    Ensure(submitted_io_cnt_ > 0);
    UringSubmit(&ring_, submitted_io_cnt_);
    submitted_io_cnt_ = 0;
    if (FLAGS_wal_fsync) { Fsync(); }

    for (auto [start_pid, pg_cnt, extent_pid] : lp_req_) {
      if (!completed_lp_.contains(start_pid)) {
        completed_lp_.add(start_pid);
        buffer_->EvictExtent(extent_pid);
      }
    }
  }
}

void GroupCommitExecutor::PhaseThree() {
  for (size_t w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
    auto &logger = log_manager_->logger_[w_i];

    auto loop_bytes = logger.precommitted_queue.LoopElements(ready_to_commit_cut_[w_i], [&](auto &txn) {
      if (SatisfyCommitConditions(w_i, txn)) {
        completed_txn_++;
        CompleteTransaction(txn);
        return true;
      }
      return false;
    });
    if (loop_bytes > 0) { logger.precommitted_queue.Erase(loop_bytes); }
  }
}

// -------------------------------------------------------------------------------------

void GroupCommitExecutor::Fsync() { fdatasync(log_manager_->wal_fd_); }

void GroupCommitExecutor::CollectPrecommittedQueue(wid_t w_i) {
  auto &logger              = log_manager_->logger_[w_i];
  ready_to_commit_cut_[w_i] = logger.precommitted_queue.CurrentTail();
}

void GroupCommitExecutor::PrepareWrite(u8 *src, size_t size, size_t offset) {
  Ensure(u64(src) % BLK_BLOCK_SIZE == 0);
  Ensure(size % BLK_BLOCK_SIZE == 0);
  Ensure(offset % BLK_BLOCK_SIZE == 0);
  auto sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    UringSubmit(&ring_, submitted_io_cnt_);
    sqe = io_uring_get_sqe(&ring_);
    Ensure(sqe != nullptr);
    submitted_io_cnt_ = 0;
  }
  submitted_io_cnt_++;
  io_uring_prep_write(sqe, log_manager_->wal_fd_, src, size, offset);
}

void GroupCommitExecutor::PrepareLargePageWrite(const transaction::SerializableTransaction &txn) {
  Ensure(txn.ToFlushedLargePages().size() == txn.ToEvictedExtents().size());

  for (auto &[pid, pg_cnt] : txn.ToFlushedLargePages()) {
    if (!already_prep_.contains(pid)) {
      already_prep_.add(pid);
      if (FLAGS_blob_normal_buffer_pool) {
        for (auto id = pid; id < pid + pg_cnt; id++) {
          PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(id)), PAGE_SIZE, id * PAGE_SIZE);
        }
      } else {
        PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(pid)), pg_cnt * PAGE_SIZE, pid * PAGE_SIZE);
      }
    }
  }

  /* Update large-page requirements for txn commit */
  for (size_t idx = 0; idx < txn.ToFlushedLargePages().size(); idx++) {
    auto &[pid, pg_cnt] = txn.ToFlushedLargePages()[idx];
    auto &extent_pid    = txn.ToEvictedExtents()[idx];
    if (start_profiling) { statistics::blob::blob_logging_io += pg_cnt * PAGE_SIZE; }
    lp_req_.emplace_back(pid, pg_cnt, extent_pid);
  }
}

void GroupCommitExecutor::CompleteTransaction(transaction::SerializableTransaction &txn) {
  Ensure((txn.state == transaction::Transaction::State::READY_TO_COMMIT) ||
         (txn.state == transaction::Transaction::State::ABORTED));
  if (txn.state == transaction::Transaction::State::ABORTED) {
    aborted_txn_++;
  } else {
    committed_txn_++;
  }
  if (FLAGS_blob_enable) {
    for (auto &lp : txn.ToFlushedLargePages()) { completed_lp_.remove(lp.start_pid); }
    buffer_->FreeStorageManager()->FreeExtents(txn.ToFreeExtents());
  }
  transaction::TransactionManager::DurableCommit(txn, round_begin_);
}

auto GroupCommitExecutor::SatisfyCommitConditions(wid_t w_i, const transaction::SerializableTransaction &txn) -> bool {
  /**
   * @brief With GSN Vector variant, commit conditions is:
   * - If read only, automatically durable this point
   * - The txn's commit (or abort) log entry is flushed
   * - Dependencies' GSN are lower than the durable GSN of those workers
   */
  if (txn.is_read_only) { return true; }
  if (txn.commit_gsn > last_gsn_snapshot_[w_i]) { return false; }
  for (auto idx = 0UL; idx < txn.vector_size; idx++) {
    auto wid = txn.Dependencies()[idx];
    if (last_gsn_snapshot_[wid] < txn.DepGSN()[idx]) { return false; }
  }
  return true;
}

}  // namespace leanstore::recovery
