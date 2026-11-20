#include "transaction/transaction_manager.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"
#include "storage/btree/tree.h"
#include "transaction/lock_manager/faster_lock_manager.h"
#include "transaction/lock_manager/inline_lock_manager.h"
#include "transaction/lock_manager/precision_lock_manager.h"
#include "transaction/lock_manager/staged_lock_manager.h"

#include "share_headers/time.h"

#include <chrono>
#include <stdexcept>
#include <thread>
#include <vector>

using LogManager = leanstore::recovery::LogManager;
using LogEntry   = leanstore::recovery::LogEntry;
using DataEntry  = leanstore::recovery::DataEntry;

namespace leanstore::transaction {

thread_local timestamp_t TransactionManager::previous_completed_time = 0;

TransactionManager::TransactionManager(buffer::BufferManager *buffer_manager, LogManager *log_manager)
    : buffer_(buffer_manager), log_manager_(log_manager) {
  switch (static_cast<LockManagerVariant>(FLAGS_txn_locking_variant)) {
    case LockManagerVariant::BASELINE_FASTER: lock_manager_ = std::make_unique<FasterLockManager>(); break;
    case LockManagerVariant::BASELINE_STAGED: lock_manager_ = std::make_unique<StagedLockManager>(); break;
    case LockManagerVariant::BASELINE_INLINE: lock_manager_ = std::make_unique<InlineLockManager>(buffer_); break;
    case LockManagerVariant::PRECISION_RANGE:
      lock_manager_ = std::make_unique<PrecisionLockManager>(
        buffer_,
        [](leng_t tree_id, std::span<const u8> lo, std::span<const u8> hi, const std::function<void(pageid_t)> &visit) {
          reinterpret_cast<storage::BTree *>(catalog[tree_id])->VisitLeaves(lo, hi, visit);
        });
      break;
    default: throw std::runtime_error("TransactionManager: unknown lock manager variant");
  }
};

TransactionManager::~TransactionManager() {}

auto TransactionManager::ParseIsolationLevel(const std::string &str) -> IsolationLevel {
  if (str == "ser") { return IsolationLevel::SERIALIZABLE; }
  Ensure(str == "nl");
  return IsolationLevel::NO_LOCK;
}

void TransactionManager::StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time,
                                          IsolationLevel next_tx_isolation_level, Transaction::Mode next_tx_mode,
                                          timestamp_t predefined_start_ts) {
  auto &txn = Transaction::active_txn;
  Ensure(!txn.IsRunning());
  txn.Initialize(this, predefined_start_ts > 0 ? predefined_start_ts : global_clock++, next_tx_type,
                 next_tx_isolation_level, next_tx_mode);
  if (FLAGS_txn_debug) {
    txn.stats.start    = tsctime::ReadTSC();
    const auto arrival = (next_tx_arrival_time > 0) ? next_tx_arrival_time : txn.stats.start;
    assert(next_tx_arrival_time <= txn.stats.start);
    statistics::worker_idle_ns[LeanStore::worker_thread_id][Rand(SAMPLING_SIZE)] = arrival - previous_completed_time;
  }
  // Propagate WAL-related run-time context for this active transaction
  auto &logger          = log_manager_->LocalLogWorker();
  const auto sync_point = LogManager::global_sync_to_this_gsn.load();
  if (sync_point > logger.GetCurrentGSN()) { logger.SetCurrentGSN(sync_point); }

  logger.rfa_gsn_flushed = LogManager::global_min_gsn_flushed.load();
}

void TransactionManager::CommitTransaction() {
  auto &logger = log_manager_->LocalLogWorker();
  auto &txn    = Transaction::active_txn;
  Ensure(txn.state == Transaction::State::STARTED);

  // Assign timestamp and Release locks according to Concurrency Control
  txn.commit_ts = global_clock++;
  if (txn.iso_level > IsolationLevel::NO_LOCK) {
    lock_manager_->ReleaseAllLocks(txn.start_ts, {});
    assert(lock_manager_->EmptyLocalSet());
  }

  // Update transactional context of current txn
  txn.state = Transaction::State::READY_TO_COMMIT;
  if (FLAGS_txn_debug) {
    txn.stats.precommit     = tsctime::ReadTSC();
    previous_completed_time = txn.stats.precommit;
  }

  if (FLAGS_wal_enable) {
    bool should_commit = false;
    if (!txn.ReadOnly()) {
      auto &entry       = logger.ReserveLogCommitEntry(txn.SerializedVectorSize());
      entry.vector_size = txn.gsn_vector.size();
      txn.commit_gsn    = entry.rc.gsn;
      should_commit     = logger.SubmitActiveLogEntry();
    } else {
      // Periodically flush leftover write logs so blocked queued txns can make progress.
      if ((FastXorBasedRandom() & 255) == 0) { logger.log_buffer.LogFlush(&logger); }
      txn.commit_gsn = logger.w_last_gsn->load(std::memory_order_acquire);
    }

    // Push the txn to the pre-commit queue
    QueueTransaction(txn);

    // Try to trigger group commit directly within the worker
    if (should_commit || (FastXorBasedRandom() % (static_cast<u32>(BitLength(FLAGS_worker_count + 1)) * 2) == 0)) {
      log_manager_->TriggerGroupCommit(LeanStore::worker_thread_id / FLAGS_txn_commit_group_size);
    }
  }

  // If log is disabled, update the statistics manually
  if (!FLAGS_wal_enable) {
    DurableCommit(txn, txn.stats.precommit);
    if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
  }
}

void TransactionManager::AbortTransaction() {
  auto &txn    = Transaction::active_txn;
  auto &logger = log_manager_->LocalLogWorker();
  Ensure(txn.IsRunning());

  // Iterate through lock_manager.write_set_ and re-install undo payload
  // If the undo payload is empty -> prev version does not exist, call tree delete
  // Otherwise, call tree update
  txn.commit_ts = global_clock++;
  if (txn.iso_level > IsolationLevel::NO_LOCK) {
    lock_manager_->ReleaseAllLocks(txn.start_ts, [&](const LockableTuple *tuple, std::span<const u8> payload) {
      auto index                = reinterpret_cast<storage::BTree *>(catalog[tuple->tree_id]);
      [[maybe_unused]] auto ret = (payload.empty())
                                    ? index->Remove({const_cast<u8 *>(tuple->key), tuple->key_len})
                                    : index->Upsert({const_cast<u8 *>(tuple->key), tuple->key_len}, payload);
      // Removing a row that is not there is a no-op, not a failure.
      assert(ret == OpResult::OK || (payload.empty() && ret == OpResult::NOT_FOUND));
    });
  }
  assert(lock_manager_->EmptyLocalSet());

  // TODO(XXX): To fully support recovery, need to generate CLRs (compensation log record) here
  // before generating TX_ABORT log entry

  // Now, mark the transaction as aborted
  txn.state = Transaction::State::ABORTED;
  if (FLAGS_txn_debug) {
    txn.stats.precommit     = tsctime::ReadTSC();
    previous_completed_time = txn.stats.precommit;
  }

  // Append txn object to the pre-commit queue, i.e., similar to CommitTransaction()
  if (FLAGS_wal_enable) {
    bool should_commit = false;
    if (!txn.ReadOnly()) {
      auto &entry       = logger.ReserveLogCommitEntry(txn.SerializedVectorSize());
      entry.vector_size = txn.gsn_vector.size();
      entry.type        = LogEntry::Type::TX_ABORT;
      txn.commit_gsn    = entry.rc.gsn;
      should_commit     = logger.SubmitActiveLogEntry();
    } else {
      // Periodically flush leftover write logs so blocked queued txns can make progress.
      if ((FastXorBasedRandom() & 255) == 0) { logger.log_buffer.LogFlush(&logger); }
      txn.commit_gsn = logger.w_last_gsn->load(std::memory_order_acquire);
    }

    // Push the txn to the pre-commit queue
    QueueTransaction(txn);

    // Try to trigger group commit directly within the worker
    if (should_commit || (FastXorBasedRandom() % (static_cast<u32>(BitLength(FLAGS_worker_count + 1)) * 2) == 0)) {
      log_manager_->TriggerGroupCommit(LeanStore::worker_thread_id / FLAGS_txn_commit_group_size);
    }
  }

  // if WAl is disabled, simply ack the txn
  if (!FLAGS_wal_enable) {
    if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
  }
}

template <class T>
void TransactionManager::DurableCommit(T &txn, timestamp_t queue_phase_start) {
  txn.state = transaction::Transaction::State::COMMITTED;
  if (FLAGS_txn_debug) {
    auto commit_stats = tsctime::ReadTSC();
    if (start_profiling_latency) {
      statistics::txn_queue[LeanStore::worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.precommit, queue_phase_start));
      statistics::txn_latency[LeanStore::worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.start, commit_stats));
      statistics::txn_exec[LeanStore::worker_thread_id].push_back(
        tsctime::TscDifferenceNs(txn.stats.start, txn.stats.precommit));
    }
  }
}

void TransactionManager::QueueTransaction(Transaction &txn) {
  assert(FLAGS_wal_enable);
  auto &logger = log_manager_->LocalLogWorker();

  /* Enabling lock-free queue */
  logger.precommitted_queue.Push(txn);
  if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
}

template void TransactionManager::DurableCommit<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &, timestamp_t);
template void TransactionManager::DurableCommit<transaction::Transaction>(transaction::Transaction &, timestamp_t);

}  // namespace leanstore::transaction
