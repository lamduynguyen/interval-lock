#pragma once

#include "buffer/buffer_manager.h"
#include "leanstore/kv_interface.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace leanstore::transaction {

class TransactionManager {
 public:
  static thread_local timestamp_t previous_completed_time;
  inline static std::atomic<timestamp_t> global_clock = 1;  // Valid timestamp always >= 1

  TransactionManager(buffer::BufferManager *buffer_manager, recovery::LogManager *log_manager);
  ~TransactionManager();

  static auto ParseIsolationLevel(const std::string &str) -> IsolationLevel;

  /// The lock word a freshly stored record starts with.
  /// Only has effect under SERIALIZABLE + for BASELINE_INLINE.
  static auto FreshRecordLockByte() -> u8 {
    return (FLAGS_txn_locking_variant == static_cast<u32>(LockManagerVariant::BASELINE_INLINE) &&
            Transaction::active_txn.state != Transaction::State::IDLE &&
            Transaction::active_txn.iso_level == IsolationLevel::SERIALIZABLE)
             ? 255
             : 0;
  }

  /// `predefined_start_ts = 0` takes a fresh ts from global_clock;
  /// otherwise, it means a restarted txn with same start_ts.
  void StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time = 0,
                        IsolationLevel next_tx_isolation_level = ParseIsolationLevel(FLAGS_txn_default_isolation_level),
                        Transaction::Mode next_tx_mode = Transaction::Mode::OLTP, timestamp_t predefined_start_ts = 0);
  void CommitTransaction();
  void AbortTransaction();

  template <class T>
  static void DurableCommit(T &txn, timestamp_t queue_phase_start);

 private:
  friend class LeanStore;
  friend class Transaction;
  void QueueTransaction(Transaction &txn);

  buffer::BufferManager *buffer_;
  recovery::LogManager *log_manager_;
  std::unique_ptr<LockManagerInterface> lock_manager_;
};

}  // namespace leanstore::transaction
