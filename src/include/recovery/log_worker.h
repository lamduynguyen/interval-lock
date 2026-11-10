#pragma once

#include "common/constants.h"
#include "common/queue.h"
#include "common/typedefs.h"
#include "recovery/log_backend.h"
#include "recovery/log_buffer.h"
#include "recovery/log_entry.h"
#include "transaction/transaction.h"

#include "liburing.h"

#include <atomic>
#include <functional>
#include <mutex>

namespace leanstore::recovery {

class LogManager;

struct LogWorker {
  // Environments
  LogManager *log_manager;
  std::atomic<bool> *is_running        = nullptr;
  LogEntry *active_log                 = nullptr;
  std::atomic<timestamp_t> *w_last_gsn = nullptr;  // this worker's slot in LogManager

  /* Timestamp management */
  timestamp_t w_gsn_clock     = 0;  // local copy of gsn, i.e. w_gsn_clock <= gsn.load()
  timestamp_t rfa_gsn_flushed = 0;  // local copy of LogManager.global_min_gsn_flushed

  /* Log buffer */
  LogBuffer log_buffer;
  LogBackend backend;

  // SPSC lock-free queue, sharing between 1 group commit thread and 1 worker
  LockFreeQueue<transaction::SerializableTransaction> precommitted_queue;

  LogWorker(std::atomic<bool> &db_is_running, LogManager *log_manager);
  ~LogWorker();

  // LogSequenceNumber utilities
  auto GetCurrentGSN() -> timestamp_t;
  void SetCurrentGSN(timestamp_t gsn);

  /**
   * @brief Data log entries are usually bigger & generated more frequently than other log types.
   * In WILO_SHARE_BUFFER design, a worker may wait for its peer to write its log buffer to the storage.
   * This causes performance issue, considering that (-> means depends on the completion of next op)
   * Optimistic Latch
   *    -> Exclusive Latch
   *    -> Log generation (WAL_RECORD in tree.cc)
   *    -> EnsureEnoughSpace() -> Log write or wait for log write
   *
   * Therefore, we should provide an alternative life cycle for DataLog,
   *  which decouple Exclusive Latch and Log generation.
   */
  auto PrepareDataLogEntry(u8 *buffer, u64 payload_size, pageid_t pid) -> DataEntry &;
  void SubmitPreparedLogEntry();

  // Log buffer utilities
  void WorkerWritesLog();

  // Log entry utilities
  auto ReserveLogMetaEntry() -> LogMetaEntry &;
  auto ReserveLogCommitEntry(u64 payload_size) -> TxnCommitEntry &;
  auto ReserveDataLog(u64 payload_size, pageid_t pid) -> DataEntry &;
  auto ReserveAllocNewPageLogEntry(pageid_t new_page_id) -> AllocNewPageEntry &;
  auto ReserveFreeExtentLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreeExtentEntry &;
  auto SubmitActiveLogEntry() -> bool;
};

}  // namespace leanstore::recovery
