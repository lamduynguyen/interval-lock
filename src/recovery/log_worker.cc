#include "recovery/log_worker.h"
#include "common/exceptions.h"
#include "common/queue.h"
#include "common/rand.h"
#include "common/utils.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"
#include "transaction/transaction.h"

#include "share_headers/time.h"

#include <algorithm>
#include <cstring>
#include <thread>

using TM = leanstore::transaction::Transaction;

namespace leanstore::recovery {

/**
 * @brief Reset LogWorker's internal state except the WAL Buffer
 */
LogWorker::LogWorker(std::atomic<bool> &db_is_running, LogManager *log_manager)
    : log_manager(log_manager),
      is_running(&db_is_running),
      log_buffer(FLAGS_wal_buffer_size_mb * MB, is_running),
      backend(log_manager, this),
      precommitted_queue(is_running) {}

LogWorker::~LogWorker() = default;

auto LogWorker::GetCurrentGSN() -> timestamp_t { return w_gsn_clock; }

void LogWorker::SetCurrentGSN(timestamp_t gsn) { w_gsn_clock = gsn; }

// --------------------------------------------------------------------------------------------

/**
 * @brief This is similar to ReserveDataLog, except that it operates with a prepared buffer.
 *  Therefore, it does not waits fot free space in the local log buffer
 */
auto LogWorker::PrepareDataLogEntry(u8 *buffer, u64 payload_size, pageid_t pid) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  active_log = reinterpret_cast<LogEntry *>(buffer);
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<DataEntry *>(active_log);
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::DATA_ENTRY;
  log_entry->pid     = pid;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = TM::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return *log_entry;
}

/**
 * @brief Submit the active log entry, which was a prepared log buffer somewhere in LeanStore
 */
void LogWorker::SubmitPreparedLogEntry() {
  auto log_size = active_log->size;
  assert(log_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, log_size);
  std::memcpy(log_buffer.Current(), reinterpret_cast<u8 *>(active_log), log_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  SubmitActiveLogEntry();
}

// --------------------------------------------------------------------------------------------

auto LogWorker::ReserveLogMetaEntry() -> LogMetaEntry & {
  log_buffer.EnsureEnoughSpace(this, sizeof(LogMetaEntry));
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<LogMetaEntry *>(active_log);
  log_entry->size    = sizeof(LogMetaEntry);
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = TM::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return static_cast<LogMetaEntry &>(*active_log);
}

auto LogWorker::ReserveLogCommitEntry(u64 payload_size) -> TxnCommitEntry & {
  const u64 total_size = sizeof(TxnCommitEntry) + payload_size;
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<TxnCommitEntry *>(active_log);
  log_entry->type    = LogEntry::Type::TX_COMMIT;
  log_entry->size    = total_size;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = TM::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  auto vector_sz     = TM::active_txn.SerializeGSNVector(log_entry->payload);
  Ensure(vector_sz == payload_size);
  return static_cast<TxnCommitEntry &>(*active_log);
}

auto LogWorker::ReserveDataLog(u64 payload_size, pageid_t pid) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<DataEntry *>(active_log);
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::DATA_ENTRY;
  log_entry->pid     = pid;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = TM::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReserveAllocNewPageLogEntry(pageid_t new_page_id) -> AllocNewPageEntry & {
  const u64 total_size = sizeof(AllocNewPageEntry);
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log             = reinterpret_cast<LogEntry *>(log_buffer.Current());
  auto log_entry         = reinterpret_cast<AllocNewPageEntry *>(active_log);
  log_entry->size        = total_size;
  log_entry->type        = LogEntry::Type::ALLOC_NEW_PAGE;
  log_entry->pid         = METADATA_PAGE_ID;
  log_entry->new_page_id = new_page_id;
  return *log_entry;
}

auto LogWorker::ReserveFreeExtentLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreeExtentEntry & {
  const u64 total_size = sizeof(FreeExtentEntry);
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry       = reinterpret_cast<FreeExtentEntry *>(active_log);
  log_entry->size      = total_size;
  log_entry->type      = (to_free) ? LogEntry::Type::FREE_EXTENT : LogEntry::Type::REUSE_EXTENT;
  log_entry->start_pid = start_pid;
  log_entry->lp_size   = size;
  log_entry->rc.w_id   = LeanStore::worker_thread_id;
  log_entry->rc.txn    = TM::active_txn.TxnID();
  log_entry->rc.gsn    = GetCurrentGSN();
  return *log_entry;
}

/**
 * @brief Finish the current active log entry and submit it to the log buffer
 *
 * @return true   Only for WILO variants -- if the system has just written the log buffer to the storage
 * @return false  Otherwise
 */
auto LogWorker::SubmitActiveLogEntry() -> bool {
  if (FLAGS_wal_debug) { active_log->ComputeChksum(); }

  bool just_write_log = false;
  log_buffer.wal_cursor += active_log->size;

  if (active_log->type == LogEntry::Type::TX_COMMIT) {
    auto written_cursor = log_buffer.write_cursor.load();
    auto difference     = (log_buffer.wal_cursor > written_cursor)
                            ? log_buffer.wal_cursor - written_cursor
                            : log_buffer.wal_cursor + FLAGS_wal_buffer_size_mb * MB - written_cursor;
    auto to_write       = (difference >= log_manager->worker_write_batch_size_);

    if (to_write) {
      just_write_log = true;
      WorkerWritesLog();
    }
  }

  return just_write_log;
}

// --------------------------------------------------------------------------------------------
void LogWorker::WorkerWritesLog() {
  if (log_buffer.wal_cursor != log_buffer.write_cursor.load()) { log_buffer.LogFlush(this); }
}

}  // namespace leanstore::recovery