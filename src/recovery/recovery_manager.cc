#include "recovery/recovery_manager.h"
#include "buffer/buffer_manager.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "sync/page_guard/exclusive_guard.h"

#include "fmt/format.h"

#include <algorithm>
#include <cstring>

using leanstore::storage::WALAfterImage;
using leanstore::storage::WALDeltaImage;
using leanstore::storage::WALEntry;
using leanstore::storage::WALFreshPage;
using leanstore::storage::WALInsert;
using leanstore::storage::WALInsertSep;
using leanstore::storage::WALLogicalSplit;
using leanstore::storage::WALNewPage;
using leanstore::storage::WALNewRoot;
using leanstore::storage::WALRemove;
using leanstore::storage::WalType;
using leanstore::sync::ExclusiveGuard;

namespace leanstore::recovery {

// -------------------------------------------------------------------------------------
/**
 * @brief Recovery was tested with READ UNCOMMITTED, and not yet support higher isolation level
 */
RecoveryManager::RecoveryManager(buffer::BufferManager *buffer_pool)
    : buffer_(buffer_pool), backend_(buffer_), max_logged_pid_(0), redo_pid_counter_(1) {
  for (auto wid = 0U; wid < MAX_NUMBER_OF_WORKER; wid++) { max_alloc_cnt_[wid] = 0; }
}

void RecoveryManager::RecoveryPreparation() {
  backend_.Connect();
  backend_.RetrieveMasterRecord();
}

auto RecoveryManager::SignatureOfPrevSession() -> u32 { return backend_.signature_; }

/**
 * @brief It's possible that there are a few empty log blocks before the last log blocks
 * This is because some workers fail to write something to their assigned block before the crash
 *
 * The upper-bound of the possible number of empty blocks is the number of workers (in previous session)
 * As a result, we will retry at least # workers empty log blocks to detect tail of the log
 *
 * After this phase, page_log_[] become read-only
 */
auto RecoveryManager::MaterializeLogs() -> u64 {
  auto last_valid_offset = backend_.ReadLogBlocks([&](u8 *buffer, u64 buffer_len, u32 signature, u64 rd_offset) {
    timestamp_t block_max_gsn = 0;
    for (auto ptr = buffer_len; ptr > 0;) {
      auto segment_sz = LogIOSegment::Deserialize(buffer, ptr, signature, [&](u8 *segment_buf, u64 buf_len) {
        AnalyzeLogSegment(LogIOSegment::Buffer(segment_buf, buf_len), block_max_gsn);
      });
      if (segment_sz == 0) { break; }
      ptr -= segment_sz;
    }
    if (block_max_gsn > 0) {
      u32 block_idx = static_cast<u32>((backend_.wal_offset_initial_ - rd_offset) / backend_.wal_block_size_) - 1;
      recovered_blocks_[LeanStore::worker_thread_id].emplace_back(block_idx, block_max_gsn);
    }
  });

  return last_valid_offset;
}

void RecoveryManager::ApplyRecoveredBlocks(WALBlockManager &mgr) {
  u64 total = 0;
  for (auto tid = 0U; tid < FLAGS_wal_recovery_threads; tid++) {
    for (auto &[idx, gsn] : recovered_blocks_[tid]) {
      mgr.RecoverBlock(idx, gsn);
      total++;
    }
    recovered_blocks_[tid].clear();
  }
  LOG_INFO("Applied {} recovered WAL blocks to ring registry", total);
}

void RecoveryManager::Analysis() {
  std::unordered_set<txnid_t> losers;

  /* Detect real losers from possible thread-local losers */
  for (auto &txn : tl_losers_[LeanStore::worker_thread_id]) {
    if (!winners_.contains(txn)) { losers.insert(txn); }
  }
  tl_losers_[LeanStore::worker_thread_id].clear();

  /* Detect losers from possible winners, i.e., if they access dirty data from other workers */
  for (auto &[txn, dep_gsn] : tl_winners_[LeanStore::worker_thread_id]) {
    /* Check whether it depends on a un-logged data */
    for (auto &[wid, ts] : dep_gsn) {
      if (ts > global_durable_gsn_[wid].load()) {
        losers.insert(txn);
        break;
      }
    }
  }

  // Bulk insertion to global list
  losers_.insert(std::begin(losers), std::end(losers));
}

/* Should only be triggered by one thread */
void RecoveryManager::PrepareRedoEnv() {
  has_recovered_ = std::make_unique<std::atomic<bool>[]>(max_logged_pid_.load() + 1);
  for (auto pid = 1U; pid <= max_logged_pid_.load(); pid++) {
    has_recovered_[pid] = false;
    buffer_->frame_[pid].Init();
  }
  {
    /* Always redo the metadata page first. Load it from disk as well: if the
     * previous session flushed it, its roots are already recovered and only the
     * NEW_ROOT records above its GSN need to be reapplied. */
    ExclusiveGuard<storage::MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
    buffer_->ReadPage(METADATA_PAGE_ID);  // Metadata page is already faulted during BM's constructor
    buffer_->frame_[METADATA_PAGE_ID].last_written_gsn = meta_page->p_gsn;
    /* Reconstruct alloc_cnt from the ALLOC_NEW_PAGE logs */
    for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
      UpdateMax(meta_page->alloc_cnt, max_alloc_cnt_[wid]);
    }
    PerPageRedo(meta_page, METADATA_PAGE_ID);
  }
  {
    /* Reconstruct the free-space state (loser set already computed by Analysis()). */
    RedoFreeExtents();
  }
}

/**
 * @brief Replay FREE_EXTENT/REUSE_EXTENT records to reconstruct the live-blob extent state.
 *
 * Replaying both record kinds over an empty RangeLock (skipping losers, whose frees were
 * never published and whose allocations never committed) reproduces the runtime state.
 */
void RecoveryManager::RedoFreeExtents() {
  /* Already in WAL order per worker; LazyGenerator is unusable (one GSN per txn trips its
   * dedup assertion). RecoverExtent is idempotent. */
  for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
    for (auto &buffer : free_extent_logs_[wid]) {
      buffer.Iterate([&](const LogEntry *log) {
        auto ext = reinterpret_cast<const FreeExtentEntry *>(log);
        if (losers_.contains(ext->rc.txn)) { return true; }
        buffer_->FreeStorageManager()->RecoverExtent(ext->type == LogEntry::Type::FREE_EXTENT, ext->start_pid,
                                                     ext->lp_size);
        return true;
      });
    }
    for (auto &buffer : free_extent_logs_[wid]) { buffer.Deallocate(); }
    free_extent_logs_[wid].clear();
  }
}

auto RecoveryManager::HasRecovered(pageid_t pid) -> bool {
  return (FLAGS_wal_enable_recovery) && ((pid > max_logged_pid_) || (has_recovered_[pid].load()));
}

template <class BTreeNode>
void RecoveryManager::PerPageRedo(ExclusiveGuard<BTreeNode> &page, pageid_t pid) {
  /* Lazy MapReduce */
  LogIOSegment::LazyGenerator log_generator;
  for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
    if (page_log_[wid].contains(pid)) {
      for (auto &buffer : page_log_[wid][pid]) { log_generator.ReceiveInput(&buffer); }
    }
  }
  log_generator.Iterate([&](const DataEntry *log) {
    assert(log->type == LogEntry::Type::DATA_ENTRY);
    /* The current log action belongs to a loser (aborted/crashed) transaction, so we skip it. */
    if (losers_.contains(log->rc.txn)) { return true; }
    /* Checkpoint-aware recovery: the page was loaded from its flushed disk image in PrepareRedoEnv,
     * so the records at its head (up to and including the  page's on-disk GSN) are already there. */
    if (log->rc.gsn <= page->p_gsn) { return true; }
    RedoLog(page, log);
    return true;
  });
  has_recovered_[pid] = true;
  /* Free those log buffers */
  for (auto wid = 0U; wid < FLAGS_wal_recovery_threads; wid++) {
    if (page_log_[wid].contains(pid)) {
      for (auto &buffer : page_log_[wid][pid]) { buffer.Deallocate(); }
    }
  }
}

void RecoveryManager::Redo() {
  if (max_logged_pid_ == 0) { return; }
  while (true) {
    auto pid = redo_pid_counter_.fetch_add(1, std::memory_order_relaxed);
    if (pid > max_logged_pid_) { break; }
    ExclusiveGuard<storage::BTreeNode> page(buffer_, pid);
    buffer_->HandlePageFault(pid);
    buffer_->frame_[pid].last_written_gsn = page->p_gsn;
    PerPageRedo(page, pid);
  }
}

void RecoveryManager::Undo() {
  /**
   * @brief Unnecessary because LeanStore doesn't support isolations higher than Read uncommitted
   *  => No transaction is aborted
   */
  // TODO: CLR-based undo for losers (incl. BLOB txns): also release the extents they
  //  allocated, i.e. treat each loser as if its FREE_EXTENT records had been logged.
  throw leanstore::ex::TODO("UNDO is not supported at the moment");
}

// -------------------------------------------------------------------------------------

/* Merge sorts and analyze all log segments */
void RecoveryManager::AnalyzeLogSegment(const LogIOSegment::Buffer &tmp, timestamp_t &block_max_gsn) {
  std::unordered_map<pageid_t, std::unordered_map<wid_t, LogIOSegment::Buffer>> tmp_analysis;
  tmp.Iterate([&](const LogEntry *log) {
    switch (log->type) {
      case LogEntry::Type::TX_START:
        tl_losers_[LeanStore::worker_thread_id].emplace(reinterpret_cast<const LogMetaEntry *>(log)->rc.txn);
        break;
      case LogEntry::Type::TX_COMMIT:
      case LogEntry::Type::TX_ABORT: {
        /* An aborted transaction should also be treated like a committed one: its CLRs (compensation log records)
         * are logged before the TX_ABORT entry and already revert its operations during redo. */
        auto mt       = reinterpret_cast<const TxnCommitEntry *>(log);
        block_max_gsn = std::max(block_max_gsn, mt->rc.gsn);
        winners_.insert(mt->rc.txn);
        tl_winners_[LeanStore::worker_thread_id].insert({mt->rc.txn, {}});
        /* Materialize the GSN Vector */
        auto wid = reinterpret_cast<const wid_t *>(mt->payload);
        auto ts  = reinterpret_cast<const timestamp_t *>(&(mt->payload)[sizeof(wid_t) * mt->vector_size]);
        UpdateMax(global_durable_gsn_[mt->rc.w_id], mt->rc.gsn); /* Evaluate the maximum global durable GSN */
        auto &durable_gsn = tl_winners_[LeanStore::worker_thread_id][mt->rc.txn];
        for (auto idx = 0U; idx < mt->vector_size; idx++) { durable_gsn[wid[idx]] = ts[idx]; }
      } break;
      case LogEntry::Type::DATA_ENTRY: {
        auto wal      = reinterpret_cast<const DataEntry *>(log);
        block_max_gsn = std::max(block_max_gsn, wal->rc.gsn);
        tmp_analysis[wal->pid][wal->rc.w_id].InsertNewLog(reinterpret_cast<const u8 *>(wal), wal->size);
      } break;
      case LogEntry::Type::ALLOC_NEW_PAGE: {
        auto alloc = reinterpret_cast<const AllocNewPageEntry *>(log);
        assert(alloc->pid == METADATA_PAGE_ID);
        max_alloc_cnt_[LeanStore::worker_thread_id] =
          std::max(max_alloc_cnt_[LeanStore::worker_thread_id], alloc->new_page_id + 1);
      } break;
      case LogEntry::Type::FREE_EXTENT:
      case LogEntry::Type::REUSE_EXTENT: {
        /* Collect for RedoFreeExtents() to replay in WAL order. */
        auto ext      = reinterpret_cast<const FreeExtentEntry *>(log);
        block_max_gsn = std::max(block_max_gsn, ext->rc.gsn);
        free_extent_logs_[LeanStore::worker_thread_id].emplace_back();
        free_extent_logs_[LeanStore::worker_thread_id].back().InsertNewLog(reinterpret_cast<const u8 *>(ext),
                                                                           ext->size);
      } break;
      case LogEntry::Type::MASTER_RECORD:
      case LogEntry::Type::WRITE_METADATA:
        /* Logging metadata should be handled/eliminated during log materialization */
        throw leanstore::ex::Unreachable("Shouldn't be here");
      case LogEntry::Type::CARRIAGE_RETURN:
        /* Doesn't have to do anything in this case */
        break;
      default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
    }
    return true;
  });
  for (auto &[pid, w_segment] : tmp_analysis) {
    UpdateMax(max_logged_pid_, pid);
    auto &log = page_log_[LeanStore::worker_thread_id];
    for (auto &[wid, segment] : w_segment) { log[pid].emplace_back(std::move(segment)); }
  }
}

template <>
void RecoveryManager::RedoLog(ExclusiveGuard<storage::MetadataPage> &page, const recovery::DataEntry *log) {
  assert(page->p_gsn < log->rc.gsn);
  auto entry = reinterpret_cast<const WALEntry *>(log);
  switch (entry->wal_type) {
    case WalType::NEW_ROOT: {
      auto init = reinterpret_cast<const WALNewRoot *>(entry);
      Ensure(init->pid == 0);
      auto meta_p                  = reinterpret_cast<storage::MetadataPage *>(page.Ptr());
      meta_p->roots[init->slot_id] = init->new_root_pid;
    } break;
    default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
  }
  // Increase page GSN
  page->p_gsn = log->rc.gsn;
}

template <>
void RecoveryManager::RedoLog(ExclusiveGuard<storage::BTreeNode> &page, const recovery::DataEntry *log) {
  assert(page->p_gsn == 0 || log->prev_gsn >= page->p_gsn);
  auto entry = reinterpret_cast<const WALEntry *>(log);
  switch (entry->wal_type) {
    case WalType::INSERT: {
      auto ins = reinterpret_cast<const WALInsert *>(entry);
      auto key = std::span<u8>(const_cast<u8 *>(ins->payload), ins->key_length);
      auto payload =
        std::span<const u8>(reinterpret_cast<const u8 *>(ins->payload + ins->key_length), ins->value_length);
      Ensure(page->HasSpaceForKV(key.size(), payload.size()));
      page->InsertKeyValue(key, payload, cmp_lambda_);
    } break;
    case WalType::INSERT_SEP: {
      auto sep = reinterpret_cast<const WALInsertSep *>(entry);
      page->InsertSeparator({const_cast<u8 *>(sep->sep_key), sep->sep_length}, sep->left_pid, sep->right_pid,
                            cmp_lambda_);
    } break;
    case WalType::LOGICAL_SPLIT: {
      auto split   = reinterpret_cast<const WALLogicalSplit *>(entry);
      auto sep_key = std::vector<u8>(split->sep.len);
      page->GetSeparatorKey(sep_key.data(), split->sep);
      page->SplitNode(nullptr, nullptr, page.PageID(), split->next_leaf_node, split->sep.slot,
                      {sep_key.data(), split->sep.len}, cmp_lambda_);
    } break;
    case WalType::FRESH_PAGE: {
      auto fresh = reinterpret_cast<const WALFreshPage *>(entry);
      new (page.Ptr()) storage::BTreeNode(!fresh->is_inner);
      if (fresh->is_inner) { page->header.right_most_child = fresh->right_most_child; }
    } break;
    case WalType::NEW_PAGE: {
      auto init = reinterpret_cast<const WALNewPage *>(entry);
      auto ptr  = reinterpret_cast<u8 *>(buffer_->ToPtr(init->pid));
      std::memcpy(ptr, init->payload, init->metadata_size);
      std::memcpy(ptr + PAGE_SIZE - init->payload_size, init->payload + init->metadata_size, init->payload_size);
    } break;
    case WalType::REMOVE: {
      auto rm = reinterpret_cast<const WALRemove *>(entry);
      page->RemoveSlot(rm->slot_id);
    } break;
    case WalType::AFTER_IMAGE: {
      auto ai = reinterpret_cast<const WALAfterImage *>(entry);
      bool found;
      auto pos = page->LowerBound({const_cast<u8 *>(ai->payload), ai->key_length}, found, cmp_lambda_);
      Ensure(found);
      auto record = page->GetPayload(pos);
      Ensure(record.size() == ai->value_length);
      std::memcpy(record.data(), ai->payload + ai->key_length, record.size());
    } break;
    case WalType::DELTA_IMAGE: {
      auto dt_log = reinterpret_cast<const WALDeltaImage *>(entry);
      auto delta  = reinterpret_cast<const FixedSizeDelta *>(dt_log->payload + dt_log->key_length);
      bool found;
      auto pos = page->LowerBound({const_cast<u8 *>(dt_log->payload), dt_log->key_length}, found, cmp_lambda_);
      Ensure(found);
      auto record = page->GetPayload(pos);
      delta->ApplyDelta(record);
    } break;
    default: throw leanstore::ex::Unreachable("All cases should be handled above"); break;
  }
  // Increase page GSN
  page->p_gsn = log->rc.gsn;
}

template void RecoveryManager::PerPageRedo<storage::BTreeNode>(ExclusiveGuard<storage::BTreeNode> &page, pageid_t pid);
template void RecoveryManager::PerPageRedo<storage::MetadataPage>(ExclusiveGuard<storage::MetadataPage> &page,
                                                                  pageid_t pid);

}  // namespace leanstore::recovery
