#include "storage/free_storage.h"
#include "leanstore/schema.h"
#include "leanstore/statistics.h"
#include "recovery/log_worker.h"
#include "transaction/transaction.h"

namespace leanstore::storage {

using TM = transaction::Transaction;

FreeStorageManager::FreeStorageManager(u64 extent_region_start, u64 extent_region_pages)
    : buddy_allocator_(std::make_unique<blob::BuddyAllocator>(extent_region_start, extent_region_pages)) {}

inline void FreeStorageManager::LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (!is_free_op) { statistics::storage::free_size -= ExtentList::TIER_SIZE[tier_idx]; }
  txn.LogWorker().ReserveFreeExtentLogEntry(is_free_op, start_pid, ExtentList::ExtentSize(tier_idx));
  /* Submit immediately: the entry is written at the current wal_cursor, and a later Reserve*
   * (e.g. the TX_COMMIT entry) would otherwise overwrite it without the cursor advancing. */
  txn.LogWorker().SubmitActiveLogEntry();
}

auto FreeStorageManager::AllocExtent(extidx_t tier) -> pageid_t {
  auto pid = buddy_allocator_->AllocExtent(tier);
  if (pid != 0) LogFreePage(false, pid, tier);
  return pid;
}

/* Queue-only: the extent is returned to the buddy allocator at commit time (FreeExtents). */
void FreeStorageManager::PrepareFreeTier(pageid_t start_pid, extidx_t tier) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  txn.ToFreeExtents().emplace_back(start_pid, tier);
  LogFreePage(true, start_pid, tier);
}

/* Publish frees after the txn is durable. Safe to call multiple times for the same
 * extent only if it was re-allocated in between; group commit calls this exactly once
 * per committed txn, so no double-free can happen here. */
void FreeStorageManager::FreeExtents(std::span<const ExtentTier> extents) {
  for (const auto &ext : extents) {
    buddy_allocator_->FreeExtent(ext.start_pid, ext.tier);
    statistics::storage::free_size += ExtentList::TIER_SIZE[ext.tier];
  }
}

void FreeStorageManager::RecoverExtent(bool is_free, pageid_t start_pid, u64 page_count) {
  buddy_allocator_->RecoverExtent(is_free, start_pid, page_count);
}

auto FreeStorageManager::IsFree(pageid_t start_pid, u64 page_count) -> bool {
  return buddy_allocator_->IsFree(start_pid, page_count);
}

}  // namespace leanstore::storage