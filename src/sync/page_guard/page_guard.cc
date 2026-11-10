#include "sync/page_guard/page_guard.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"
#include "transaction/lock_manager/inline_lock_manager.h"
#include "transaction/lock_manager/precision_lock_manager.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include <stdexcept>
#include <type_traits>

namespace leanstore::sync {

template <class PageClass>
PageGuard<PageClass>::PageGuard() : PageGuard<PageClass>(nullptr, 0, GuardMode::MOVED) {}

template <class PageClass>
PageGuard<PageClass>::PageGuard(buffer::BufferManager *bm, pageid_t pid, GuardMode mode)
    : buffer_(bm), page_id_(pid), mode_(mode) {}

template <class PageClass>
auto PageGuard<PageClass>::operator->() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return reinterpret_cast<PageClass *>(buffer_->ToPtr(page_id_));
}

template <class PageClass>
auto PageGuard<PageClass>::Ptr() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return reinterpret_cast<PageClass *>(buffer_->ToPtr(page_id_));
}

template <class PageClass>
auto PageGuard<PageClass>::Mode() -> GuardMode {
  return mode_;
}

template <class PageClass>
auto PageGuard<PageClass>::PageID() -> pageid_t {
  return page_id_;
}

template <class PageClass>
auto PageGuard<PageClass>::GSN() -> timestamp_t {
  return Ptr()->p_gsn;
}

template <class PageClass>
auto PageGuard<PageClass>::TryLockShared(leng_t tree_id, std::span<u8> key, leng_t slot_id) -> bool {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (txn.iso_level < transaction::IsolationLevel::SERIALIZABLE) { return true; }

  // Lock point == Range of one key
  if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
    return static_cast<transaction::PrecisionLockManager *>(txn.LockManager())
      ->LockRange(tree_id, page_id_, txn.start_ts, {key}, {key}, transaction::LockMode::SHARED);
  }
  LOCKABLE_TUPLE_STACK(lockable, key, tree_id);
  return txn.LockManager()->TryLockShared(txn.start_ts, lockable, page_id_, slot_id);
}

template <class PageClass>
auto PageGuard<PageClass>::TryLockExclusive(leng_t tree_id, std::span<u8> key) -> bool {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (txn.iso_level < transaction::IsolationLevel::SERIALIZABLE) { return true; }

  LOCKABLE_TUPLE_STACK(lockable, key, tree_id);
  if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
    if (!static_cast<transaction::PrecisionLockManager *>(txn.LockManager())
           ->LockRange(tree_id, page_id_, txn.start_ts, {key}, {key}, transaction::LockMode::EXCLUSIVE)) {
      return false;
    }
  }

  // The inline manager defers its word flip to the latched stage.
  return txn.LockManager()->TryLockExclusive(txn.start_ts, lockable);
}

template <class PageClass>
auto PageGuard<PageClass>::TryLockExclusiveInPlace(leng_t tree_id, std::span<u8> key, leng_t slot_id) -> bool {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  // Only inline lock manager uses this API; the other variants
  // acquire locks already in the optimistic stage.
  if (txn.iso_level < transaction::IsolationLevel::SERIALIZABLE ||
      FLAGS_txn_locking_variant != static_cast<u32>(transaction::LockManagerVariant::BASELINE_INLINE)) {
    return true;
  }
  LOCKABLE_TUPLE_STACK(lockable, key, tree_id);
  return static_cast<transaction::InlineLockManager *>(txn.LockManager())
    ->TryLockExclusiveInPlace(txn.start_ts, lockable, page_id_, slot_id);
}

template <class PageClass>
void PageGuard<PageClass>::RegisterInsertedRecord(leng_t tree_id, std::span<u8> key, leng_t slot_id) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (txn.iso_level >= transaction::IsolationLevel::SERIALIZABLE &&
      FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::BASELINE_INLINE)) {
    LOCKABLE_TUPLE_STACK(lockable, key, tree_id);
    static_cast<transaction::InlineLockManager *>(txn.LockManager())
      ->RegisterInsertedRecord(txn.start_ts, lockable, tree_id, page_id_, slot_id);
  }
}

template <class PageClass>
void PageGuard<PageClass>::SetUndoPayload(leng_t tree_id, std::span<const u8> payload, std::span<u8> key) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (txn.iso_level >= transaction::IsolationLevel::SERIALIZABLE) {
    LOCKABLE_TUPLE_STACK(lockable, key, tree_id);
    txn.LockManager()->SetUndoPayload(txn.start_ts, lockable, payload);
  }
}

template <class PageClass>
void PageGuard<PageClass>::DetectGSNDependency() {
  if (FLAGS_wal_enable) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning());
    auto &logger = txn.LogWorker();
    auto buffer  = txn.BufferPool();
    if (GSN() > logger.rfa_gsn_flushed &&
        buffer->BufferFramePage(page_id_).last_writer != LeanStore::worker_thread_id) {
      prev_owner_ = buffer->BufferFramePage(page_id_).last_writer;
      prev_gsn_   = GSN();
    }
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), GSN()));
  }
}

template <class PageClass>
void PageGuard<PageClass>::AdvanceGSN() {
  if (FLAGS_wal_enable) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning());
    // This transaction should already hold X-lock on this page
    auto &logger = txn.LogWorker();
    auto buffer  = txn.BufferPool();
    //  therefore, it's GSN should not be higher than the local copy of GSN
    Ensure(GSN() <= logger.GetCurrentGSN());
    Ptr()->p_gsn                                  = logger.GetCurrentGSN() + 1;
    buffer->BufferFramePage(page_id_).last_writer = LeanStore::worker_thread_id;
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), GSN()));
  }
}

template <class PageClass>
void PageGuard<PageClass>::UpdateDependencyVector() {
  /* prev_gsn_ > 0 means somebody modifies the page */
  if (FLAGS_wal_enable && prev_gsn_ > 0) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning() && prev_owner_ != LeanStore::worker_thread_id &&
           prev_owner_ != std::numeric_limits<wid_t>::max());
    txn.gsn_vector[prev_owner_] = prev_gsn_;
  }
}

template class PageGuard<storage::Page>;
template class PageGuard<storage::MetadataPage>;
template class PageGuard<storage::BTreeNode>;

}  // namespace leanstore::sync
