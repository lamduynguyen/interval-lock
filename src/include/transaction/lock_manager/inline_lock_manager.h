#pragma once

#include "transaction/lock_manager/utils/fingerprint_set.h"
#include "transaction/lock_manager/utils/undo_arena.h"
#include "transaction/lock_manager_interface.h"

#include <span>
#include <vector>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::transaction {
/// One claim this transaction holds. The slot is only a hint: records move, so `key` is what
/// identifies the claim; the rest is only the fast path.
struct InlineHeldLock {
  fingerprint_t fingerprint;
  leng_t tree_id;
  pageid_t page_id;
  leng_t slot_id;
  std::vector<u8> key;
  bool exclusive;
};

// -------------------------------------------------------------------------------------
/**
 * @brief Lock stored in the record itself: one byte between the key suffix and the payload.
 * `0` unlocked, `1..254` a shared-holder count, `255` exclusive.
 * Only support No-Wait, so there is no waiter list.
 */
class InlineLockManager : public LockManagerInterface {
 public:
  explicit InlineLockManager(buffer::BufferManager *buffer);
  ~InlineLockManager() override;

  // Lock APIs
  auto EmptyLocalSet() -> bool override;
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;
  void SetUndoPayload(timestamp_t txn_ts, const LockableTuple *key, std::span<const u8> payload) override;

  // Record-granularity lock APIs
  auto TryLockShared(timestamp_t txn_ts, const LockableTuple *key, pageid_t page_id, leng_t slot_id) -> bool override;
  auto TryLockExclusive(timestamp_t txn_ts, const LockableTuple *key) -> bool override;
  auto TryLockExclusiveInPlace(timestamp_t txn_ts, const LockableTuple *key, pageid_t page_id, leng_t slot_id) -> bool;
  void RegisterInsertedRecord(timestamp_t txn_ts, const LockableTuple *key, leng_t tree_id, pageid_t page_id,
                              leng_t slot_id);

 private:
  // Word encoding: `0` free, shared claims count up to MAX_SHARED, EXCLUSIVE_HELD is X.
  static constexpr u8 UNLOCKED       = 0;
  static constexpr u8 MAX_SHARED     = 254;
  static constexpr u8 EXCLUSIVE_HELD = 255;

  using HeldLock = InlineHeldLock;
  using HeldSet  = FingerprintSet<InlineHeldLock>;

  /// Per-worker state
  struct LocalState {
    FingerprintSet<UndoSlot> write_set_;
    UndoArena undo_arena_;
    HeldSet held_;
  };

  // Lock bookkeeping
  auto FindHeld(const LockableTuple *key) -> HeldLock *;
  auto AddUndo(const LockableTuple *key) -> void;
  void ReleaseOne(HeldLock &held);  // word release, fast path first

  auto Local() -> LocalState &;

  std::vector<LocalState> local_states_;
  buffer::BufferManager *buffer_;
};

}  // namespace leanstore::transaction
