#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "sync/epoch_handler.h"
#include "sync/spin_latch.h"
#include "transaction/deadlock_prevention.h"
#include "transaction/lock_manager/utils/fingerprint_set.h"
#include "transaction/lock_manager/utils/lock_tlb.h"
#include "transaction/lock_manager/utils/undo_arena.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include <gtest/gtest_prod.h>

#include <atomic>
#include <span>
#include <vector>

namespace leanstore::transaction {
/// One lock claim this transaction holds, plus where its undo image starts in the arena.
struct FasterHeldLock {
  fingerprint_t fingerprint;
  DeadlockPrevention *lock;
  u32 undo_off;
  u32 undo_len;
  bool exclusive;
};

// -------------------------------------------------------------------------------------
/**
 * @brief BASELINE_FASTER variant: FAST-style hash buckets:
 * - per-worker held set,
 * - undo arena + abort replay,
 * - TLB with standing pins
 * - epoch-based memory reclamation of idle entries.
 *
 * Entries live in a per-worker circular buffer.
 * Any lock state overflow goes to new/delete and is freed separately.
 */
class FasterLockManager : public LockManagerInterface {
 public:
  FasterLockManager();
  ~FasterLockManager() override;

  // Lock APIs
  auto EmptyLocalSet() -> bool override;
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *key, [[maybe_unused]] pageid_t page_id,
                     [[maybe_unused]] leng_t slot_id) override;
  bool TryLockExclusive(timestamp_t txn_ts, const LockableTuple *key) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *key) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *key) override;
  void SetUndoPayload(timestamp_t txn_ts, const LockableTuple *key, std::span<const u8> payload) override;

 private:
  static constexpr u64 NUM_BUCKETS = 1ULL << 16;
  FRIEND_TEST(FasterLockManagerTest, BasicAcquireReleaseUpgrade);
  FRIEND_TEST(FasterLockManagerTest, SmallPoolAllocFreeLifecycle);
  FRIEND_TEST(FasterLockManagerTest, LockTableGC);

  // Types
  struct FasterEntry {
    fingerprint_t fp = 0;
    DeadlockPrevention lock;
    FasterEntry *next{nullptr};
    bool parked{false};  // parked on its ring's free list
  };

  struct EntryRing {
    FasterEntry *base{nullptr};
    u32 cap{0};
    u32 used{0};
    u32 cursor{0};  // circular-reuse position
    std::vector<FasterEntry *> free;
    sync::SpinLatch latch;
  };

  // Table and LockPool
  auto FindPinned(fingerprint_t fp) -> DeadlockPrevention *;  // find + Pin under the bucket latch
  bool ReclaimIfFree(fingerprint_t fp);
  bool UnlinkIfFree(FasterEntry *entry);
  /// Unlink iff idle and free the entry; false means still held, the epoch retries later.
  auto InsertPinned(fingerprint_t fp, FasterEntry *candidate) -> DeadlockPrevention *;
  auto AllocEntry() -> FasterEntry *;  // ring slot or overflow
  void FreeEntry(FasterEntry *entry) noexcept;

  // Held/undo/TLB lifecycle
  using HeldLock = FasterHeldLock;
  using HeldSet  = FingerprintSet<FasterHeldLock>;

  struct LocalState {
    HeldSet held_;
    UndoArena undo_arena_;
    LockTlb tlb_;
  };

  auto LookupLock(fingerprint_t fp, LocalState &state) -> DeadlockPrevention *;  // TLB first; misses pin via the table
  void PutTlb(fingerprint_t fp, DeadlockPrevention *lock, LocalState &state);    // TLB eviction defers idle locks
  void ReleaseHeld(const HeldLock &held, timestamp_t txn_ts, LocalState &state);
  void DeferIdle(fingerprint_t fp);  // epoch-deferred erasure of an idle entry
  void DrainDeferred();              // destructor-time sweep, while the rings are alive

  auto Local() -> LocalState &;

  alignas(64) sync::SpinLatch latches_[NUM_BUCKETS];
  FasterEntry *buckets_[NUM_BUCKETS]{};
  std::atomic<size_t> table_size_{0};
  EntryRing rings_[MAX_NUMBER_OF_WORKER];  // slot wid touched only by worker wid
  sync::EpochHandler epoch_{MAX_NUMBER_OF_WORKER};
  std::vector<LocalState> local_states_;
};

// -------------------------------------------------------------------------------------
}  // namespace leanstore::transaction
