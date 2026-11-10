#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/epoch_handler.h"
#include "transaction/lock_manager/utils/fingerprint_set.h"
#include "transaction/lock_manager/utils/lock_pool.h"
#include "transaction/lock_manager/utils/lock_tlb.h"
#include "transaction/lock_manager/utils/undo_arena.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include <gtest/gtest_prod.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <span>
#include <unordered_set>
#include <vector>

namespace leanstore::transaction {

enum class State : u8 { WAIT = 0, GRANTED = 1, OBSOLETE = 2 };

class StagedLockManager;

/// One lock node: the txn's claim on one fingerprint, chained into a hash bucket.
struct LockNode {
  std::atomic<LockNode *> next{nullptr};
  std::atomic<State> state{State::WAIT};  // State lives in StagedLockManager
  fingerprint_t fp;                       // fingerprint of {tree_id, key bytes}; no key bytes stored
  timestamp_t txn_ts;                     // owner txn id
  u64 seq;                                // global insertion order (FIFO priority)
  LockMode mode;
  StagedLockManager *owner;
};

// -------------------------------------------------------------------------------------
/**
 * @brief Based on Jung et al., "A Scalable Lock Manager for Multicores" (TODS 2014).
 * Latch-free lock nodes in per-bucket hash chains.
 * Blocked txns poll and abort on a wait-for cycle.
 * Release is staged: nodes go OBSOLETE, unlinked and reclaimed later by the epoch handler.
 */
class StagedLockManager : public LockManagerInterface {
 public:
  // Key APIs
  StagedLockManager();
  ~StagedLockManager() override;

  /// Instance-owned per-worker state, constructed with the manager,
  /// indexed by LeanStore::worker_thread_id.
  struct LocalState {
    FingerprintSet<fingerprint_t> read_set_;
    FingerprintSet<UndoSlot> write_set_;
    UndoArena undo_arena_;
    std::vector<LockNode *> waiters_;          // BFS scratch: current waiters
    std::vector<timestamp_t> frontier_;        // BFS scratch: txns to expand
    std::unordered_set<timestamp_t> visited_;  // BFS scratch: expanded txns
    u64 local_releases{0};                     // per-worker release cadence
  };

  bool EmptyLocalSet() override;
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *key, pageid_t page_id, leng_t slot_id) override;
  bool TryLockExclusive(timestamp_t txn_ts, const LockableTuple *key) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *key) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *key) override;
  void SetUndoPayload(timestamp_t txn_ts, const LockableTuple *key, std::span<const u8> payload) override;

 private:
  FRIEND_TEST(StagedLockManagerTest, SharedCoexist);
  FRIEND_TEST(StagedLockManagerTest, BlockingAndWakeup);
  FRIEND_TEST(StagedLockManagerTest, DeadlockDetectionAbortsSelf);
  FRIEND_TEST(StagedLockManagerTest, UpgradePath);
  FRIEND_TEST(StagedLockManagerTest, UpgradeBehindPendingXIsDeadlock);
  FRIEND_TEST(StagedLockManagerTest, LockTableGC);
  FRIEND_TEST(StagedLockManagerTest, StressTestMultipleKeys);

  static constexpr u64 NUM_BUCKETS             = 1ULL << 16;
  static constexpr u64 RELEASES_PER_EPOCH      = 64;
  static constexpr u64 RELEASES_PER_GC         = 256;
  static constexpr u64 SLEEP_START_US          = 25;  // backoff ramp: 4x per poll up to MAX_SLEEP_US
  static constexpr u64 MAX_SLEEP_US            = 1000;
  static constexpr u64 DEADLOCK_CHECK_INTERVAL = 64;  // cycle check every N blocked polls

  static auto ReturnToPool(void *ptr, void *ctx) -> bool;

  // Chain manipulation
  auto InsertNode(timestamp_t txn_ts, LockMode mode, fingerprint_t fp) -> LockNode *;
  auto IsBlocked(LockNode *me) -> bool;
  auto FindOwnNode(timestamp_t txn_ts, fingerprint_t fp, LockMode mode) -> LockNode *;

  // Lock utilities
  auto AcquireNode(LockNode *node) -> bool;
  auto DeadlockExists(LockNode *node) -> bool;

  // Release & garbage collection
  void RetireNode(LockNode *node);
  auto GcBucket(u64 bucket) -> bool;
  void CollectGarbage();  // Unlink + return OBSOLETE nodes to the pool

  // Misc
  auto Local() -> LocalState &;

  static auto BucketOf(u64 fp) -> u64 { return fp & (NUM_BUCKETS - 1); }

  std::atomic<LockNode *> buckets_[NUM_BUCKETS];
  std::atomic<u64> hints_[NUM_BUCKETS / 32];  // bitmap for all buckets: WAIT & RETIRED mark
  std::atomic<u64> next_seq_{1};
  std::atomic<size_t> live_nodes_{0};
  std::atomic<u64> waiting_{0};  // nodes in WAIT; a cycle needs >= 2, so it gates the cycle check
  sync::EpochHandler epoch_{MAX_NUMBER_OF_WORKER};
  std::mutex gc_locks_[NUM_BUCKETS / 64];  // One stripe lock per 64 buckets.
  std::vector<LocalState> local_states_;

  /// Per-worker budget as faster/precision, pooled: drawn on demand until the total is out.
  LockPool<LockNode> pool_;
};

// -------------------------------------------------------------------------------------
}  // namespace leanstore::transaction
