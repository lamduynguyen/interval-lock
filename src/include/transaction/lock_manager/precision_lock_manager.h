#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "storage/btree/node.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/spin_latch.h"
#include "transaction/lock_manager/utils/fingerprint_set.h"
#include "transaction/lock_manager/utils/lock_pool.h"
#include "transaction/lock_manager/utils/lock_tlb.h"
#include "transaction/lock_manager/utils/undo_arena.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include <atomic>
#include <functional>
#include <span>
#include <unordered_map>
#include <vector>

namespace leanstore::buffer {
class BufferManager;
struct BufferFrame;
}  // namespace leanstore::buffer

namespace leanstore::transaction {
/// Visits every leaf of `tree_id` that intersects [lo, hi] in ascending key order, and runs the per-leaf callback.
using TraverseFunc = std::function<void(leng_t tree_id, std::span<const u8> lo, std::span<const u8> hi,
                                        const std::function<void(pageid_t)> &visit)>;

/// One range a txn holds (or waits for), represent the data what the txn wants to access.
/// The entry object is managed by its txn; every page whose range overlaps with
/// the range entry keeps a pointer to it on that page's ref list.
struct RangeEntry {
  // Largest entry size (i.e., lo + hi keys) that the lock pool supports.
  static constexpr u16 MAX_ENTRY_SIZE = 32;
  static constexpr u16 KEY_LEN_MASK   = 0x7FFF;
  static constexpr u16 INCLUSIVE_BIT  = 0x8000;  /// Set when the end includes its key

  inline auto LoAfter() const -> bool { return (lo_len & INCLUSIVE_BIT) == 0; }

  inline auto HiAfter() const -> bool { return (hi_len & INCLUSIVE_BIT) != 0; }

  inline auto Lo() const -> std::span<const u8> { return {data, static_cast<u16>(lo_len & KEY_LEN_MASK)}; }

  inline auto Hi() const -> std::span<const u8> {
    return {data + (lo_len & KEY_LEN_MASK), static_cast<u16>(hi_len & KEY_LEN_MASK)};
  }

  static auto Cmp(std::span<const u8> a, std::span<const u8> b) -> int;
  static auto CmpBound(std::span<const u8> a, bool a_after, std::span<const u8> b, bool b_after) -> int;
  auto Covers(const RangeBound &other_lo, const RangeBound &other_hi) const -> bool;

  /// Do the two ranges share at least one key? Bounds are virtual points just before/after their
  /// keys, so adjacent ends at the same key ([.., k) and [k, ..)) do not overlap.
  auto Overlaps(const RangeBound &other_lo, const RangeBound &other_hi) const -> bool;

  class State {
   public:
    static constexpr u8 EXCLUSIVE = 1U << 0;
    static constexpr u8 GRANT     = 1U << 1;
    static constexpr u8 TOMBSTONE = 1U << 2;

    void Init(LockMode mode, bool granted) {
      bits_.store(static_cast<u8>((mode == LockMode::EXCLUSIVE ? EXCLUSIVE : 0) | (granted ? GRANT : 0)),
                  std::memory_order_relaxed);
    }

    auto Exclusive() const -> bool { return (bits_.load(std::memory_order_relaxed) & EXCLUSIVE) != 0; }

    auto Granted() const -> bool { return (bits_.load(std::memory_order_acquire) & GRANT) != 0; }

    auto Tombstone() const -> bool { return (bits_.load(std::memory_order_acquire) & TOMBSTONE) != 0; }

    void SetMode(LockMode mode) {
      const auto mask = static_cast<u8>(~EXCLUSIVE);
      if (mode == LockMode::EXCLUSIVE) {
        bits_.fetch_or(EXCLUSIVE, std::memory_order_relaxed);
      } else {
        bits_.fetch_and(mask, std::memory_order_relaxed);
      }
    }

    void SetGranted(bool granted) {
      const auto mask = static_cast<u8>(~GRANT);
      if (granted) {
        bits_.fetch_or(GRANT, std::memory_order_release);
      } else {
        bits_.fetch_and(mask, std::memory_order_release);
      }
    }

    void MarkTombstone() { bits_.fetch_or(TOMBSTONE, std::memory_order_release); }

   private:
    std::atomic<u8> bits_{0};
  };

  leng_t tree_id;                 // the caller's tree; only the INVALID_PAGE_ID fallback re-traverses it
  std::atomic<pageid_t> page_id;  // owning page, or INVALID_PAGE_ID once a split/merge moves the ref
  timestamp_t txn_ts;
  State state;
  u16 lo_len;  // lo key length | INCLUSIVE_BIT
  u16 hi_len;  // hi key length | INCLUSIVE_BIT
  u8 data[];   // VLA, covers [lo bytes][hi bytes]
};

/**
 * @brief Page-granularity 2PL wait-die lock manager,
 * with every lock entry represents a key range instead of a tuple.
 *
 * Locks live in per-page lists inside the page's BufferFrame, guarded by a separate latch, i.e.,
 * different from the page latch, so reading never increases the page version.
 */
class PrecisionLockManager : public LockManagerInterface {
  /// Spins on `granted` before re-walking the page to re-test the death condition.
  static constexpr u64 WAIT_SPINS = 128;

 public:
  explicit PrecisionLockManager(buffer::BufferManager *buffer, TraverseFunc traverse);
  ~PrecisionLockManager();

  bool EmptyLocalSet() override;
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;

  // Tuple API: undo bookkeeping only; shared claims are the page ranges above.
  bool TryLockExclusive(timestamp_t, const LockableTuple *key) override;
  void SetUndoPayload(timestamp_t txn_ts, const LockableTuple *key, std::span<const u8> payload) override;

  /// Range lock utilities
  auto LockRange(leng_t relation, pageid_t page_id, timestamp_t txn_ts, const RangeBound &lo, const RangeBound &hi,
                 LockMode mode, u64 expected_page_state = 0, bool scan = false) -> bool;
  void GrantScanRange(leng_t tree_id, timestamp_t txn_ts, const RangeBound &lo, const RangeBound &hi);
  void OnPageSplit(sync::ExclusiveGuard<storage::BTreeNode> &left, sync::ExclusiveGuard<storage::BTreeNode> &right,
                   std::span<const u8> split_key);
  void OnPageMerge(sync::ExclusiveGuard<storage::BTreeNode> &left, sync::ExclusiveGuard<storage::BTreeNode> &right);

  // Test fixtures observe the entry lifecycle through the per-worker ownership lists.
  friend class TestPrecisionLockManager;
  friend class TestPrecisionLockManagerNoWait;

 private:
  // Misc
  auto MakeEntry(leng_t tree_id, pageid_t page_id, timestamp_t txn_ts, const RangeBound &lo, const RangeBound &hi)
    -> RangeEntry *;
  void RecycleEntry(RangeEntry *entry);
  void UnlinkRef(buffer::BufferFrame &frame, RangeEntry *entry);
  void PromoteWaiters(buffer::BufferFrame &frame);

  struct PageTlb {
    u64 epoch{0};  // release epoch the slot was filled in, see tlb_epoch_
    RangeEntry *entry{nullptr};
  };

  /// Per-worker mutable state
  struct alignas(64) LocalState {
    ~LocalState() { delete[] tlb_; }

    auto TlbSlot(pageid_t page_id, u64 slot_cnt) -> PageTlb *;

    FingerprintSet<UndoSlot> write_set_;
    UndoArena undo_arena_;
    std::vector<RangeEntry *> entries_;
    std::unordered_multimap<fingerprint_t, RangeEntry *> range_index_;
    bool restoring_{false};  // Avoid locking during Transaction::ReleaseAllLocks()

    // Per-worker cache of the range entries.
    // This cache only maintains the last range per transaction, i.e., when a txn holds
    // multiple range locks over the same page, the last inserted one will be cached.
    // Its memory is allocated upon first use, is fixed size over full DB lifetime.
    //
    // The number of slots (~ mem usage) is determined by FLAGS_txn_lock_tlb_slots,
    // 0 means disabled, otherwise pow2-rounded(FLAGS_txn_lock_tlb_slots)
    PageTlb *tlb_{nullptr};
    u64 tlb_epoch_{1};  // bumped per release to logically invalidate slots from older epochs
  };

  void ReleaseStateEntries(LocalState &state);
  auto Local() -> LocalState &;

  buffer::BufferManager *buffer_;
  const TraverseFunc traverse_;
  const LockPolicy policy_;
  const u64 tlb_slot_cnt_;
  alignas(64) LocalState local_states_[MAX_NUMBER_OF_WORKER];
  LockPool<RangeEntry, RangeEntry::MAX_ENTRY_SIZE> pool_;
};

}  // namespace leanstore::transaction
