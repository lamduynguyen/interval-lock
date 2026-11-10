#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "storage/extent/extent_list.h"
#include "sync/hybrid_guard.h"

#include <array>
#include <atomic>
#include <vector>

namespace leanstore::storage::blob {

/**
 * @brief Buddy allocator for BLOB extents, with worker-local free lists.
 *
 * Each worker owns one FreeListShard (indexed by LeanStore::worker_thread_id):
 * allocation consumes the worker's own lists (uncontended latch) and only
 * steals from other workers (under their latch) when its own lists run dry.
 * Freed blocks return to the freeing worker's lists; the per-tier bitmaps are
 * the global allocation truth, so a shard latch only protects its vectors.
 */
class BuddyAllocator {
 public:
  // Buddy tiers match extent tiers: 0-17 for powers of 2 (1..131072 pages)
  // Tiers 18-31 map to tier 17 (capped at MAX_EXTENT_SIZE)
  static constexpr u8 BUDDY_TIERS     = 18;
  static constexpr u64 MAX_BUDDY_SIZE = ExtentList::MAX_EXTENT_SIZE;

  // Maximum region pages (8GB / 4KB = 2M pages)
  static constexpr u64 MAX_REGION_PAGES = (8UL << 30) / PAGE_SIZE;
  // Maximum bitmap words per tier (for tier 0: 2M pages / 64 = 32K words)
  static constexpr u64 MAX_BITMAP_WORDS = MAX_REGION_PAGES / 64 + 1;

  explicit BuddyAllocator(u64 extent_region_start, u64 extent_region_pages);
  ~BuddyAllocator() = default;

  auto AllocExtent(extidx_t tier) -> pageid_t;
  void FreeExtent(pageid_t start_pid, extidx_t tier);
  auto IsFree(pageid_t start_pid, u64 page_count) const -> bool;
  void RecoverExtent(bool is_free, pageid_t start_pid, u64 page_count);

 private:
  static constexpr auto NO_SHARDS = MAX_NUMBER_OF_WORKER;

  struct FreeListShard {
    sync::HybridLatch latch;
    std::vector<pageid_t> free_blocks[BUDDY_TIERS];
  };

  static constexpr u8 ExtentTierToBuddyTier(extidx_t tier) {
    return (tier < BUDDY_TIERS) ? static_cast<u8>(tier) : BUDDY_TIERS - 1;
  }

  static constexpr u64 BuddyTierSize(u8 tier) { return 1UL << tier; }

  static constexpr pageid_t BuddyOf(pageid_t pid, u64 block_size, u64 region_start) {
    return region_start + ((pid - region_start) ^ block_size);
  }

  static constexpr bool IsAligned(pageid_t pid, u64 block_size, u64 region_start) {
    return ((pid - region_start) & (block_size - 1)) == 0;
  }

  /* The calling thread's shard; out-of-range worker ids (auxiliary/test threads)
   * are folded into the shard range deterministically. Defined in the .cc since
   * it reads LeanStore::worker_thread_id. */
  auto MyShard() -> FreeListShard &;

  void SplitBlock(pageid_t start_pid, u8 source_tier, u8 target_tier);
  void TryCoalesce(pageid_t start_pid, u8 tier);

  /* Pop one pid from a shard's tier list; returns 0 if empty. Latches internally. */
  auto PopFromShard(FreeListShard &shard, u8 tier) -> pageid_t;
  /* Remove a specific pid from a shard's tier list; returns false if absent. Latches internally. */
  auto RemoveFromShard(FreeListShard &shard, u8 tier, pageid_t pid) -> bool;
  /* Push a pid onto a shard's tier list. Latches internally. */
  void AddToShard(FreeListShard &shard, u8 tier, pageid_t pid);

  FreeListShard shards_[NO_SHARDS];

  u64 region_start_;
  u64 region_pages_;
  u64 region_end_;

  // Fixed-size bitmaps
  std::atomic<u64> bitmaps_[BUDDY_TIERS][MAX_BITMAP_WORDS];

  template <bool IS_FREE>
  void SetFree(pageid_t start_pid, u8 tier);
  auto IsBlockFree(pageid_t start_pid, u8 tier) const -> bool;
};

}  // namespace leanstore::storage::blob