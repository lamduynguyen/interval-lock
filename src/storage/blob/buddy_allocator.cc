#include "storage/blob/buddy_allocator.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

#include <algorithm>
#include <atomic>

namespace leanstore::storage::blob {

BuddyAllocator::BuddyAllocator(u64 extent_region_start, u64 extent_region_pages)
    : region_start_(extent_region_start),
      region_pages_(extent_region_pages),
      region_end_(extent_region_start + extent_region_pages) {
  // Initialize bitmaps to 0
  for (u8 t = 0; t < BUDDY_TIERS; t++) {
    for (u64 w = 0; w < MAX_BITMAP_WORDS; w++) { bitmaps_[t][w].store(0, std::memory_order_relaxed); }
  }

  // Distribute the initial free blocks across worker shards round-robin so that
  // no single worker starts with the entire region in its own lists
  auto shard_rr = 0U;
  u64 current   = region_start_;
  while (current < region_end_) {
    u64 remaining = region_end_ - current;
    u8 best_tier  = 0;
    for (u8 t = 0; t < BUDDY_TIERS; t++) {
      u64 tier_size = BuddyTierSize(t);
      if (tier_size <= remaining && IsAligned(current, tier_size, region_start_)) {
        best_tier = t;
      } else if (tier_size > remaining) {
        break;
      }
    }
    u64 block_size = BuddyTierSize(best_tier);
    shards_[shard_rr].free_blocks[best_tier].push_back(current);
    shard_rr = (shard_rr + 1) % NO_SHARDS;
    SetFree<true>(current, best_tier);
    current += block_size;
  }
}

template <bool IS_FREE>
void BuddyAllocator::SetFree(pageid_t start_pid, u8 tier) {
  u64 block_size = BuddyTierSize(tier);
  u64 block_idx  = (start_pid - region_start_) / block_size;
  u64 word_idx   = block_idx / 64;
  u64 bit_idx    = block_idx % 64;
  u64 mask       = 1UL << bit_idx;

  if (word_idx < MAX_BITMAP_WORDS) {
    if constexpr (IS_FREE) {
      bitmaps_[tier][word_idx].fetch_or(mask, std::memory_order_release);
    } else {
      bitmaps_[tier][word_idx].fetch_and(~mask, std::memory_order_release);
    }
  }
}

template void BuddyAllocator::SetFree<true>(pageid_t, u8);
template void BuddyAllocator::SetFree<false>(pageid_t, u8);

auto BuddyAllocator::IsBlockFree(pageid_t start_pid, u8 tier) const -> bool {
  u64 block_size = BuddyTierSize(tier);
  u64 block_idx  = (start_pid - region_start_) / block_size;
  u64 word_idx   = block_idx / 64;
  u64 bit_idx    = block_idx % 64;

  if (word_idx >= MAX_BITMAP_WORDS) return false;
  return (bitmaps_[tier][word_idx].load(std::memory_order_acquire) & (1UL << bit_idx)) != 0;
}

auto BuddyAllocator::MyShard() -> FreeListShard & {
  auto wid = LeanStore::worker_thread_id;
  /* Workers index their own shard; auxiliary/test threads without a valid
   * worker id fold into the shard range deterministically. */
  return shards_[wid < NO_SHARDS ? wid : (NO_SHARDS - 1 + (wid % NO_SHARDS)) % NO_SHARDS];
}

auto BuddyAllocator::PopFromShard(FreeListShard &shard, u8 tier) -> pageid_t {
  sync::HybridGuard guard(&shard.latch, sync::GuardMode::EXCLUSIVE);
  auto &free_list = shard.free_blocks[tier];
  if (free_list.empty()) { return 0; }
  auto pid = free_list.back();
  free_list.pop_back();
  return pid;
}

auto BuddyAllocator::RemoveFromShard(FreeListShard &shard, u8 tier, pageid_t pid) -> bool {
  sync::HybridGuard guard(&shard.latch, sync::GuardMode::EXCLUSIVE);
  auto &free_list = shard.free_blocks[tier];
  auto it         = std::find(free_list.begin(), free_list.end(), pid);
  if (it == free_list.end()) { return false; }
  free_list.erase(it);
  return true;
}

void BuddyAllocator::AddToShard(FreeListShard &shard, u8 tier, pageid_t pid) {
  sync::HybridGuard guard(&shard.latch, sync::GuardMode::EXCLUSIVE);
  shard.free_blocks[tier].push_back(pid);
}

auto BuddyAllocator::AllocExtent(extidx_t tier) -> pageid_t {
  u8 buddy_tier = ExtentTierToBuddyTier(tier);

  /* Fast path: consume our own shard, splitting larger blocks as needed. */
  for (u8 t = buddy_tier; t < BUDDY_TIERS; t++) {
    auto pid = PopFromShard(MyShard(), t);
    if (pid != 0) {
      SetFree<false>(pid, t);
      if (t > buddy_tier) { SplitBlock(pid, t, buddy_tier); }
      return pid;
    }
  }

  /* Slow path: steal from other workers' shards. */
  for (u8 t = buddy_tier; t < BUDDY_TIERS; t++) {
    for (auto s = 0U; s < NO_SHARDS; s++) {
      auto pid = PopFromShard(shards_[s], t);
      if (pid != 0) {
        SetFree<false>(pid, t);
        if (t > buddy_tier) { SplitBlock(pid, t, buddy_tier); }
        return pid;
      }
    }
  }
  return 0;
}

void BuddyAllocator::SplitBlock(pageid_t start_pid, u8 source_tier, u8 target_tier) {
  for (u8 t = source_tier; t > target_tier; t--) {
    u64 half           = BuddyTierSize(t) / 2;
    pageid_t buddy_pid = start_pid + half;
    AddToShard(MyShard(), t - 1, buddy_pid);
    SetFree<true>(buddy_pid, t - 1);
  }
}

void BuddyAllocator::FreeExtent(pageid_t start_pid, extidx_t tier) {
  u8 buddy_tier = ExtentTierToBuddyTier(tier);
  SetFree<true>(start_pid, buddy_tier);
  TryCoalesce(start_pid, buddy_tier);
}

/**
 * @brief Insert a newly-freed block into the freeing worker's lists, merging
 * upward with its buddy while possible.
 *
 * Invariant: a free block lives in exactly ONE shard's tier list, and its bitmap
 * bit is set iff it is in that list. Each iteration: if the buddy of [pid, tier)
 * is free, remove it from whichever shard holds it and promote the lower half
 * to tier+1. Otherwise the block is parked in the current worker's shard/tier.
 */
void BuddyAllocator::TryCoalesce(pageid_t start_pid, u8 tier) {
  auto pid  = start_pid;
  auto curr = tier;
  while (curr < BUDDY_TIERS - 1) {
    auto buddy_pid = BuddyOf(pid, BuddyTierSize(curr), region_start_);
    auto removed   = false;
    if (buddy_pid < region_end_ && IsBlockFree(buddy_pid, curr)) {
      /* The buddy may sit in any worker's shard; scan them all. */
      for (auto s = 0U; s < NO_SHARDS && !removed; s++) { removed = RemoveFromShard(shards_[s], curr, buddy_pid); }
    }
    if (!removed) { break; }

    /* Both halves leave the current tier; the lower half is promoted. */
    SetFree<false>(pid, curr);
    SetFree<false>(buddy_pid, curr);
    pid = std::min(pid, buddy_pid);
    SetFree<true>(pid, curr + 1);
    curr++;
  }
  AddToShard(MyShard(), curr, pid);
}

auto BuddyAllocator::IsFree(pageid_t start_pid, u64 page_count) const -> bool {
  if (start_pid < region_start_ || start_pid + page_count > region_end_) return false;

  u8 tier = 0;
  while (tier < BUDDY_TIERS && BuddyTierSize(tier) < page_count) tier++;
  if (tier >= BUDDY_TIERS || BuddyTierSize(tier) != page_count) return false;
  if (!IsAligned(start_pid, page_count, region_start_)) return false;
  return IsBlockFree(start_pid, tier);
}

void BuddyAllocator::RecoverExtent(bool is_free, pageid_t start_pid, u64 page_count) {
  if (start_pid < region_start_ || start_pid + page_count > region_end_) return;

  u8 tier = 0;
  while (tier < BUDDY_TIERS && BuddyTierSize(tier) < page_count) tier++;
  if (tier >= BUDDY_TIERS || BuddyTierSize(tier) != page_count) return;
  if (!IsAligned(start_pid, page_count, region_start_)) return;

  if (is_free) {
    SetFree<true>(start_pid, tier);
    TryCoalesce(start_pid, tier);
  } else {
    SetFree<false>(start_pid, tier);
    /* The block may be parked in any worker's shard; scan them all. */
    for (auto s = 0U; s < NO_SHARDS; s++) {
      if (RemoveFromShard(shards_[s], tier, start_pid)) { break; }
    }
  }
}

}  // namespace leanstore::storage::blob