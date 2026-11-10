#pragma once

#include "storage/blob/buddy_allocator.h"
#include "storage/extent/extent_list.h"

#include "gtest/gtest_prod.h"

#include <memory>
#include <span>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::storage {

/**
 * @brief Free-space manager for BLOB extents, backed by a BuddyAllocator.
 *
 * Transactional semantics:
 * - AllocExtent: reserves + logs REUSE_EXTENT immediately (inside the active txn)
 * - PrepareFreeTier: only queues the extent to txn.ToFreeExtents() + logs FREE_EXTENT;
 *   the buddy allocator is NOT touched yet
 * - FreeExtents: called by GroupCommit after durability; actually returns extents
 *   to the buddy allocator
 */
class FreeStorageManager {
 public:
  FreeStorageManager(u64 extent_region_start, u64 extent_region_pages);
  ~FreeStorageManager() = default;

  auto AllocExtent(extidx_t tier) -> pageid_t;
  void PrepareFreeTier(pageid_t start_pid, extidx_t tier);
  void FreeExtents(std::span<const ExtentTier> extents);

  void RecoverExtent(bool is_free, pageid_t start_pid, u64 page_count);
  auto IsFree(pageid_t start_pid, u64 page_count) -> bool;

 private:
  friend class leanstore::buffer::BufferManager;

  void LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx);

  std::unique_ptr<blob::BuddyAllocator> buddy_allocator_;
};

}  // namespace leanstore::storage