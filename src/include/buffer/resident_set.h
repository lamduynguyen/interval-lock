#pragma once

#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/page_state.h"

#include <atomic>
#include <functional>
#include <shared_mutex>

namespace leanstore::buffer {

class ResidentPageSet {
 public:
  static constexpr u64 EMPTY     = ~0ULL;
  static constexpr u64 TOMBSTONE = (~0ULL) - 1;

  explicit ResidentPageSet(u64 max_count);
  ~ResidentPageSet();

  auto Capacity() -> u64;
  auto Contain(pageid_t page_id) -> bool;
  void Insert(pageid_t page_id);
  auto Remove(pageid_t page_id) -> bool;
  void IterateClockBatch(u64 batch, const std::function<void(pageid_t)> &evict_fn);
  void IterateShard(u32 shard, const std::function<void(pageid_t)> &fn);

 private:
  class Entry {
   public:
    std::atomic<pageid_t> pid;
  };

  struct Shard {
    Entry *slots;
    std::atomic<u64> clock_pos;

    Shard(Entry *slots) : slots(slots), clock_pos(0) {}
  };

  inline auto ShardOf(pageid_t pid) const -> u32 { return static_cast<u32>(pid % num_shards_); }

  const u64 capacity_;
  const u32 num_shards_;
  u64 shard_count_;
  u64 shard_mask_;
  std::atomic<u32> next_shard_{0};
  Shard *shards_;
};

}  // namespace leanstore::buffer