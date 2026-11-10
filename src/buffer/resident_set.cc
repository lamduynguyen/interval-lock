#include "buffer/resident_set.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

#include <sys/mman.h>
#include <bit>
#include <cassert>
#include <cstring>
#include <mutex>
#include <stdexcept>

namespace leanstore::buffer {

ResidentPageSet::ResidentPageSet(u64 max_count)
    : capacity_(max_count), num_shards_(FLAGS_wal_checkpoint_shards), next_shard_(0) {
  assert(num_shards_ > 0);
  auto shard_capacity = (max_count + num_shards_ - 1) / num_shards_;
  shard_count_        = std::bit_ceil(static_cast<u64>(shard_capacity * 1.5));
  shard_mask_         = shard_count_ - 1;
  Ensure(shard_count_ >= 8);

  shards_ = static_cast<Shard *>(::operator new[](num_shards_ * sizeof(Shard)));
  for (auto i = 0U; i < num_shards_; i++) {
    auto entries = static_cast<Entry *>(AllocHuge(shard_count_ * sizeof(Entry)));
    memset(static_cast<void *>(entries), 0xFF, shard_count_ * sizeof(Entry));
    new (&shards_[i]) Shard(entries);
  }
}

ResidentPageSet::~ResidentPageSet() {
  for (u64 i = 0; i < num_shards_; ++i) {
    munmap(shards_[i].slots, shard_count_ * sizeof(Entry));
    shards_[i].~Shard();
  }
  ::operator delete[](shards_);
}

auto ResidentPageSet::Capacity() -> u64 { return capacity_; }

auto ResidentPageSet::Contain(pageid_t page_id) -> bool {
  auto &shard   = shards_[ShardOf(page_id)];
  u64 pos       = HashFn(page_id) & shard_mask_;
  u64 start_pos = pos;
  do {
    u64 curr = shard.slots[pos].pid.load();
    if (curr == EMPTY) { return false; }
    if (curr == page_id) { return true; }
    pos = (pos + 1) & shard_mask_;
  } while (pos != start_pos);
  return false;
}

void ResidentPageSet::Insert(pageid_t page_id) {
  auto &shard = shards_[ShardOf(page_id)];
  u64 pos     = HashFn(page_id) & shard_mask_;
  while (true) {
    u64 curr = shard.slots[pos].pid.load();
    if (curr == page_id) { UnreachableCode(); }
    if ((curr == EMPTY) || (curr == TOMBSTONE)) {
      if (shard.slots[pos].pid.compare_exchange_weak(curr, page_id)) { return; }
    }
    pos = (pos + 1) & shard_mask_;
  }
}

auto ResidentPageSet::Remove(pageid_t page_id) -> bool {
  auto &shard = shards_[ShardOf(page_id)];
  u64 pos     = HashFn(page_id) & shard_mask_;
  while (true) {
    u64 curr = shard.slots[pos].pid.load();
    if (curr == EMPTY) { return false; }
    if (curr == page_id) {
      if (shard.slots[pos].pid.compare_exchange_weak(curr, TOMBSTONE)) { return true; }
    }
    pos = (pos + 1) & shard_mask_;
  }
  return false;
}

void ResidentPageSet::IterateClockBatch(u64 batch, const std::function<void(pageid_t)> &evict_fn) {
  /* Round-robin across shards so eviction covers all pages. */
  u32 shard_idx = next_shard_.fetch_add(1, std::memory_order_relaxed) % num_shards_;
  auto &shard   = shards_[shard_idx];

  u64 pos;
  u64 new_position;
  do {
    pos          = shard.clock_pos.load();
    new_position = (pos + batch) % shard_count_;
  } while (!shard.clock_pos.compare_exchange_weak(pos, new_position));

  for (u64 idx = 0; idx < batch; idx++) {
    u64 curr = shard.slots[pos].pid.load();
    // Three conditions
    // - Page is not a TOMBSTONE, i.e., evicted
    // - The current slot is not empty, i.e., it stores a proper page
    // - The stored page must not be the metadata page (pid == 0), it must always reside in memory
    if ((curr != TOMBSTONE) && (curr != EMPTY) && (curr != METADATA_PAGE_ID)) { evict_fn(curr); }
    pos = (pos + 1) & shard_mask_;
  }
}

void ResidentPageSet::IterateShard(u32 shard, const std::function<void(pageid_t)> &fn) {
  assert(shard < num_shards_);
  auto &s = shards_[shard];
  for (u64 idx = 0; idx < shard_count_; idx++) {
    u64 curr = s.slots[idx].pid.load(std::memory_order_relaxed);
    if ((curr != TOMBSTONE) && (curr != EMPTY)) { fn(curr); }
  }
}

}  // namespace leanstore::buffer