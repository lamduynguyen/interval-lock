#include "transaction/lock_manager/utils/lock_pool.h"
#include "transaction/lock_manager/precision_lock_manager.h"
#include "transaction/lock_manager/staged_lock_manager.h"

#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

#include <sys/mman.h>
#include <new>

namespace leanstore::transaction {

template <class T, u16 TAIL>
LockPool<T, TAIL>::LockPool(const std::function<void(T *)> &init) : init_(init) {
  // Same per-worker byte budget for every lock manager;
  // a bigger slot simply yields fewer slots.
  segment_cap_ = static_cast<u32>(FLAGS_worker_count * (FLAGS_txn_lock_pool_bytes / SLOT_BYTES));
  if (segment_cap_ != 0) { segment_ = static_cast<T *>(AllocHuge(segment_cap_ * SLOT_BYTES)); }
}

template <class T, u16 TAIL>
LockPool<T, TAIL>::~LockPool() {
  if (segment_ != nullptr) { munmap(segment_, segment_cap_ * SLOT_BYTES); }
}

template <class T, u16 TAIL>
auto LockPool<T, TAIL>::Alloc() -> T * {
  const auto wid  = LeanStore::worker_thread_id;
  auto &free_list = free_lists_[wid < MAX_NUMBER_OF_WORKER ? wid : 0];
  if (free_list.empty()) {
    if (!segment_full_.load(std::memory_order_relaxed)) {
      const u32 slot = segment_used_.fetch_add(1, std::memory_order_relaxed);
      if (slot < segment_cap_) {
        auto *node = new (reinterpret_cast<RawSlot *>(segment_)[slot].bytes) T();
        if (init_) { init_(node); }
        return node;
      }
      segment_used_.fetch_sub(1, std::memory_order_relaxed);  // keep the counter at the cap
      segment_full_.store(true, std::memory_order_relaxed);   // no slot can ever free up again
    }
    auto *raw  = new RawSlot();  // overflow: whole slot, freed by Free
    auto *node = new (raw->bytes) T();
    if (init_) { init_(node); }
    return node;
  }
  auto *node = free_list.back();
  free_list.pop_back();
  return node;
}

template <class T, u16 TAIL>
void LockPool<T, TAIL>::Free(T *node) noexcept {
  const auto wid  = LeanStore::worker_thread_id;
  auto &free_list = free_lists_[wid < MAX_NUMBER_OF_WORKER ? wid : 0];
  if (Owns(node)) {
    free_list.push_back(node);
    return;
  }
  delete reinterpret_cast<RawSlot *>(node);  // T sits at the slot's offset 0
}

template <class T, u16 TAIL>
auto LockPool<T, TAIL>::Owns(const T *node) const -> bool {
  const auto *base = reinterpret_cast<const u8 *>(segment_);
  const auto *p    = reinterpret_cast<const u8 *>(node);
  return base != nullptr && p >= base && p < base + segment_cap_ * SLOT_BYTES;
}

template class LockPool<LockNode>;
template class LockPool<RangeEntry, RangeEntry::MAX_ENTRY_SIZE>;

}  // namespace leanstore::transaction
