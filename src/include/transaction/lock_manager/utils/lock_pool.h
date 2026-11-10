#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <atomic>
#include <functional>
#include <vector>

namespace leanstore::transaction {

/**
 * @brief One AllocHuge'd segment of `T` nodes.
 * - per-worker free lists,
 * - claim segment slots one at a time,
 * - overflow to new/delete,
 * - per-worker byte budget FLAGS_txn_lock_pool_bytes (slot size decides the count).
 *
 * A slot reserves `TAIL` bytes behind the T object, so a T with a flexible-array tail
 * (e.g. RangeEntry::data[]) lands exactly on it. Overflow nodes are whole slots, so Free's
 * delete stays well-formed; callers must not hand foreign (e.g. exact-size malloc'd) nodes
 * to Free. T must be trivially destructible.
 */
template <class T, u16 TAIL = 0>
class LockPool {
  struct RawSlot {
    alignas(T) u8 bytes[sizeof(T) + TAIL];
  };

  static constexpr size_t SLOT_BYTES = sizeof(RawSlot);

 public:
  explicit LockPool(const std::function<void(T *)> &init = {});
  ~LockPool();

  LockPool(const LockPool &)            = delete;
  LockPool &operator=(const LockPool &) = delete;

  auto Alloc() -> T *;
  void Free(T *node) noexcept;
  auto Owns(const T *node) const -> bool;

 private:
  std::function<void(T *)> init_;

  T *segment_{nullptr};
  std::atomic<u32> segment_used_{0};
  std::atomic<bool> segment_full_{false};
  u32 segment_cap_{0};                                 // FLAGS_txn_lock_pool_bytes / SLOT_BYTES per worker
  std::vector<T *> free_lists_[MAX_NUMBER_OF_WORKER];  // slot wid is touched only by worker wid
};

}  // namespace leanstore::transaction
