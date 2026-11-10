#pragma once

#include "common/typedefs.h"
#include "transaction/lockable_tuple.h"

#include <vector>

namespace leanstore::transaction {

class DeadlockPrevention;

/**
 * @brief Per-worker direct-mapped TLB:
 * fingerprint -> DeadlockPrevention* in front of the table.
 *
 * A hit keeps the pointee valid without any bucket-latch traffic;
 * pins are owned by the TLB until eviction/Flush.
 * FLAGS_txn_lock_tlb_slots=0 disables the TLB entirely.
 */
struct LockTlb {
  struct Entry {
    fingerprint_t fp;
    DeadlockPrevention *lock;
  };

  Entry *slots_{nullptr};
  size_t mask_{0};

  LockTlb();
  ~LockTlb();
  auto Enabled() -> bool;
  auto Lookup(fingerprint_t fp) -> DeadlockPrevention *;
  auto Put(fingerprint_t fp, DeadlockPrevention *lock) -> Entry;
  auto Flush() -> std::vector<Entry>;
};

}  // namespace leanstore::transaction
