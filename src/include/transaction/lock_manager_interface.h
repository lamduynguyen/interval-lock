#pragma once

#include "common/typedefs.h"
#include "leanstore/config.h"
#include "transaction/lockable_tuple.h"

#include <atomic>
#include <functional>
#include <memory>
#include <span>
#include <unordered_map>
#include <unordered_set>

namespace leanstore::transaction {

using WriteSetCallback = std::function<void(const LockableTuple *, std::span<const u8>)>;

/// One end of a key range, used by PrecisionLockManager
struct RangeBound {
  std::span<const u8> key;
  bool inclusive{true};
};

using LocalReadSet = std::unordered_set<const LockableTuple *, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
using LocalWriteSet =
  std::unordered_map<const LockableTuple *, std::vector<u8>, LockableTuple::HashPtr, LockableTuple::EqualPtr>;

enum class LockManagerVariant : u8 {
  BASELINE_FASTER = 0,  // FASTER-style hash buckets + circular buffer of lock entries
  BASELINE_STAGED = 1,  // Jung et al., "A Scalable Lock Manager for Multicores" (TODS 2014)
  BASELINE_INLINE = 2,  // one lock byte stored in the tuple itself, no-wait only
  PRECISION_RANGE = 3,  // range-granularity wait-die, locks declared by the workload
};

enum class LockMode : u8 { SHARED = 0, EXCLUSIVE = 1 };

enum class LockPolicy : u8 { NO_WAIT = 0, WAIT_DIE = 1 };

inline auto Conflicts(LockMode a, LockMode b) -> bool { return a == LockMode::EXCLUSIVE || b == LockMode::EXCLUSIVE; }

class LockManagerInterface {
 public:
  virtual ~LockManagerInterface() = default;

  virtual bool EmptyLocalSet()                                                           = 0;
  virtual void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) = 0;

  // Lock APIs, taken on tuples.
  /// The optimistic-stage exclusive claim, taken while no page latch is held -- which is what
  /// lets a manager block here. The default is the "nothing to do at this stage" case: managers
  /// whose conflict decision happens later, under the exclusive latch, keep it.
  virtual bool TryLockExclusive(timestamp_t, const LockableTuple *) { return true; }

  /// Take a shared claim. Managers keeping the state inside the record itself use the page and its slot;
  /// the rest ignore both, so the default is a no-op yes.
  virtual auto TryLockShared(timestamp_t, const LockableTuple *, pageid_t, leng_t) -> bool { return true; }

  /// Per-key releases; the default keeps the claim until commit/abort all at once (no per-key
  /// action to take). Managers that release live locks here override.
  virtual void Unlock(timestamp_t, const LockableTuple *) {}

  virtual void UnlockShared(timestamp_t, const LockableTuple *) {}

  /// Register the undo image once the page latch is held.
  /// An empty image means "no previous version", so an abort deletes the key.
  virtual void SetUndoPayload(timestamp_t txn_ts, const LockableTuple *, std::span<const u8>) = 0;
};

}  // namespace leanstore::transaction
