#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <array>
#include <atomic>
#include <memory>
#include <vector>

namespace leanstore::sync {

struct EpochEntry {
  void *ptr;
  void *ctx;  // opaque context handed back to the deleter (nullptr unless deferred with one)
  u64 epoch;
  // Returns true when the entry was consumed; false keeps it for a later sweep
  // (e.g. a lock manager entry that is still pinned/owned and cannot be erased yet)
  bool (*deleter)(void *, void *);

  EpochEntry(void *ptr, void *ctx, u64 epoch, bool (*d)(void *, void *));
};

struct EpochHandler {
  static constexpr u64 MAX_VALUE = ~0ULL;

  explicit EpochHandler(u64 no_threads);
  ~EpochHandler();

  // Advance the epoch that new guards record, so older deferred entries become reclaimable.
  void AdvanceEpoch();
  // Free wid's entries older than min(local_epoch). Called by wid's owner.
  void Reclaim(wid_t wid);
  void DeferFreePointer(wid_t wid, void *ptr, bool (*d)(void *, void *), void *ctx = nullptr);

  /* Epoch-based ptr reclaimation */
  const u64 no_threads;
  std::atomic<u64> global_epoch;
  std::vector<std::atomic<u64>> local_epoch;
  std::array<std::vector<EpochEntry>, MAX_NUMBER_OF_WORKER> to_free_ptr = {};
};

class EpochGuard {
 public:
  EpochGuard(std::atomic<u64> *epoch, const std::atomic<u64> &global_epoch) : epoch_(epoch) {
    epoch->store(global_epoch.load());
  }

  ~EpochGuard() { epoch_->store(EpochHandler::MAX_VALUE); }

 private:
  std::atomic<u64> *epoch_;
};

}  // namespace leanstore::sync