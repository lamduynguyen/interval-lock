#include "sync/epoch_handler.h"

namespace leanstore::sync {

EpochEntry::EpochEntry(void *ptr, void *ctx, u64 epoch, bool (*d)(void *, void *))
    : ptr(ptr), ctx(ctx), epoch(epoch), deleter(d) {}

EpochHandler::EpochHandler(u64 no_threads) : no_threads(no_threads), global_epoch(0), local_epoch(no_threads) {
  for (auto &epoch : local_epoch) { epoch.store(MAX_VALUE, std::memory_order_relaxed); }
}

EpochHandler::~EpochHandler() {
  /* Free existing to_free pointers */
  for (auto &ptr_list : to_free_ptr) {
    for (auto &entry : ptr_list) { entry.deleter(entry.ptr, entry.ctx); }
  }
}

void EpochHandler::AdvanceEpoch() { global_epoch++; }

void EpochHandler::Reclaim(wid_t wid) {
  auto min_epoch = MAX_VALUE;
  for (const auto &epoch : local_epoch) { min_epoch = std::min(min_epoch, epoch.load()); }
  auto &list  = to_free_ptr[wid];
  size_t kept = 0;
  for (size_t idx = 0; idx < list.size(); idx++) {
    auto &entry = list[idx];
    // Too young, or the deleter could not consume it yet (still pinned/owned):
    // keep it for a later sweep instead of dropping it
    if (entry.epoch >= min_epoch || !entry.deleter(entry.ptr, entry.ctx)) {
      if (kept != idx) { list[kept] = entry; }
      kept++;
    }
  }
  list.erase(list.begin() + kept, list.end());
}

void EpochHandler::DeferFreePointer(wid_t wid, void *ptr, bool (*d)(void *, void *), void *ctx) {
  to_free_ptr[wid].emplace_back(ptr, ctx, local_epoch[wid].load(), d);
}

}  // namespace leanstore::sync