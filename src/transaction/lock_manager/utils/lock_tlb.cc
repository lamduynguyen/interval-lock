#include "transaction/lock_manager/utils/lock_tlb.h"

#include "leanstore/config.h"
#include "transaction/deadlock_prevention.h"

#include <bit>
#include <cstdlib>

namespace leanstore::transaction {

LockTlb::LockTlb() {
  const size_t slots = FLAGS_txn_lock_tlb_slots == 0 ? 0 : std::bit_ceil(FLAGS_txn_lock_tlb_slots);
  if (slots == 0) { return; }  // disabled: no array, every Lookup misses
  slots_ = static_cast<Entry *>(std::calloc(slots, sizeof(Entry)));
  mask_  = slots - 1;
}

LockTlb::~LockTlb() { std::free(slots_); }

auto LockTlb::Enabled() -> bool { return slots_ != nullptr; }

auto LockTlb::Lookup(fingerprint_t fp) -> DeadlockPrevention * {
  if (slots_ == nullptr) { return nullptr; }
  auto &e = slots_[fp & mask_];
  return e.fp == fp ? e.lock : nullptr;
}

auto LockTlb::Put(fingerprint_t fp, DeadlockPrevention *lock) -> Entry {
  if (slots_ == nullptr) { return {}; }
  auto &e = slots_[fp & mask_];
  Entry evicted{};
  if (e.lock != nullptr && e.fp != fp) {
    evicted = e;
    e.lock->Unpin();  // evicted: release its standing pin
  }
  e.fp   = fp;
  e.lock = lock;
  return evicted;
}

auto LockTlb::Flush() -> std::vector<Entry> {
  std::vector<Entry> flushed;
  if (slots_ == nullptr) { return flushed; }
  for (size_t idx = 0; idx <= mask_; ++idx) {
    auto &e = slots_[idx];
    if (e.lock != nullptr) {
      e.lock->Unpin();
      flushed.push_back(e);
    }
    e = Entry{};
  }
  return flushed;
}

}  // namespace leanstore::transaction
