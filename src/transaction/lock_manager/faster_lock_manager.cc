#include "transaction/lock_manager/faster_lock_manager.h"

#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

#include <sys/mman.h>
#include <cassert>
#include <stdexcept>

namespace leanstore::transaction {
// -------------------------------------------------------------------------------------
// Held/undo/TLB lifecycle
// -------------------------------------------------------------------------------------
auto FasterLockManager::Local() -> LocalState & { return local_states_[LeanStore::worker_thread_id]; }

bool FasterLockManager::EmptyLocalSet() { return Local().held_.Empty(); }

void FasterLockManager::SetUndoPayload(timestamp_t, const LockableTuple *key, std::span<const u8> payload) {
  auto &state = Local();
  auto *held  = state.held_.Find(LockableTuple::Fingerprint(key));
  if (held == nullptr || !held->exclusive) {
    throw std::runtime_error("SetUndoPayload called without holding an exclusive lock");
  }
  const auto [off, len] = state.undo_arena_.Append(key, payload);
  held->undo_off        = off;
  held->undo_len        = len;
}

bool FasterLockManager::TryLockShared(timestamp_t txn_ts, const LockableTuple *key, [[maybe_unused]] pageid_t page_id,
                                      [[maybe_unused]] leng_t slot_id) {
  auto &state  = Local();
  const u64 fp = LockableTuple::Fingerprint(key);
  if (state.held_.Find(fp) != nullptr) { return true; }  // already held (S or X)

  auto *lock         = LookupLock(fp, state);
  const bool granted = lock->TryLockShared(txn_ts);  // may wait
  if (granted) { state.held_.Add(HeldLock{fp, lock, NO_UNDO_OFF, 0, false}); }
  if (!granted && !state.tlb_.Enabled()) { lock->Unpin(); }  // lookup pin dies with the refusal
  return granted;
}

bool FasterLockManager::TryLockExclusive(timestamp_t txn_ts, const LockableTuple *key) {
  auto &state  = Local();
  const u64 fp = LockableTuple::Fingerprint(key);

  if (auto *held = state.held_.Find(fp); held != nullptr) {
    if (held->exclusive) { return true; }
    if (!held->lock->TryLockUpgrade(txn_ts)) { return false; }  // own S -> entry not reclaimable
    held->exclusive       = true;
    const auto [off, len] = state.undo_arena_.Append(key, {});
    held->undo_off        = off;
    held->undo_len        = len;
    return true;
  }

  // Same for a plain X: empty undo payload now, real payload via SetUndoPayload later.
  auto *lock            = LookupLock(fp, state);
  const auto [off, len] = state.undo_arena_.Append(key, {});
  const bool granted    = lock->TryLockExclusive(txn_ts);  // may wait
  if (granted) { state.held_.Add(HeldLock{fp, lock, off, len, true}); }
  if (!granted && !state.tlb_.Enabled()) { lock->Unpin(); }  // lookup pin dies with the refusal
  return granted;
}

void FasterLockManager::Unlock(timestamp_t txn_ts, const LockableTuple *key) {
  auto &state = Local();
  auto *held  = state.held_.Find(LockableTuple::Fingerprint(key));
  if (held == nullptr || !held->exclusive) {
    throw std::runtime_error("Unlock called without holding an exclusive lock");
  }
  ReleaseHeld(*held, txn_ts, state);  // release first: Remove swap-and-pops over the slot
  state.held_.Remove(held);
}

void FasterLockManager::UnlockShared(timestamp_t txn_ts, const LockableTuple *key) {
  auto &state = Local();
  auto *held  = state.held_.Find(LockableTuple::Fingerprint(key));
  if (held == nullptr || held->exclusive) {
    throw std::runtime_error("UnlockShared called without holding a shared lock");
  }
  ReleaseHeld(*held, txn_ts, state);  // release first: Remove swap-and-pops over the slot
  state.held_.Remove(held);
}

void FasterLockManager::ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) {
  auto &state           = Local();
  const size_t num_held = state.held_.Size();

  if (write_set_cb) {
    // Undo runs while locks are still held; the callback may re-lock and
    // re-append, so swap the arena out first to keep the bytes stable.
    UndoArena undo;
    undo.data_.swap(state.undo_arena_.data_);
    for (size_t i = 0; i < num_held; ++i) {
      const HeldLock held = state.held_.At(i);  // copy: SetUndoPayload rewrites the slot's fields
      if (!held.exclusive || held.undo_off == NO_UNDO_OFF) { continue; }
      auto *tuple           = undo.TupleAt(held.undo_off);
      const u8 *payload_ptr = undo.data_.data() + held.undo_off + offsetof(LockableTuple, key) + tuple->key_len;
      write_set_cb(tuple, std::span<const u8>(payload_ptr, held.undo_len));
    }
    assert(state.held_.Size() == num_held);
    if (undo.data_.capacity() > state.undo_arena_.data_.capacity()) { undo.data_.swap(state.undo_arena_.data_); }
  }

  for (size_t i = 0; i < state.held_.Size(); ++i) { ReleaseHeld(state.held_.At(i), txn_ts, state); }
  state.held_.Clear();
  state.undo_arena_.Clear();

  // 10%-coin throttled boundary GC: the sweep costs ~4% churn per txn.
  epoch_.AdvanceEpoch();
  if (FastXorBasedRandom() % 10 == 0) {
    const auto wid = LeanStore::worker_thread_id;
    sync::EpochGuard guard(&epoch_.local_epoch[wid], epoch_.global_epoch);
    epoch_.Reclaim(wid);
  }
}

auto FasterLockManager::LookupLock(fingerprint_t fp, LocalState &state) -> DeadlockPrevention * {
  if (auto *lock = state.tlb_.Lookup(fp)) { return lock; }
  if (auto *lock = FindPinned(fp)) {
    PutTlb(fp, lock, state);  // keeps the lookup pin as the TLB's standing pin
    return lock;
  }
  auto *candidate = AllocEntry();
  auto *lock      = InsertPinned(fp, candidate);
  if (lock != &candidate->lock) { FreeEntry(candidate); }  // lost an insert race
  PutTlb(fp, lock, state);
  return lock;
}

void FasterLockManager::PutTlb(fingerprint_t fp, DeadlockPrevention *lock, LocalState &state) {
  const LockTlb::Entry evicted = state.tlb_.Put(fp, lock);
  if (evicted.lock != nullptr) { DeferIdle(evicted.fp); }
}

/// Unlock and defer the erase of idle entries
/// Without a TLB the standing pin rode this held entry; drop it here.
void FasterLockManager::ReleaseHeld(const HeldLock &held, timestamp_t txn_ts, LocalState &state) {
  const bool idle = held.exclusive ? held.lock->Unlock(txn_ts) : held.lock->UnlockShared(txn_ts);
  if (!state.tlb_.Enabled()) {
    held.lock->Unpin();
    if (idle) { DeferIdle(held.fingerprint); }
    return;
  }
  if (idle && state.tlb_.Lookup(held.fingerprint) == nullptr) { DeferIdle(held.fingerprint); }
}

void FasterLockManager::DeferIdle(fingerprint_t fp) {
  const auto wid = LeanStore::worker_thread_id;
  sync::EpochGuard guard(&epoch_.local_epoch[wid], epoch_.global_epoch);
  epoch_.DeferFreePointer(
    wid, (void *)(uintptr_t)fp,
    [](void *ptr, void *ctx) {
      // Reclaim iff the table still has it idle; a false result retries later.
      return static_cast<FasterLockManager *>(ctx)->ReclaimIfFree((u64)(uintptr_t)ptr);
    },
    this);
}

void FasterLockManager::DrainDeferred() {
  // A flushed TLB entry may be idle without reaching an epoch list: erase it here.
  for (size_t idx = 0; idx < FLAGS_worker_count; ++idx) {
    for (const auto &e : local_states_[idx].tlb_.Flush()) { (void)ReclaimIfFree(e.fp); }
  }
  for (auto &list : epoch_.to_free_ptr) {
    for (const auto &entry : list) { (void)ReclaimIfFree((u64)(uintptr_t)entry.ptr); }
    list.clear();
  }
}

// -------------------------------------------------------------------------------------
// Table
// -------------------------------------------------------------------------------------
/// Find + Pin under the bucket latch: closes the lookup→reclaim window.
auto FasterLockManager::FindPinned(fingerprint_t fp) -> DeadlockPrevention * {
  const auto b = fp & (NUM_BUCKETS - 1);
  latches_[b].lock();
  for (auto *e = buckets_[b]; e != nullptr; e = e->next) {
    if (e->fp == fp) {
      e->lock.Pin();
      latches_[b].unlock();
      return &e->lock;
    }
  }
  latches_[b].unlock();
  return nullptr;
}

/// Insert the candidate; on a lost race, Pin and return the winner.
auto FasterLockManager::InsertPinned(fingerprint_t fp, FasterEntry *candidate) -> DeadlockPrevention * {
  const auto b = fp & (NUM_BUCKETS - 1);
  latches_[b].lock();
  for (auto *e = buckets_[b]; e != nullptr; e = e->next) {
    if (e->fp == fp) {
      e->lock.Pin();
      latches_[b].unlock();
      return &e->lock;
    }
  }
  candidate->fp   = fp;
  candidate->next = buckets_[b];
  buckets_[b]     = candidate;
  table_size_.fetch_add(1, std::memory_order_relaxed);
  candidate->lock.Pin();
  latches_[b].unlock();
  return &candidate->lock;
}

/// Unlink iff idle (under the bucket latch) and free the entry into its ring.
/// Returns false only when the entry exists but is still held, retries later.
bool FasterLockManager::ReclaimIfFree(fingerprint_t fp) {
  const auto b      = fp & (NUM_BUCKETS - 1);
  FasterEntry *prev = nullptr;
  latches_[b].lock();
  for (auto *e = buckets_[b]; e != nullptr; prev = e, e = e->next) {
    if (e->fp != fp) { continue; }
    const bool idle = e->lock.IsFree();
    if (idle) {
      if (prev == nullptr) {
        buckets_[b] = e->next;
      } else {
        prev->next = e->next;
      }
      e->next = nullptr;
      table_size_.fetch_sub(1, std::memory_order_relaxed);
    }
    latches_[b].unlock();
    if (idle) { FreeEntry(e); }
    return idle;
  }
  latches_[b].unlock();
  return true;  // gone from the table: nothing to reclaim
}

/// Ring-wrap reclaim: unlink an idle entry from the bucket chain
bool FasterLockManager::UnlinkIfFree(FasterEntry *entry) {
  const auto b      = entry->fp & (NUM_BUCKETS - 1);
  FasterEntry *prev = nullptr;
  latches_[b].lock();
  for (auto *e = buckets_[b]; e != nullptr; prev = e, e = e->next) {
    if (e != entry) { continue; }
    if (!e->lock.IsFree()) {
      latches_[b].unlock();
      return false;
    }
    if (prev == nullptr) {
      buckets_[b] = e->next;
    } else {
      prev->next = e->next;
    }
    e->next = nullptr;
    table_size_.fetch_sub(1, std::memory_order_relaxed);
    latches_[b].unlock();
    return true;
  }
  latches_[b].unlock();
  return false;
}

// -------------------------------------------------------------------------------------
// Pool
// -------------------------------------------------------------------------------------
auto FasterLockManager::AllocEntry() -> FasterEntry * {
  const auto wid = LeanStore::worker_thread_id;
  auto &ring     = rings_[wid < MAX_NUMBER_OF_WORKER ? wid : 0];
  ring.latch.lock();
  if (!ring.free.empty()) {
    auto *e   = ring.free.back();
    e->parked = false;
    ring.free.pop_back();
    ring.latch.unlock();
    return e;
  }
  if (ring.cap == 0) { ring.cap = static_cast<u32>(FLAGS_txn_lock_pool_bytes / sizeof(FasterEntry)); }
  if (ring.cap != 0 && ring.base == nullptr) {
    ring.base = static_cast<FasterEntry *>(AllocHuge(ring.cap * sizeof(FasterEntry)));
  }
  if (ring.used < ring.cap) {
    auto *e = new (&ring.base[ring.used++]) FasterEntry();
    ring.latch.unlock();
    return e;
  }
  for (u32 i = 0; i < ring.cap; ++i) {  // ring full: reuse the oldest idle slot
    auto *e = &ring.base[(ring.cursor + i) % ring.cap];
    if (e->parked || !UnlinkIfFree(e)) { continue; }
    ring.cursor = static_cast<u32>((ring.cursor + i + 1) % ring.cap);
    e->fp       = 0;
    e->next     = nullptr;
    ring.latch.unlock();
    return e;
  }
  ring.latch.unlock();
  return new FasterEntry();  // all ring slots busy: overflow
}

void FasterLockManager::FreeEntry(FasterEntry *entry) noexcept {
  for (auto &ring : rings_) {
    if (ring.base != nullptr && entry >= ring.base && entry < ring.base + ring.cap) {
      ring.latch.lock();
      entry->parked = true;  // fungible within its ring: park on the owner's free list
      ring.free.push_back(entry);
      ring.latch.unlock();
      return;
    }
  }
  delete entry;  // overflow entry: not in any ring
}

// -------------------------------------------------------------------------------------
FasterLockManager::FasterLockManager() : local_states_(FLAGS_worker_count) {}

FasterLockManager::~FasterLockManager() {
  // Drain before the rings die: deferred erases free entries into them.
  DrainDeferred();
  // Rings are thread_local-lifetime storage: only unmap their backing memory.
  for (auto &ring : rings_) {
    if (ring.base != nullptr) { munmap(ring.base, ring.cap * sizeof(FasterEntry)); }
  }
}

// -------------------------------------------------------------------------------------
}  // namespace leanstore::transaction
