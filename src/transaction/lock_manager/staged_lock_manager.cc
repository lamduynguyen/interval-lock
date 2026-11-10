#include "transaction/lock_manager/staged_lock_manager.h"

#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/leanstore.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace leanstore::transaction {
// ---------------------------------------------------------------------------
auto StagedLockManager::Local() -> LocalState & {
  assert(LeanStore::worker_thread_id < FLAGS_worker_count);
  return local_states_[LeanStore::worker_thread_id];
}

// ---------------------------------------------------------------------------
auto StagedLockManager::ReturnToPool(void *ptr, void *ctx) -> bool {
  (void)ctx;
  auto *node = static_cast<LockNode *>(ptr);
  node->owner->pool_.Free(node);
  return true;
}

StagedLockManager::StagedLockManager()
    : local_states_(FLAGS_worker_count), pool_([this](LockNode *node) { node->owner = this; }) {
  WarningMessage("StagedLockManager only runs deadlock detection; the locking policy flag is ignored");
}

StagedLockManager::~StagedLockManager() {
  // Drain the deferred frees: segment nodes park back in the pool, only the overflow ones die.
  for (auto &list : epoch_.to_free_ptr) {
    for (auto &entry : list) {
      auto *node = static_cast<LockNode *>(entry.ptr);
      if (!pool_.Owns(node)) { delete node; }
    }
    list.clear();
  }
  for (u64 b = 0; b < NUM_BUCKETS; ++b) {
    auto *node = buckets_[b].load(std::memory_order_relaxed);
    while (node != nullptr) {
      auto *next = node->next.load(std::memory_order_relaxed);
      if (!pool_.Owns(node)) { delete node; }
      node = next;
    }
  }
}

bool StagedLockManager::EmptyLocalSet() { return Local().read_set_.Empty() && Local().write_set_.Empty(); }

// ---------------------------------------------------------------------------
// Chain manipulation
// ---------------------------------------------------------------------------
auto StagedLockManager::InsertNode(timestamp_t txn_ts, LockMode mode, fingerprint_t fp) -> LockNode * {
  auto *node   = pool_.Alloc();
  node->fp     = fp;
  node->txn_ts = txn_ts;
  node->mode   = mode;
  node->seq    = next_seq_.fetch_add(1, std::memory_order_relaxed);
  node->state.store(State::WAIT, std::memory_order_relaxed);
  live_nodes_.fetch_add(1, std::memory_order_relaxed);
  waiting_.fetch_add(1, std::memory_order_release);

  // Latch-free push-front: insert BEFORE scanning, so scanners never miss us
  auto &head          = buckets_[BucketOf(fp)];
  LockNode *head_prev = head.load(std::memory_order_acquire);
  do {
    node->next.store(head_prev, std::memory_order_release);
  } while (!head.compare_exchange_weak(head_prev, node, std::memory_order_acq_rel));
  return node;
}

auto StagedLockManager::IsBlocked(LockNode *me) -> bool {
  // Blocked while an incompatible GRANTED node or earlier-seq waiter exists.
  auto *node = buckets_[BucketOf(me->fp)].load(std::memory_order_acquire);
  while (node != nullptr) {
    auto state = node->state.load(std::memory_order_acquire);
    if (node != me && node->txn_ts != me->txn_ts && state != State::OBSOLETE && node->fp == me->fp &&
        Conflicts(node->mode, me->mode)) {
      // Granted nodes block; among waiters, only earlier-seq ones block (FIFO)
      if (state == State::GRANTED || node->seq < me->seq) { return true; }
    }
    node = node->next.load(std::memory_order_acquire);
  }
  return false;
}

// ---------------------------------------------------------------------------
// Acquisition
// ---------------------------------------------------------------------------
auto StagedLockManager::AcquireNode(LockNode *node) -> bool {
  // Re-scan, deadlock-check, poll (yields, then backoff sleeps); false on deadlock.
  auto try_grant = [&] {
    sync::EpochGuard guard(&epoch_.local_epoch[LeanStore::worker_thread_id], epoch_.global_epoch);
    if (IsBlocked(node)) { return false; }
    node->state.store(State::GRANTED, std::memory_order_release);
    waiting_.fetch_sub(1, std::memory_order_release);
    return true;
  };

  u64 sleep_us = SLEEP_START_US;
  u64 poll     = 0;
  bool hinted  = false;
  while (true) {
    if (try_grant()) { return true; }
    // Hint my bucket for the deadlock check (stale bits only cost a walk)
    if (!hinted) {
      const auto bucket = BucketOf(node->fp);
      hints_[bucket >> 5].fetch_or(1ull << (bucket & 31), std::memory_order_release);
      hinted = true;
    }

    // The wait-for-graph walk is expensive: run it rarely, not per poll.
    if (poll % DEADLOCK_CHECK_INTERVAL == 0 && DeadlockExists(node)) { return false; }
    ++poll;
    // Brief spin, then sleep-poll: plain polling beats wake machinery.
    for (u64 i = 0; i < YIELDS_BEFORE_SLEEP; ++i) {
      AsmYield();
      if (try_grant()) { return true; }
    }
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    if (sleep_us < MAX_SLEEP_US) { sleep_us = std::min<u64>(sleep_us * 4, MAX_SLEEP_US); }
  }
}

bool StagedLockManager::TryLockShared(timestamp_t txn_ts, const LockableTuple *key, [[maybe_unused]] pageid_t page_id,
                                      [[maybe_unused]] leng_t slot_id) {
  const u64 fp = LockableTuple::Fingerprint(key);
  auto &state  = Local();
  if (state.read_set_.Find(fp) != nullptr || state.write_set_.Find(fp) != nullptr) { return true; }
  auto *node = InsertNode(txn_ts, LockMode::SHARED, fp);
  if (!AcquireNode(node)) {
    RetireNode(node);
    return false;
  }
  state.read_set_.Add(fingerprint_t{fp});
  return true;
}

bool StagedLockManager::TryLockExclusive(timestamp_t txn_ts, const LockableTuple *key) {
  const u64 fp = LockableTuple::Fingerprint(key);
  auto &state  = Local();
  if (state.write_set_.Find(fp) != nullptr) { return true; }

  auto *node = InsertNode(txn_ts, LockMode::EXCLUSIVE, fp);
  if (!AcquireNode(node)) {
    RetireNode(node);
    return false;
  }

  // Upgrade: retire my earlier S node once X is granted, so it stops blocking others;
  // Fresh X: an insert, or an upgrade, must have a payload placeholder either way --
  // a release replays a write without undo payload as a delete of the row it inserted in.
  if (auto *s_node = FindOwnNode(txn_ts, fp, LockMode::SHARED); s_node != nullptr) {
    RetireNode(s_node);
    state.read_set_.Remove(state.read_set_.Find(fp));
  }
  const auto [off, len] = state.undo_arena_.Append(key, {});
  state.write_set_.Add(UndoSlot{fp, off, len});
  return true;
}

// ---------------------------------------------------------------------------
// Deadlock detection: wait-for graph cycle check, abort-self policy
// ---------------------------------------------------------------------------

auto StagedLockManager::DeadlockExists(LockNode *node) -> bool {
  if (waiting_.load(std::memory_order_acquire) < 2) { return false; }  // a cycle needs 2 waiters

  auto &state = Local();
  state.waiters_.clear();
  state.frontier_.clear();
  state.visited_.clear();

  sync::EpochGuard guard(&epoch_.local_epoch[LeanStore::worker_thread_id], epoch_.global_epoch);

  // Collect waiters from hinted buckets only.
  for (u64 w = 0; w < NUM_BUCKETS / 32; ++w) {
    auto bits = hints_[w].load(std::memory_order_acquire) & 0xFFFFFFFFu;
    while (bits != 0) {
      const auto bucket = (w << 5) + std::countr_zero(bits);
      bits &= bits - 1;
      auto *p = buckets_[bucket].load(std::memory_order_acquire);
      while (p != nullptr) {
        if (p->state.load(std::memory_order_acquire) == State::WAIT) { state.waiters_.push_back(p); }
        p = p->next.load(std::memory_order_acquire);
      }
    }
  }

  // BFS from my blockers; a path back to my txn_ts is a deadlock.
  auto push_blockers = [&](fingerprint_t fp, LockMode mode, u64 my_seq, bool skip_self) {
    auto *p = buckets_[BucketOf(fp)].load(std::memory_order_acquire);
    while (p != nullptr) {
      auto node_state = p->state.load(std::memory_order_acquire);
      if (node_state != State::OBSOLETE && (node_state == State::GRANTED || p->seq < my_seq) &&
          (!skip_self || p->txn_ts != node->txn_ts) && p->fp == fp && Conflicts(p->mode, mode)) {
        state.frontier_.push_back(p->txn_ts);
      }
      p = p->next.load(std::memory_order_acquire);
    }
  };
  push_blockers(node->fp, node->mode, node->seq, /*skip_self=*/true);

  while (!state.frontier_.empty()) {
    auto txn = state.frontier_.back();
    state.frontier_.pop_back();
    if (txn == node->txn_ts) { return true; }
    if (!state.visited_.insert(txn).second) { continue; }
    for (auto *w : state.waiters_) {
      if (w->txn_ts == txn) { push_blockers(w->fp, w->mode, w->seq, /*skip_self=*/false); }
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Release / staged deallocation
// ---------------------------------------------------------------------------

void StagedLockManager::RetireNode(LockNode *node) {
  auto was_waiting = node->state.exchange(State::OBSOLETE, std::memory_order_acq_rel) == State::WAIT;
  live_nodes_.fetch_sub(1, std::memory_order_relaxed);
  if (was_waiting) { waiting_.fetch_sub(1, std::memory_order_release); }
  // Flag the bucket dirty AFTER the OBSOLETE store: a GC pass that sees the
  // flag is guaranteed to observe the state during its chain walk.
  const auto bucket = BucketOf(node->fp);
  hints_[bucket >> 5].fetch_or(1ull << (32 + (bucket & 31)), std::memory_order_release);
  const auto releases = Local().local_releases++;
  if (releases % RELEASES_PER_EPOCH == 0) { epoch_.AdvanceEpoch(); }
  if (releases % RELEASES_PER_GC == 0) { CollectGarbage(); }
}

auto StagedLockManager::FindOwnNode(timestamp_t txn_ts, fingerprint_t fp, LockMode mode) -> LockNode * {
  sync::EpochGuard guard(&epoch_.local_epoch[LeanStore::worker_thread_id], epoch_.global_epoch);
  auto *node = buckets_[BucketOf(fp)].load(std::memory_order_acquire);
  while (node != nullptr) {
    if (node->txn_ts == txn_ts && node->mode == mode &&
        node->state.load(std::memory_order_acquire) != State::OBSOLETE && node->fp == fp) {
      return node;
    }
    node = node->next.load(std::memory_order_acquire);
  }
  return nullptr;
}

void StagedLockManager::Unlock(timestamp_t txn_ts, const LockableTuple *key) {
  const u64 fp = LockableTuple::Fingerprint(key);
  auto &state  = Local();
  if (state.write_set_.Find(fp) == nullptr) {
    throw std::runtime_error("Unlock called without holding an exclusive lock");
  }
  auto *node = FindOwnNode(txn_ts, fp, LockMode::EXCLUSIVE);
  if (node == nullptr) { throw std::runtime_error("Unlock: lock node missing in hash chain"); }
  state.write_set_.Remove(state.write_set_.Find(fp));
  RetireNode(node);
}

void StagedLockManager::UnlockShared(timestamp_t txn_ts, const LockableTuple *key) {
  const u64 fp = LockableTuple::Fingerprint(key);
  auto &state  = Local();
  if (state.read_set_.Find(fp) == nullptr) {
    throw std::runtime_error("UnlockShared called without holding a shared lock");
  }
  auto *node = FindOwnNode(txn_ts, fp, LockMode::SHARED);
  if (node == nullptr) { throw std::runtime_error("UnlockShared: lock node missing in hash chain"); }
  state.read_set_.Remove(state.read_set_.Find(fp));
  RetireNode(node);
}

void StagedLockManager::SetUndoPayload(timestamp_t txn_ts, const LockableTuple *key, std::span<const u8> payload) {
  (void)txn_ts;
  const u64 fp = LockableTuple::Fingerprint(key);
  auto &state  = Local();
  auto *slot   = state.write_set_.Find(fp);
  if (slot == nullptr) { throw std::runtime_error("SetUndoPayload called without holding an exclusive lock"); }
  const auto [off, len] = state.undo_arena_.Append(key, payload);
  slot->undo_off        = off;
  slot->undo_len        = len;
}

void StagedLockManager::ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) {
  auto &state = Local();
  for (size_t i = 0; i != state.read_set_.Size(); ++i) {
    auto *node = FindOwnNode(txn_ts, state.read_set_.At(i), LockMode::SHARED);
    if (node == nullptr) { throw std::runtime_error("ReleaseAllLocks: S-lock node missing in hash chain"); }
    RetireNode(node);
  }
  state.read_set_.Clear();

  UndoArena undo;
  undo.data_.swap(state.undo_arena_.data_);  // hand the undo images over before the callback

  // Slots are copied: SetUndoPayload during a relock rewrites the slot's undo fields
  for (size_t i = 0; i != state.write_set_.Size(); ++i) {
    const UndoSlot slot = state.write_set_.At(i);
    if (write_set_cb && slot.undo_off != NO_UNDO_OFF) {
      write_set_cb(undo.TupleAt(slot.undo_off), undo.PayloadAt(slot.undo_off, slot.undo_len));
    }
    auto *node = FindOwnNode(txn_ts, slot.fingerprint, LockMode::EXCLUSIVE);
    if (node == nullptr) { throw std::runtime_error("ReleaseAllLocks: X-lock node missing in hash chain"); }
    RetireNode(node);
  }
  state.write_set_.Clear();
  if (undo.data_.capacity() > state.undo_arena_.data_.capacity()) {
    undo.data_.swap(state.undo_arena_.data_);
  }  // keep the larger
  // Keep the capacity but drop the records, which belong to the transaction that just ended; a
  // bump-pointer arena that keeps them re-copies everything each time it doubles.
  state.undo_arena_.Clear();
}

// ---------------------------------------------------------------------------
auto StagedLockManager::GcBucket(u64 bucket) -> bool {
  // CAS-unlink OBSOLETE runs; false when another GC held the stripe.
  std::mutex &stripe = gc_locks_[bucket & (NUM_BUCKETS / 64 - 1)];
  if (!stripe.try_lock()) { return false; }
  std::unique_lock<std::mutex> lock(stripe, std::adopt_lock);
  sync::EpochGuard guard(&epoch_.local_epoch[LeanStore::worker_thread_id], epoch_.global_epoch);
  const wid_t wid = LeanStore::worker_thread_id;

  // Unlink leading OBSOLETE run
  while (true) {
    auto *head = buckets_[bucket].load(std::memory_order_acquire);
    if (head == nullptr || head->state.load(std::memory_order_acquire) != State::OBSOLETE) { break; }
    auto *after = head->next.load(std::memory_order_acquire);
    if (buckets_[bucket].compare_exchange_strong(head, after)) { epoch_.DeferFreePointer(wid, head, ReturnToPool); }
  }

  // Unlink interior OBSOLETE runs: CAS the last live node's next pointer.
  auto *prev = buckets_[bucket].load(std::memory_order_acquire);
  while (prev != nullptr) {
    auto *cur = prev->next.load(std::memory_order_acquire);
    if (cur == nullptr) { break; }
    if (cur->state.load(std::memory_order_acquire) == State::OBSOLETE) {
      auto *last = cur;
      while (last->next.load(std::memory_order_acquire) != nullptr &&
             last->next.load(std::memory_order_acquire)->state.load(std::memory_order_acquire) == State::OBSOLETE) {
        last = last->next.load(std::memory_order_acquire);
      }
      auto *after = last->next.load(std::memory_order_acquire);
      if (prev->next.compare_exchange_strong(cur, after)) {
        for (auto *p = cur; p != after && p != nullptr;) {
          auto *n = p->next.load(std::memory_order_acquire);
          epoch_.DeferFreePointer(wid, p, ReturnToPool);
          p = n;
        }
      }
      continue;  // re-read prev->next after the CAS attempt
    }
    prev = cur;
  }

  bool has_waiter = false;  // no waiter left: the deadlock check can skip this bucket
  for (auto *p = buckets_[bucket].load(std::memory_order_acquire); p != nullptr && !has_waiter;
       p       = p->next.load(std::memory_order_acquire)) {
    has_waiter = p->state.load(std::memory_order_acquire) == State::WAIT;
  }
  if (!has_waiter) { hints_[bucket >> 5].fetch_and(~(1ull << (bucket & 31)), std::memory_order_release); }
  return true;
}

// ---------------------------------------------------------------------------
void StagedLockManager::CollectGarbage() {
  epoch_.Reclaim(LeanStore::worker_thread_id);  // reclaim first: this pass's returns go next pass
  for (u64 w = 0; w < NUM_BUCKETS / 32; ++w) {
    auto bits = hints_[w].load(std::memory_order_acquire) >> 32;
    while (bits != 0) {
      const auto bucket = (w << 5) + std::countr_zero(bits);
      bits &= bits - 1;
      const auto bit = 1ull << (32 + (bucket & 31));
      if (hints_[w].fetch_and(~bit, std::memory_order_acq_rel) & bit) {
        if (!GcBucket(bucket)) { hints_[w].fetch_or(bit, std::memory_order_release); }  // stripe busy: retry next pass
      }
    }
  }
}

// ---------------------------------------------------------------------------
}  // namespace leanstore::transaction
