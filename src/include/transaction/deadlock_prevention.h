#pragma once

#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "share_headers/logger.h"
#include "sync/spin_latch.h"
#include "transaction/lock_manager_interface.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace leanstore::transaction {

/** Deadlock-prevention lock queue: the no-wait and wait-die policies behind one shared API.
 *  Originally based on https://github.com/rotaki/tpcc-runner/blob/master/protocols/waitdie/include/waitdielock.hpp
 *
 *  Each object captures `FLAGS_txn_locking_policy` at construction and then runs
 *  exactly that policy's grant logic:
 *  - no-wait refuses on any conflict regardless of age;
 *  - wait-die makes the older transaction wait and the younger one abort.
 *
 *  Optimizations over original implementation:
 *  - no heap allocation on the lock/unlock path
 *  - fixes a use-after-free bug in PromoteWaiters()
 *  - O(1) owner insert and O(1) min-timestamp lookup
 *  - tiny TTAS spin latch instead of std::mutex for the few-instruction critical sections
 */
class DeadlockPrevention {
 private:
  enum Operation : uint8_t { I, S, E, U };

  struct alignas(64) Waiter {
    Waiter(timestamp_t ts, Operation op) : ts(ts), op(op) {}

    std::atomic<bool> waiting{true};
    timestamp_t ts;
    Operation op;
    Waiter *next = nullptr;
  };

  struct OwnerSet {
    static constexpr timestamp_t NO_OWNER = std::numeric_limits<timestamp_t>::max();
    static constexpr uint32_t INLINE_TS   = 16;

    Operation op       = I;         // I, S, E
    timestamp_t min_ts = NO_OWNER;  // smallest (oldest) owner timestamp
    uint32_t size      = 0;
    timestamp_t inline_ts[INLINE_TS];  // first INLINE_TS owners: no allocation for the common case
    std::vector<timestamp_t> spill;    // owners beyond INLINE_TS

    bool Empty() const { return size == 0; }

    size_t Size() const { return size; }

    timestamp_t &At(uint32_t i) { return i < INLINE_TS ? inline_ts[i] : spill[i - INLINE_TS]; }

    timestamp_t At(uint32_t i) const { return i < INLINE_TS ? inline_ts[i] : spill[i - INLINE_TS]; }

    void Insert(timestamp_t t) {
      if (size < INLINE_TS) {
        inline_ts[size] = t;
      } else {
        spill.push_back(t);
      }
      ++size;
      if (t < min_ts) { min_ts = t; }
    }

    void Remove(timestamp_t t) {
      uint32_t i = 0;
      while (i < size && At(i) != t) { ++i; }
      if (i == size) { throw std::runtime_error("timestamp not in list"); }
      At(i) = At(size - 1);
      if (size > INLINE_TS) { spill.pop_back(); }
      --size;
      if (size == 0) {
        op     = I;
        min_ts = NO_OWNER;
      } else if (t == min_ts) {
        min_ts = At(0);
        for (uint32_t j = 1; j < size; ++j) { min_ts = std::min(min_ts, At(j)); }
      }
    }
  };

  sync::SpinLatch latch_;
  Waiter *waiters_ = nullptr;  // intrusive list, sorted by ts descending (youngest first)
  OwnerSet owners_;
  std::atomic<uint64_t> refs_{0};
  LockPolicy policy_ = static_cast<LockPolicy>(FLAGS_txn_locking_policy);

 public:
  DeadlockPrevention() = default;

  // --- Lock-table GC support ---
  void Pin() { refs_.fetch_add(1, std::memory_order_relaxed); }

  void Unpin() { refs_.fetch_sub(1, std::memory_order_release); }

  bool IsFree() {
    std::lock_guard guard(latch_);
    return waiters_ == nullptr && owners_.Empty() && refs_.load(std::memory_order_acquire) == 0;
  }

  void Trace() {
    std::lock_guard guard(latch_);
    TraceWithoutLatch();
  }

  void TraceWithoutLatch() { LOG_DEBUG("Waiter: {}; Owner: {}", WaitersToString(), OwnersToString()); }

  bool TryLockShared(timestamp_t ts) {
    std::unique_lock guard(latch_);
    const Operation op   = owners_.op;
    const bool no_waiter = (waiters_ == nullptr);

    // No-wait: nobody can ever be queued, so the waiter list stays empty.
    // The only question is whether an exclusive owner holds the tuple.
    if (policy_ == LockPolicy::NO_WAIT) {
      if (op == I || op == S) {
        owners_.Insert(ts);
        owners_.op = S;
        return true;
      }
      return false;
    }

    // owner(I, S), no waiter -> grant
    if ((op == I || op == S) && no_waiter) {
      owners_.Insert(ts);
      owners_.op = S;
      return true;
    }
    // owner(I), waiter -> wait (defensive; PromoteWaiters never leaves this state behind)
    if (op == I) { return EnqueueAndWait(guard, ts, S); }
    // owner(S), waiter  or  owner(E) -> wait if older than every owner, otherwise die
    if (ts < owners_.min_ts) { return EnqueueAndWait(guard, ts, S); }
    return false;
  }

  bool TryLockExclusive(timestamp_t ts) {
    std::unique_lock guard(latch_);
    const Operation op = owners_.op;

    // No-wait: an X request never queues, so it lands only on a completely idle tuple.
    // Every shared holder aborts us, no matter how much younger it is.
    if (policy_ == LockPolicy::NO_WAIT) {
      if (op == I) {
        owners_.Insert(ts);
        owners_.op = E;
        return true;
      }
      return false;
    }

    if (op == I) {
      if (waiters_ == nullptr) {
        owners_.Insert(ts);
        owners_.op = E;
        return true;
      }
      return EnqueueAndWait(guard, ts, E);
    }
    // owner(S, E) -> wait if older than every owner, otherwise die
    if (ts < owners_.min_ts) { return EnqueueAndWait(guard, ts, E); }
    return false;
  }

  bool TryLockUpgrade(timestamp_t ts) {
    std::unique_lock guard(latch_);
    if (owners_.op != S) { throw std::runtime_error("No lock to upgrade"); }
    // No-wait: nobody can be queued behind us, so the upgrade only lands when we are the only shared owner.
    if (policy_ == LockPolicy::NO_WAIT) {
      if (owners_.Size() == 1) {
        owners_.op = E;
        return true;
      }
      return false;
    }
    if (owners_.min_ts != ts) { return false; }  // a holder older than us exists -> die
    if (owners_.Size() == 1) {
      owners_.op = E;
      return true;
    }
    // Our ts is the minimum owner ts, and every waiter is older than the owner minimum
    // at enqueue time, so this node lands at the head of the waiter list.
    return EnqueueAndWait(guard, ts, U);
  }

  bool UnlockShared(timestamp_t ts) {
    std::lock_guard guard(latch_);
    if (owners_.op != S) { throw std::runtime_error("No shared lock to unlock"); }
    owners_.Remove(ts);
    PromoteWaiters();
    return owners_.Empty() && waiters_ == nullptr;
  }

  bool Unlock(timestamp_t ts) {
    std::lock_guard guard(latch_);
    if (owners_.op != E) { throw std::runtime_error("No exclusive lock to unlock"); }
    owners_.Remove(ts);
    PromoteWaiters();
    return owners_.Empty() && waiters_ == nullptr;
  }

 private:
  // Requires latch_ held; releases it before waiting.
  bool EnqueueAndWait(std::unique_lock<sync::SpinLatch> &guard, timestamp_t ts, Operation op) {
    Waiter self(ts, op);

    // insert before the first waiter with a smaller ts (descending order, FIFO among equals)
    Waiter **pp = &waiters_;
    while (*pp != nullptr && (*pp)->ts >= ts) { pp = &(*pp)->next; }
    self.next = *pp;
    *pp       = &self;

    // With no owners nobody would ever wake us, so try to grant right away.
    if (owners_.op == I) { PromoteWaiters(); }
    guard.unlock();

    while (self.waiting.load(std::memory_order_acquire)) { AsmYield(); }
    return true;
  }

  // Requires latch_ held.
  void PromoteWaiters() {
    while (waiters_ != nullptr) {
      Waiter *w            = waiters_;
      const Operation o_op = owners_.op;

      switch (w->op) {
        case S:
          if (o_op == E) { return; }
          owners_.Insert(w->ts);
          owners_.op = S;
          break;
        case E:
          if (o_op != I) { return; }
          owners_.Insert(w->ts);
          owners_.op = E;
          break;
        case U:
          if (o_op != S) { throw std::runtime_error("Failed Promoting Waiter"); }
          if (owners_.Size() > 1) { return; }
          assert(owners_.min_ts == w->ts);
          owners_.op = E;
          break;
        default: throw std::runtime_error("Unhandled State Found");
      }

      waiters_ = w->next;
      w->waiting.store(false, std::memory_order_release);
    }
  }

  static const char *OpName(Operation op) {
    switch (op) {
      case S: return "S";
      case E: return "E";
      case U: return "U";
      default: return "I";
    }
  }

  std::string WaitersToString() const {
    std::string out = "[";
    for (const Waiter *w = waiters_; w != nullptr; w = w->next) {
      if (w != waiters_) { out += ", "; }
      out += "W(ts=" + std::to_string(w->ts) + ", op=" + OpName(w->op) + ")";
    }
    return out + "]";
  }

  std::string OwnersToString() const {
    std::string out = std::string("(") + OpName(owners_.op) + "):[";
    for (uint32_t i = 0; i < owners_.size; ++i) {
      if (i != 0) { out += ", "; }
      out += "O(ts=" + std::to_string(owners_.At(i)) + ")";
    }
    return out + "]";
  }
};

}  // namespace leanstore::transaction
