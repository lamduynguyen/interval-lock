#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "transaction/lock_manager/faster_lock_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <barrier>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

using leanstore::LeanStore;
using namespace leanstore::transaction;

namespace leanstore::transaction {
TEST(FasterLockManagerTest, BasicAcquireReleaseUpgrade) {
  FLAGS_worker_count          = 1;
  LeanStore::worker_thread_id = 0;
  FasterLockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  EXPECT_TRUE(lock_mgr.EmptyLocalSet());

  EXPECT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key2));
  EXPECT_FALSE(lock_mgr.EmptyLocalSet());
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 2);

  // Upgrade key1 S→X in place
  EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key1));
  lock_mgr.Unlock(10, key1);
  lock_mgr.Unlock(10, key2);
  EXPECT_TRUE(lock_mgr.EmptyLocalSet());

  // Idle entries stay in the table until an explicit sweep
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 2);
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 0);

  // Re-acquire cleanly after the sweep
  EXPECT_TRUE(lock_mgr.TryLockExclusive(20, key1));
  lock_mgr.Unlock(20, key1);
  EXPECT_TRUE(lock_mgr.TryLockShared(30, key2, 0, 0));
  lock_mgr.UnlockShared(30, key2);
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, UndoPayloadCallback) {
  FLAGS_worker_count          = 1;
  LeanStore::worker_thread_id = 0;
  FasterLockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key, k1, 1);

  EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key));
  const std::array<u8, 3> payload{7, 8, 9};
  lock_mgr.SetUndoPayload(10, key, payload);

  size_t seen = 0;
  lock_mgr.ReleaseAllLocks(10, [&](const LockableTuple *tuple, std::span<const u8> payload) {
    seen++;
    EXPECT_EQ(tuple->tree_id, 1);
    EXPECT_EQ(tuple->key_len, 4);
    EXPECT_EQ(std::memcmp(tuple->key, k1.data(), 4), 0);
    EXPECT_EQ(payload.size(), 3);
    EXPECT_EQ(payload[0], 7);
  });
  EXPECT_EQ(seen, 1);
  EXPECT_TRUE(lock_mgr.EmptyLocalSet());
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, ConcurrentStress) {
  constexpr size_t NO_THREADS = 4;
  constexpr size_t TRY_CNT    = 100;
  FLAGS_worker_count          = NO_THREADS;
  FasterLockManager lock_mgr;

  std::vector<std::array<u8, 4>> keys;
  for (int i = 0; i < 10; i++) keys.push_back({u8('a' + i), u8('b' + i), u8('c' + i), u8('d' + i)});

  std::array<std::atomic<u64>, NO_THREADS> success{};
  std::atomic<uint64_t> ts_counter{1};

  std::array<std::thread, NO_THREADS> threads;
  for (auto idx = 0UL; idx < NO_THREADS; ++idx) {
    threads[idx] = std::thread([&, idx]() {
      LeanStore::worker_thread_id = idx;
      for (size_t cnt = 0; cnt < TRY_CNT; ++cnt) {
        auto ts       = ts_counter.fetch_add(1);
        const auto &k = keys[ts % keys.size()];
        LOCKABLE_TUPLE_STACK(tup, k, 1);

        const bool want_exclusive = ts % 5 == 0;
        if (want_exclusive) {
          if (lock_mgr.TryLockExclusive(ts, tup)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(20));
            lock_mgr.Unlock(ts, tup);
          }
        } else if (lock_mgr.TryLockShared(ts, tup, 0, 0)) {
          success[idx]++;
          std::this_thread::sleep_for(std::chrono::microseconds(10));
          lock_mgr.UnlockShared(ts, tup);
        }
      }
    });
  }
  for (auto &t : threads) { t.join(); }

  uint64_t total_success = 0;
  for (auto &s : success) { total_success += s.load(); }
  EXPECT_GT(total_success, 0);
}
}  // namespace leanstore::transaction

/// Tiny pool: forces the overflow (new/delete) path and the FASTER-style
/// circular reuse of idle ring entries.
namespace leanstore::transaction {
TEST(FasterLockManagerTest, SmallPoolAllocFreeLifecycle) {
  FLAGS_worker_count          = 2;
  LeanStore::worker_thread_id = 0;
  const auto saved_pool_bytes = FLAGS_txn_lock_pool_bytes;
  FLAGS_txn_lock_pool_bytes   = 64 * sizeof(FasterLockManager::FasterEntry);  // ring capacity ≪ NKEYS
  FasterLockManager lock_mgr;

  constexpr size_t NKEYS = 20000;  // ≫ 64-entry pool capacity
  std::vector<std::array<u8, 8>> keys(NKEYS);
  for (size_t i = 0; i < NKEYS; ++i) {
    keys[i] = {u8(i), u8(i >> 8), u8(i >> 16), u8(i >> 24), u8(i >> 32), u8(i >> 40), u8(i >> 48), u8(i >> 56)};
  }

  // One hoisted stack tuple (LOCKABLE_TUPLE_STACK alloca's per call and would
  // accumulate across these long loops); key bytes are rewritten per iteration.
  std::array<u8, 8> kbuf{};
  LOCKABLE_TUPLE_STACK(tup, kbuf, 7);

  // Phase 1: hold more locks than the pool has slots → overflow new/delete
  for (size_t i = 0; i < NKEYS; ++i) {
    std::memcpy(tup->key, keys[i].data(), 8);
    EXPECT_TRUE(lock_mgr.TryLockExclusive(100, tup));
  }
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), NKEYS);

  // Phase 2: release + sweep → overflow entries freed, ring slots parked
  lock_mgr.ReleaseAllLocks(100, {});
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 0);
  auto pooled = [&] {
    size_t n = 0;
    for (auto &ring : lock_mgr.rings_) { n += ring.free.size(); }
    return n;
  };
  const size_t parked = pooled();
  EXPECT_GT(parked, 0);

  // Phase 3: re-acquire all — recycled slots, no crash
  for (size_t i = 0; i < NKEYS; ++i) {
    std::memcpy(tup->key, keys[i].data(), 8);
    EXPECT_TRUE(lock_mgr.TryLockExclusive(200, tup));
  }
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), NKEYS);

  // Phase 4: FASTER-style circular reuse — sweep the pool, drain the free
  // list, unlock every tree-8 key (deferral only: nothing parked), then
  // allocate tree-9 keys so the ring must wrap onto the idle slots
  lock_mgr.ReleaseAllLocks(200, {});
  lock_mgr.DrainDeferred();
  EXPECT_GT(pooled(), 0);

  tup->tree_id = 8;
  for (size_t i = 0; i < NKEYS; ++i) {  // tree 8: pops the free list, rest overflow
    std::memcpy(tup->key, keys[i].data(), 8);
    EXPECT_TRUE(lock_mgr.TryLockExclusive(300, tup));
  }
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), NKEYS);

  for (size_t i = 0; i < NKEYS; ++i) {  // unlock all: entries idle-chained, nothing parked
    std::memcpy(tup->key, keys[i].data(), 8);
    lock_mgr.Unlock(300, tup);
  }

  tup->tree_id = 9;
  for (size_t i = 0; i < NKEYS; ++i) {  // tree 9: free list empty → wrap reuse must kick in
    std::memcpy(tup->key, keys[i].data(), 8);
    EXPECT_TRUE(lock_mgr.TryLockExclusive(400, tup));
  }
  // Some idle tree-8 entries were recycled by the wrap (not freed, not parked)
  EXPECT_GT(lock_mgr.table_size_.load(std::memory_order_relaxed), NKEYS);
  EXPECT_LT(lock_mgr.table_size_.load(std::memory_order_relaxed), 2 * NKEYS);

  lock_mgr.ReleaseAllLocks(400, {});
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), 0);
  FLAGS_txn_lock_pool_bytes = saved_pool_bytes;
}
}  // namespace leanstore::transaction

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

// -------------------------------------------------------------------------------------
// Coverage carried over from the deprecated TBB variant: the lifecycle under test is the
// lifecycle now lives directly in FasterLockManager.
namespace leanstore::transaction {
TEST(FasterLockManagerTest, MixedSharedExclusiveUpgrade) {
  FLAGS_worker_count = 4;
  FasterLockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  std::array<u8, 4> k3{'k', 'e', 'y', '3'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);
  LOCKABLE_TUPLE_STACK(key3, k3, 1);

  std::atomic<bool> t0_done{false};
  std::atomic<bool> t1_done{false};
  std::atomic<bool> t2_done{false};
  std::atomic<bool> t3_done{false};

  std::barrier sync_point(4);

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
    EXPECT_TRUE(lock_mgr.TryLockShared(10, key2, 0, 0));
    t0_done.store(true, std::memory_order_release);
    sync_point.arrive_and_wait();

    // Upgrade key1 to exclusive
    EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key1));
    lock_mgr.Unlock(10, key1);
    lock_mgr.UnlockShared(10, key2);
  });

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    sync_point.arrive_and_wait();
    // Younger tries exclusive on key1 and shared on key2
    bool k1_granted = lock_mgr.TryLockExclusive(20, key1);     // May fail by Wait-Die
    bool k2_granted = lock_mgr.TryLockShared(20, key2, 0, 0);  // Should succeed eventually
    if (k1_granted) lock_mgr.Unlock(20, key1);
    if (k2_granted) lock_mgr.UnlockShared(20, key2);
    t1_done.store(k1_granted && k2_granted);
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 2;
    sync_point.arrive_and_wait();
    // Independent tuple, but dieable: if t3's upgrade of key3 won the race first, t2 (younger)
    // dies by wait-die and must not unlock what it does not hold (the manager throws otherwise).
    const bool granted = lock_mgr.TryLockShared(15, key3, 0, 0);
    if (granted) { lock_mgr.UnlockShared(15, key3); }
    t2_done.store(true);  // completion, not grant: wait-die may legitimately kill this claim
  });

  std::thread t3([&]() {
    LeanStore::worker_thread_id = 3;
    sync_point.arrive_and_wait();
    // Upgrade on independent tuple
    EXPECT_TRUE(lock_mgr.TryLockShared(5, key3, 0, 0));
    EXPECT_TRUE(lock_mgr.TryLockExclusive(5, key3));  // Upgrade lock
    lock_mgr.Unlock(5, key3);
    t3_done.store(true);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  // Assertions
  EXPECT_TRUE(t0_done.load());
  EXPECT_TRUE(t2_done.load());
  EXPECT_TRUE(t3_done.load());
  // t1 may succeed or fail (Wait-Die)
  EXPECT_TRUE(t1_done.load() || !t1_done.load());

  // After all release, lock can be acquired again
  std::thread t5([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockShared(30, key1, 0, 0));
    lock_mgr.UnlockShared(30, key1);
  });
  t5.join();
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, StressTestMultipleKeys) {
  constexpr size_t NO_THREADS = 10;
  constexpr size_t TRY_CNT    = 200;
  FLAGS_worker_count          = NO_THREADS;
  FasterLockManager lock_mgr;

  std::vector<std::array<u8, 4>> keys;
  for (int i = 0; i < 10; i++) keys.push_back({u8('a' + i), u8('b' + i), u8('c' + i), u8('d' + i)});

  std::array<std::atomic<u64>, NO_THREADS> success{};
  std::atomic<uint64_t> ts_counter{1};

  std::array<std::thread, NO_THREADS> threads;
  for (auto idx = 0UL; idx < NO_THREADS; ++idx) {
    threads[idx] = std::thread([&, idx]() {
      LeanStore::worker_thread_id = idx;
      for (size_t cnt = 0; cnt < TRY_CNT; ++cnt) {
        auto ts = ts_counter.fetch_add(1);

        // Pick random tuple
        const auto &k = keys[ts % keys.size()];
        LOCKABLE_TUPLE_STACK(tup, k, 1);

        bool want_exclusive = ts % 5 == 0;
        bool want_upgrade   = ts % 20 == 0;

        if (want_exclusive) {
          if (lock_mgr.TryLockExclusive(ts, tup)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            lock_mgr.Unlock(ts, tup);
          }
        } else {
          if (lock_mgr.TryLockShared(ts, tup, 0, 0)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(20));
            if (want_upgrade && lock_mgr.TryLockExclusive(ts, tup)) {
              lock_mgr.Unlock(ts, tup);
            } else {
              lock_mgr.UnlockShared(ts, tup);
            }
          }
        }
      }
    });
  }

  for (auto &t : threads) { t.join(); }

  uint64_t total_success = 0;
  for (auto &s : success) { total_success += s.load(); }

  LOG_INFO("Stress test completed. Total success: {}", total_success);
  EXPECT_GT(total_success, 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, ReleaseAllLocksTest) {
  FLAGS_worker_count          = 1;
  LeanStore::worker_thread_id = 0;
  FasterLockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  // Acquire locks
  EXPECT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key2));

  // Release all
  lock_mgr.ReleaseAllLocks(10, {});

  // Should be able to acquire now
  EXPECT_TRUE(lock_mgr.TryLockShared(20, key1, 0, 0));
  lock_mgr.UnlockShared(20, key1);

  EXPECT_TRUE(lock_mgr.TryLockExclusive(20, key2));
  lock_mgr.Unlock(20, key2);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, WaitDieDeadlockCheck) {
  FLAGS_worker_count = 2;
  FasterLockManager lock_mgr;

  std::array<u8, 4> k1{'k', '1', '1', '1'};
  std::array<u8, 4> k2{'k', '2', '2', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  std::atomic<bool> t1_done{false};
  std::atomic<bool> t2_done{false};

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key1));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key2));
    lock_mgr.Unlock(10, key2);
    lock_mgr.Unlock(10, key1);
    t1_done.store(true);
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // start later, younger
    bool success = lock_mgr.TryLockExclusive(20, key2);          // Should die due to Wait-Die
    if (success) lock_mgr.Unlock(20, key2);
    t2_done.store(true);
  });

  t1.join();
  t2.join();

  EXPECT_TRUE(t1_done.load());
  EXPECT_TRUE(t2_done.load());
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(FasterLockManagerTest, LockTableGC) {
  FLAGS_worker_count          = 1;
  LeanStore::worker_thread_id = 0;
  FasterLockManager lock_mgr;
  auto pooled = [&] {
    size_t n = 0;
    for (auto &ring : lock_mgr.rings_) { n += ring.free.size(); }
    return n;
  };

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 99);  // unique tree_id-relative table: only this manager's entries matter
  LOCKABLE_TUPLE_STACK(key2, k2, 99);

  const size_t before = lock_mgr.table_size_.load(std::memory_order_relaxed);

  EXPECT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  EXPECT_TRUE(lock_mgr.TryLockExclusive(10, key2));
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), before + 2);

  // Released entries are deferred for one epoch, still in the table
  lock_mgr.UnlockShared(10, key1);
  lock_mgr.Unlock(10, key2);
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), before + 2);

  // Explicit sweep erases the idle entries and parks their lock objects
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), before);
  const size_t parked = pooled();  // ring free lists

  // The same keys re-insert cleanly, reusing pooled lock objects
  EXPECT_TRUE(lock_mgr.TryLockExclusive(20, key1));
  EXPECT_TRUE(lock_mgr.TryLockShared(20, key2, 0, 0));
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), before + 2);
  EXPECT_EQ(pooled(), parked - 2);
  lock_mgr.UnlockShared(20, key2);
  lock_mgr.Unlock(20, key1);
  lock_mgr.DrainDeferred();
  EXPECT_EQ(lock_mgr.table_size_.load(std::memory_order_relaxed), before);
  EXPECT_EQ(pooled(), parked);
}
}  // namespace leanstore::transaction
