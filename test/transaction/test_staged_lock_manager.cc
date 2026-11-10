#include "common/typedefs.h"
#include "leanstore/leanstore.h"
#include "transaction/lock_manager/staged_lock_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

using leanstore::LeanStore;
using namespace leanstore::transaction;

// The staged manager aborts the caller on detected deadlock; retry since the
// conflict usually clears right after the other side aborts/releases
static auto RetryUntilTrue(const std::function<bool()> &fn, int attempts = 1000) -> bool {
  for (int i = 0; i < attempts; i++) {
    if (fn()) { return true; }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

namespace leanstore::transaction {
TEST(StagedLockManagerTest, SharedCoexist) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
    lock_mgr.UnlockShared(10, key1);
  });
  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    EXPECT_TRUE(lock_mgr.TryLockShared(20, key1, 0, 0));
    lock_mgr.UnlockShared(20, key1);
  });
  t0.join();
  t1.join();
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, BlockingAndWakeup) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<bool> t0_x_done{false};

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    ASSERT_TRUE(lock_mgr.TryLockExclusive(10, key1));
    t0_x_done.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock_mgr.Unlock(10, key1);  // must wake the sleeping waiter
  });

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    while (!t0_x_done.load(std::memory_order_acquire)) { std::this_thread::yield(); }
    // Blocks (polls) until t0's unlock frees the key
    EXPECT_TRUE(lock_mgr.TryLockExclusive(20, key1));
    lock_mgr.Unlock(20, key1);
  });

  t0.join();
  t1.join();
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, FifoAmongExclusiveWaiters) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<bool> holder_done{false};
  std::atomic<int> grant_seq{0};
  std::atomic<int> first_granter{-1};
  std::atomic<int> second_granter{-1};

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    ASSERT_TRUE(lock_mgr.TryLockExclusive(10, key1));
    holder_done.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock_mgr.Unlock(10, key1);
  });

  // First waiter inserts its node before the second one
  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    while (!holder_done.load(std::memory_order_acquire)) { std::this_thread::yield(); }
    ASSERT_TRUE(lock_mgr.TryLockExclusive(20, key1));  // blocks
    int turn = grant_seq.fetch_add(1) + 1;
    if (turn == 1) {
      first_granter.store(1);
    } else {
      second_granter.store(1);
    }
    lock_mgr.Unlock(20, key1);  // wakes t2 if it re-slept behind us
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 2;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lock_mgr.TryLockExclusive(30, key1));  // blocks behind t1
    int turn = grant_seq.fetch_add(1) + 1;
    if (turn == 1) {
      first_granter.store(2);
    } else {
      second_granter.store(2);
    }
    lock_mgr.Unlock(30, key1);
  });

  t0.join();
  t1.join();
  t2.join();
  EXPECT_EQ(first_granter.load(), 1);  // earlier waiter granted first
  EXPECT_EQ(second_granter.load(), 2);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, DeadlockDetectionAbortsSelf) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  std::barrier sync_point(2);
  std::atomic<int> aborts{0};

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    ASSERT_TRUE(lock_mgr.TryLockExclusive(10, key1));
    sync_point.arrive_and_wait();
    // Waits on key2 (held by t1) -> cycle -> abort-self, unless t1 aborted
    // first and its release granted us the lock
    if (!lock_mgr.TryLockExclusive(10, key2)) {
      aborts.fetch_add(1);
    } else {
      lock_mgr.Unlock(10, key2);
    }
    lock_mgr.Unlock(10, key1);
  });

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    ASSERT_TRUE(lock_mgr.TryLockExclusive(20, key2));
    sync_point.arrive_and_wait();
    if (!lock_mgr.TryLockExclusive(20, key1)) {
      aborts.fetch_add(1);
    } else {
      lock_mgr.Unlock(20, key1);
    }
    lock_mgr.Unlock(20, key2);
  });

  t0.join();
  t1.join();
  // The paper's policy (3): the detecting transaction aborts itself; the
  // check-then-sleep protocol guarantees at least one side detects the cycle
  EXPECT_GE(aborts.load(), 1);
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, UpgradePath) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  LeanStore::worker_thread_id = 0;
  ASSERT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  ASSERT_TRUE(lock_mgr.TryLockExclusive(10, key1));
  // S node retired on successful upgrade; only the X node stays live
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 1);
  lock_mgr.Unlock(10, key1);
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
  EXPECT_TRUE(lock_mgr.EmptyLocalSet());
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, UpgradeBehindPendingXIsDeadlock) {
  // Reproduces the TPC-C freeze: t10 holds S on key1 and t20 has a pending X
  // request behind it. When t10 then upgrades to X, its only blocker is t20's
  // *waiter* node (FIFO priority), not a granted holder — so a detector whose
  // edges only point at granted locks sees an empty graph and both sleep
  // forever. The detector must follow waiter edges too.
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  LeanStore::worker_thread_id = 0;
  ASSERT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));

  std::atomic<bool> t20_got{false};
  std::thread t20([&]() {
    LeanStore::worker_thread_id = 1;
    if (lock_mgr.TryLockExclusive(20, key1)) {
      t20_got.store(true);
      lock_mgr.Unlock(20, key1);
    }
  });

  // Let t20's X request insert and start waiting behind our S lock.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(t20_got.load());

  // Our upgrade closes the cycle; we must be told to abort instead of hanging.
  EXPECT_FALSE(lock_mgr.TryLockExclusive(10, key1));
  lock_mgr.UnlockShared(10, key1);
  t20.join();
  EXPECT_TRUE(t20_got.load());
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, LockTableGC) {
  StagedLockManager lock_mgr;
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  LeanStore::worker_thread_id = 0;
  ASSERT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  ASSERT_TRUE(lock_mgr.TryLockExclusive(10, key2));
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 2);

  lock_mgr.UnlockShared(10, key1);
  lock_mgr.Unlock(10, key2);
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);  // retired, still chained (OBSOLETE)

  // Re-acquire on a fresh node works even before GC ran
  ASSERT_TRUE(lock_mgr.TryLockShared(10, key1, 0, 0));
  lock_mgr.UnlockShared(10, key1);

  EXPECT_TRUE(lock_mgr.EmptyLocalSet());
}
}  // namespace leanstore::transaction

namespace leanstore::transaction {
TEST(StagedLockManagerTest, StressTestMultipleKeys) {
  StagedLockManager lock_mgr;
  constexpr int kThreads    = 8;
  constexpr int kIterations = 100;
  std::atomic<u64> total_success{0};

  // All threads hammer the same two keys
  std::array<u8, 8> k0_val{'k', 'e', 'y', '0', '0', '0', '0', '0'};
  std::array<u8, 8> k1_val{'k', 'e', 'y', '1', '1', '1', '1', '1'};

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; t++) {
    threads.emplace_back([&, t]() {
      LeanStore::worker_thread_id = t;
      u64 ts                      = (t + 1) * 1000;
      LOCKABLE_TUPLE_STACK(key0, k0_val, 1);
      LOCKABLE_TUPLE_STACK(key1, k1_val, 1);
      for (int i = 0; i < kIterations; i++) {
        bool ok;
        if (i % 2 == 0) {
          ok = RetryUntilTrue([&] { return lock_mgr.TryLockShared(ts, key0, 0, 0); }) &&
               RetryUntilTrue([&] { return lock_mgr.TryLockShared(ts, key1, 0, 0); });
          if (ok) {
            lock_mgr.UnlockShared(ts, key0);
            lock_mgr.UnlockShared(ts, key1);
          }
        } else {
          ok = RetryUntilTrue([&] { return lock_mgr.TryLockExclusive(ts, key0); });
          if (ok) { lock_mgr.Unlock(ts, key0); }
        }
        if (ok) { total_success.fetch_add(1); }
        ts++;
      }
    });
  }
  for (auto &thread : threads) { thread.join(); }
  EXPECT_GT(total_success.load(), 0);
  EXPECT_EQ(lock_mgr.live_nodes_.load(), 0);
}
}  // namespace leanstore::transaction

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
