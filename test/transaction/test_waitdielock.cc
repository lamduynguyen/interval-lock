#include "common/typedefs.h"
#include "transaction/deadlock_prevention.h"

#include "gtest/gtest.h"

#include <barrier>
#include <memory>
#include <thread>

using namespace leanstore::transaction;

static constexpr i32 NO_THREADS = 200;

TEST(DeadlockPreventionTest, SharedLockBasic) {
  DeadlockPrevention lock;

  // Lock with timestamp 10
  EXPECT_TRUE(lock.TryLockShared(10));

  // Lock with timestamp 5 should succeed (smaller ts goes to waiter)
  std::thread t1([&lock]() { EXPECT_TRUE(lock.TryLockShared(5)); });
  t1.join();

  // Unlock first lock
  lock.UnlockShared(10);
}

TEST(DeadlockPreventionTest, ExclusiveLockBasic) {
  DeadlockPrevention lock;

  // Exclusive lock with timestamp 10
  EXPECT_TRUE(lock.TryLockExclusive(10));

  // Lock with smaller timestamp 5 should go to waiter
  std::thread t1([&lock]() { EXPECT_TRUE(lock.TryLockExclusive(5)); });

  // Lock with bigger timestamp 15 should fail immediately
  EXPECT_FALSE(lock.TryLockExclusive(15));

  // Unlock first lock to promote waiter
  lock.Unlock(10);
  t1.join();
}

TEST(DeadlockPreventionTest, SharedUpgrade) {
  DeadlockPrevention lock;

  // Two shared owners
  EXPECT_TRUE(lock.TryLockShared(10));
  EXPECT_TRUE(lock.TryLockShared(15));

  // Upgrade smaller ts
  std::thread t([&]() {
    auto success = lock.TryLockUpgrade(10);  // will block until TS 15 releases
    // now TS 10 should hold exclusive lock
    ASSERT_TRUE(success);  // Should not block
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  lock.UnlockShared(15);  // unblock TS 10
  t.join();

  // now TS 10 holds exclusive lock
  lock.Unlock(10);
}

TEST(DeadlockPreventionTest, WaiterPromotionOrder) {
  DeadlockPrevention lock;

  // Exclusive lock first
  EXPECT_TRUE(lock.TryLockExclusive(20));

  // Barrier for threads to synchronize before trying lock
  std::barrier sync(3);
  std::atomic<bool> t1_acquired{false};
  std::atomic<bool> t2_acquired{false};

  // Thread 1: exclusive (TS 10). Dieable: if t2's OLDER shared claim lands first, wait-die
  // kills this younger X request -- the thread must not unlock a lock it was never granted.
  std::thread t1([&] {
    sync.arrive_and_wait();  // synchronize start
    if (lock.TryLockExclusive(10)) {
      t1_acquired.store(true);
      lock.Unlock(10);
    }
  });

  // Thread 2: oldest shared (TS 5); spins internally, eventually always granted.
  std::thread t2([&] {
    sync.arrive_and_wait();  // synchronize start
    if (lock.TryLockShared(5)) {
      t2_acquired.store(true);
      lock.UnlockShared(5);
    }
  });

  sync.arrive_and_wait();  // release both threads simultaneously

  // Release the main exclusive lock to promote waiters
  lock.Unlock(20);
  t1.join();
  t2.join();

  // The oldest waiter (ts=5) always acquires eventually; t1's X may legitimately die by
  // wait-die when t2's older shared claim landed first.
  EXPECT_TRUE(t2_acquired.load());
}

TEST(DeadlockPreventionTest, WaitDieAcquisitionFailure) {
  DeadlockPrevention lock;

  // deterministic timestamps: smaller = older
  uint64_t ts_old   = 10;  // oldest
  uint64_t ts_young = 50;  // younger transaction

  // Old transaction acquires exclusive lock first
  ASSERT_TRUE(lock.TryLockExclusive(ts_old));

  // Younger transaction tries to acquire exclusive lock (should fail due to wait-die)
  bool acquired_young = lock.TryLockExclusive(ts_young);

  // Wait-die dictates younger aborts instead of waiting
  EXPECT_FALSE(acquired_young);

  // Unlock old transaction
  lock.Unlock(ts_old);

  // After unlock, younger transaction should now succeed
  ASSERT_TRUE(lock.TryLockExclusive(ts_young));
  lock.Unlock(ts_young);
}

TEST(DeadlockPreventionTest, Deterministic4ThreadsWaitDie) {
  DeadlockPrevention lock;

  // Smaller timestamp = older transaction
  uint64_t ts[4] = {40, 30, 20, 10};  // t0=youngest, t3=oldest

  std::atomic<bool> acquired[4]{false, false, false, false};

  // Step counter to enforce strict order
  std::atomic<int> step{0};

  // Thread 0: youngest shared lock
  std::thread t0([&] {
    while (step.load() != 0) { std::this_thread::yield(); }
    EXPECT_TRUE(lock.TryLockShared(ts[0]));  // should succeed
    acquired[0].store(true, std::memory_order_release);
    step.fetch_add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    lock.UnlockShared(ts[0]);
  });

  // Thread 1: younger exclusive lock (may abort due to Wait-Die)
  std::thread t1([&] {
    while (step.load() != 1) { std::this_thread::yield(); }
    acquired[1].store(lock.TryLockExclusive(ts[1]), std::memory_order_release);
    step.fetch_add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (acquired[1].load()) { lock.Unlock(ts[1]); }
  });

  // Thread 2: shared lock younger than t0 (may succeed)
  std::thread t2([&] {
    while (step.load() != 2) { std::this_thread::yield(); }
    acquired[2].store(lock.TryLockShared(ts[2]), std::memory_order_release);
    step.fetch_add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (acquired[2].load()) { lock.UnlockShared(ts[2]); }
  });

  // Thread 3: oldest exclusive lock (should succeed)
  std::thread t3([&] {
    while (step.load() != 3) { std::this_thread::yield(); }
    acquired[3].store(lock.TryLockExclusive(ts[3]), std::memory_order_release);
    step.fetch_add(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (acquired[3].load()) { lock.Unlock(ts[3]); }
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  // Check results
  EXPECT_TRUE(acquired[0].load());  // t0 succeeds
  EXPECT_TRUE(acquired[1].load());  // t1 succeeds
  EXPECT_TRUE(acquired[2].load());  // t2 succeeds
  EXPECT_TRUE(acquired[3].load());  // oldest t3 always succeeds

  // Lock should be free at the end
  EXPECT_TRUE(lock.TryLockShared(50));
  lock.UnlockShared(50);
}

TEST(DeadlockPreventionTest, StressTest) {
  DeadlockPrevention lock;
  constexpr auto try_cnt = 1000UL;

  std::array<std::thread, NO_THREADS> threads;
  std::array<std::atomic<u64>, NO_THREADS> success;
  std::atomic<uint64_t> ts_counter{1};

  for (int idx = 0; idx < NO_THREADS; ++idx) {
    threads[idx] = std::thread([&, idx]() {
      for (auto cnt = 0UL; cnt < try_cnt; cnt++) {
        auto ts             = ts_counter.fetch_add(1);
        bool want_exclusive = ts % 3 == 0;
        bool want_upgrade   = ts % 10 == 0;

        if (want_exclusive) {
          if (lock.TryLockExclusive(ts)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            lock.Unlock(ts);
          }
          // else: younger txn died in Wait-Die
        } else {
          if (lock.TryLockShared(ts)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            if (want_upgrade && lock.TryLockUpgrade(ts)) {
              std::this_thread::sleep_for(std::chrono::microseconds(100));
              lock.Unlock(ts);
            } else {
              lock.UnlockShared(ts);
            }
          }
        }
      }
    });
  }

  for (auto &t : threads) { t.join(); }

  // Lock should be free
  EXPECT_EQ(ts_counter.load(), NO_THREADS * try_cnt + 1);
  EXPECT_TRUE(lock.TryLockShared(ts_counter.load()));
  lock.UnlockShared(ts_counter.load());
  auto total_success = 0UL;
  auto zero_threads  = 0UL;

  for (int i = 0; i < NO_THREADS; ++i) {
    uint64_t s = success[i].load(std::memory_order_relaxed);
    total_success += s;
    if (s == 0) { zero_threads++; }
  }
  LOG_INFO("2PL Stress Test: Timestamp {}; # success {}; # zero threads {}", ts_counter.load(), total_success,
           zero_threads);
}

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
