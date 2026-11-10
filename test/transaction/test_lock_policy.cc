#include "common/typedefs.h"
#include "leanstore/config.h"
#include "transaction/deadlock_prevention.h"
#include "transaction/lock_manager/inline_lock_manager.h"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <thread>

using namespace leanstore::transaction;

class NoWaitLockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_                   = FLAGS_txn_locking_policy;
    FLAGS_txn_locking_policy = 0;
  }

  void TearDown() override { FLAGS_txn_locking_policy = saved_; }

  // A SMALLER timestamp is the OLDER transaction. Under wait-die the older one waits; no-wait
  // ignores age, so these tests use "older"/"younger" only to show that age no longer matters.
  static constexpr uint64_t OLDER   = 100;
  static constexpr uint64_t YOUNGER = 200;

  uint32_t saved_{1};
};

TEST_F(NoWaitLockTest, SharedLocksStillCoexist) {
  DeadlockPrevention lock;
  EXPECT_TRUE(lock.TryLockShared(OLDER));
  EXPECT_TRUE(lock.TryLockShared(YOUNGER));
  lock.UnlockShared(OLDER);
  lock.UnlockShared(YOUNGER);
  EXPECT_TRUE(lock.TryLockExclusive(YOUNGER));
  lock.Unlock(YOUNGER);
}

TEST_F(NoWaitLockTest, ExclusiveIsRefusedByAnySharedHolderRegardlessOfAge) {
  DeadlockPrevention lock;
  // A shared holder YOUNGER than the requester. Wait-die would hand the lock over; no-wait must not.
  EXPECT_TRUE(lock.TryLockShared(YOUNGER));
  EXPECT_FALSE(lock.TryLockExclusive(OLDER));
  EXPECT_FALSE(lock.TryLockExclusive(YOUNGER));
}

TEST_F(NoWaitLockTest, SharedIsRefusedByAnExclusiveHolderRegardlessOfAge) {
  DeadlockPrevention lock;
  EXPECT_TRUE(lock.TryLockExclusive(YOUNGER));
  // Both an older and a younger shared request must die.
  EXPECT_FALSE(lock.TryLockShared(OLDER));
  EXPECT_FALSE(lock.TryLockShared(YOUNGER));
  lock.Unlock(YOUNGER);
  EXPECT_TRUE(lock.TryLockShared(OLDER));
}

TEST_F(NoWaitLockTest, UpgradeNeedsToBeTheSoleSharedHolder) {
  DeadlockPrevention lock;
  EXPECT_TRUE(lock.TryLockShared(OLDER));
  EXPECT_TRUE(lock.TryLockShared(YOUNGER));
  EXPECT_FALSE(lock.TryLockUpgrade(OLDER));
  lock.UnlockShared(YOUNGER);
  // Now we are alone, so the upgrade lands -- even though a younger txn held it a moment ago.
  EXPECT_TRUE(lock.TryLockUpgrade(OLDER));
  lock.Unlock(OLDER);
}

TEST_F(NoWaitLockTest, NothingEverQueuesSoTheLockTableStaysReusable) {
  DeadlockPrevention lock;
  EXPECT_TRUE(lock.TryLockShared(OLDER));
  EXPECT_FALSE(lock.TryLockExclusive(YOUNGER));
  // The refused requester must not have left state behind: once the holder leaves, the tuple is
  // immediately available to whoever asks next.
  lock.UnlockShared(OLDER);
  EXPECT_TRUE(lock.TryLockExclusive(YOUNGER));
  lock.Unlock(YOUNGER);
  EXPECT_TRUE(lock.IsFree());
}

TEST_F(NoWaitLockTest, WaitDieBlocksWhereNoWaitRefuses) {
  // The one case where the two protocols differ in a way a test can see from outside: an older
  // requester meeting a younger holder. Wait-die makes it wait and hand over on release; no-wait
  // refuses it straight away. Both are correct, they just trade aborts against blocking.
  for (auto policy : {1, 0}) {
    FLAGS_txn_locking_policy = policy;
    DeadlockPrevention lock;
    EXPECT_TRUE(lock.TryLockShared(YOUNGER));

    std::atomic<bool> acquired{false};
    std::thread requester([&] { acquired.store(lock.TryLockExclusive(OLDER)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (policy == 1) {
      EXPECT_FALSE(acquired.load()) << "wait-die should still be blocked on the shared holder";
      lock.UnlockShared(YOUNGER);
      requester.join();
      EXPECT_TRUE(acquired.load());
      lock.Unlock(OLDER);
    } else {
      requester.join();  // returns without the holder ever releasing
      EXPECT_FALSE(acquired.load());
      lock.UnlockShared(YOUNGER);
    }
  }
}
