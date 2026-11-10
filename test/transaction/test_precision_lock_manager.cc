#include "test/base_test.h"

#include "buffer/buffer_manager.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "sync/page_guard/exclusive_guard.h"
#include "transaction/lock_manager/precision_lock_manager.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>

namespace leanstore::transaction {

using transaction::LockableTuple;
using transaction::PrecisionLockManager;

namespace {
// 4 big-endian bytes, so memcmp order -- what RangeCmp uses -- matches integer order. The storage
// engine's own fold byte-swaps and does not preserve that, so do not copy it here: these tests only
// need "10 sorts before 30".
auto Key(int v) -> std::array<u8, 4> {
  auto raw = static_cast<u32>(v);
  return {static_cast<u8>(raw >> 24), static_cast<u8>(raw >> 16), static_cast<u8>(raw >> 8), static_cast<u8>(raw)};
}

auto AsSpan(const std::array<u8, 4> &k) -> std::span<const u8> { return std::span<const u8>(k.data(), k.size()); }
}  // namespace

class TestPrecisionLockManager : public BaseTest {
 protected:
  std::unique_ptr<PrecisionLockManager> lock_mgr_;

  static constexpr pageid_t PAGE_A = 1;
  static constexpr pageid_t PAGE_B = 2;
  static constexpr leng_t REL_1    = 0;

  void SetUp() override {
    // Production runs one txn per worker, so each simulated txn gets its own worker state.
    FLAGS_worker_count          = 8;
    LeanStore::worker_thread_id = 0;
    BaseTest::SetupTestFile();
    // The checkpointer is built with WAL on; the page guards only need the runtime checks off.
    saved_wal_enable_ = FLAGS_wal_enable;
    FLAGS_wal_enable  = false;
    // The mini tree is a two-leaf chain rooted at the PAGE_A leaf: descend lands on A, then the
    // chain runs to B unless B's lower fence (500) is already past `hi`.
    // A test may replace the walk through SetTraverse, which the manager sees through this hook.
    lock_mgr_ = std::make_unique<PrecisionLockManager>(
      buffer_.get(), [this](leng_t tree_id, std::span<const u8> lo, std::span<const u8> hi,
                            const std::function<void(pageid_t)> &visit) {
        if (traverse_hook_) {
          traverse_hook_(tree_id, lo, hi, visit);
          return;
        }
        visit(PAGE_A);
        if (hi.empty() || RangeEntry::Cmp(hi, AsSpan(Key(500))) >= 0) { visit(PAGE_B); }
      });
    SetupMiniTree();
  }

  void TearDown() override {
    // Clear every worker state: each simulated txn owned its own block.
    for (u32 w = 0; w < FLAGS_worker_count; ++w) {
      LeanStore::worker_thread_id = static_cast<wid_t>(w);
      lock_mgr_->ReleaseAllLocks(0, {});
    }
    txn_worker_.clear();
    lock_mgr_.reset();
    FLAGS_wal_enable = saved_wal_enable_;
  }

  bool saved_wal_enable_{true};
  /// Replaces the mini-tree walk when a test needs to mutate pages during a release walk.
  TraverseFunc traverse_hook_;

  /// The split/merge hooks borrow exclusively latched page guards; the fixture's pages are
  /// planted by hand, so lock their page states directly and wrap them.
  auto LockPage(pageid_t pid) -> sync::ExclusiveGuard<storage::BTreeNode> {
    buffer_->GetPageState(pid).LockExclusive();
    return sync::ExclusiveGuard<storage::BTreeNode>(buffer_.get(), buffer_->ToPtr(pid));
  }

  /// Distinct simulated transactions get distinct worker states, never sharing a LocalState.
  std::unordered_map<timestamp_t, wid_t> txn_worker_;

  auto WorkerOf(timestamp_t txn_ts) -> wid_t {
    auto [it, inserted] = txn_worker_.try_emplace(txn_ts, static_cast<wid_t>(txn_worker_.size()));
    assert(it->second < FLAGS_worker_count);
    return it->second;
  }

  /// The current thread impersonates the txn's worker for the duration of one manager call.
  void EnterTxn(timestamp_t txn_ts) { LeanStore::worker_thread_id = WorkerOf(txn_ts); }

  void Release(timestamp_t txn_ts, const WriteSetCallback &write_set_cb = {}) {
    EnterTxn(txn_ts);
    lock_mgr_->ReleaseAllLocks(txn_ts, write_set_cb);
  }

  /// The release walk reads real BTreeNode headers off the claimed pages, so PAGE_A and PAGE_B
  /// are a real two-leaf chain (A: [0,500), B: [500,1000)), and PAGE_A is the tree's root.
  void SetupMiniTree() {
    auto cmp   = ComparisonLambda{ComparisonOperator::MEMCMP, std::memcmp};
    auto k0    = Key(0);
    auto k500  = Key(500);
    auto k1000 = Key(1000);
    auto *a    = new (buffer_->ToPtr(PAGE_A)) storage::BTreeNode(true);
    auto *b    = new (buffer_->ToPtr(PAGE_B)) storage::BTreeNode(true);
    a->SetFences(std::span<u8>(k0.data(), k0.size()), std::span<u8>(k500.data(), k500.size()), cmp);
    b->SetFences(std::span<u8>(k500.data(), k500.size()), std::span<u8>(k1000.data(), k1000.size()), cmp);
    a->header.next_leaf_node = PAGE_B;
  }

  /// Locks `[lo, hi]` on `page`. `Lock` is the wide-range shorthand: every test above uses one range.
  auto LockRange(pageid_t page, int lo, int hi, timestamp_t ts, bool exclusive = false) -> bool {
    EnterTxn(ts);
    auto lo_key = Key(lo);
    auto hi_key = Key(hi);
    return lock_mgr_->LockRange(REL_1, page, ts, {AsSpan(lo_key)}, {AsSpan(hi_key)},
                                exclusive ? LockMode::EXCLUSIVE : LockMode::SHARED);
  }

  auto Lock(pageid_t page, timestamp_t ts, bool exclusive = false) -> bool {
    return LockRange(page, 0, 1000, ts, exclusive);
  }

  /// Range with independently inclusive/exclusive ends.
  auto LockSpan(pageid_t page, int lo, bool lo_inclusive, int hi, bool hi_inclusive, timestamp_t ts,
                bool exclusive = false) -> bool {
    EnterTxn(ts);
    auto lo_key = Key(lo);
    auto hi_key = Key(hi);
    return lock_mgr_->LockRange(REL_1, page, ts, {AsSpan(lo_key), lo_inclusive}, {AsSpan(hi_key), hi_inclusive},
                                exclusive ? LockMode::EXCLUSIVE : LockMode::SHARED);
  }

  void SetTraverse(TraverseFunc fn) { traverse_hook_ = std::move(fn); }

  /// Every live entry sits in exactly one worker's ownership list, so the sum is the number of
  /// entries currently held (and later recycled) by the manager.
  auto LiveEntryCount() const -> size_t {
    size_t sum = 0;
    for (const auto &state : lock_mgr_->local_states_) { sum += state.entries_.size(); }
    return sum;
  }
};

TEST_F(TestPrecisionLockManager, SharedEntriesCoexistExclusiveDies) {
  ASSERT_TRUE(Lock(PAGE_A, 100, false));
  ASSERT_TRUE(Lock(PAGE_A, 200, false));  // two readers of one page
  EXPECT_EQ(LiveEntryCount(), 2);
  // any exclusive request conflicts with a shared holder regardless of age
  EXPECT_FALSE(Lock(PAGE_A, 300, true));
  EXPECT_EQ(LiveEntryCount(), 2);  // and it left nothing behind on the way out
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 1);  // txn 200 still holds its shared lock
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, DifferentPagesDoNotConflict) {
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  EXPECT_TRUE(Lock(PAGE_B, 200, true));
  EXPECT_EQ(LiveEntryCount(), 2);
  Release(100, {});
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, OlderHolderWinsYoungerDies) {
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  EXPECT_FALSE(Lock(PAGE_A, 200, true));
  Release(100, {});
  // once the older holder is gone the younger one may proceed
  ASSERT_TRUE(Lock(PAGE_A, 200, true));
  EXPECT_EQ(LiveEntryCount(), 1);
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, AbortRetryRejectsTheStaleTlbSlot) {
  // A wait-die death re-runs the txn with its original start_ts, after ReleaseAllLocks recycled
  // its entries. The released entry left a TLB slot behind; its stale grant must not authorize
  // the retry's claim (which would then never be recorded on the page).
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  Release(100, {});
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  EXPECT_EQ(LiveEntryCount(), 1);
  EXPECT_FALSE(Lock(PAGE_A, 200, true));  // the retry's claim is really on the page
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, OlderWaiterWaitsForYoungerHolder) {
  // wait-die: the older transaction waits, the younger one would have died
  ASSERT_TRUE(Lock(PAGE_A, 200, true));

  std::atomic<bool> acquired{false};
  std::thread waiter([&] {
    LeanStore::worker_thread_id = 1;
    acquired.store(Lock(PAGE_A, 100, true));
    // the per-thread entry set belongs to the thread that made it, so release here
    Release(100, {});
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(acquired.load());
  EXPECT_EQ(LiveEntryCount(), 2);  // the holder's entry plus the waiter's queued one

  Release(200, {});
  waiter.join();
  EXPECT_TRUE(acquired.load());
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, YoungerCannotOvertakeAnOlderWaiter) {
  ASSERT_TRUE(Lock(PAGE_A, 200, true));

  std::atomic<bool> older_done{false};
  std::thread older([&] {
    LeanStore::worker_thread_id = 1;
    older_done.store(Lock(PAGE_A, 100, true));
    Release(100, {});
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // ts=150 is younger than the queued ts=100 but older than the holder. It must die, not slip in:
  // that is the reason a waiting entry counts as a conflict at all.
  EXPECT_FALSE(Lock(PAGE_A, 150, true));

  Release(200, {});  // promotes the older waiter
  older.join();
  EXPECT_TRUE(older_done.load());
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, ReadThenWriteUpgradesInPlace) {
  // One entry per transaction per page: a transaction that reads a page and then writes it must not
  // end up holding two, and the upgrade has to be visible to everybody else.
  //
  // Timestamps here run backwards from the usual: a SMALLER ts is the older transaction, because
  // wait-die grants the page to the oldest and kills the newest.
  ASSERT_TRUE(Lock(PAGE_A, 100, false));
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  EXPECT_EQ(LiveEntryCount(), 1);

  // It is exclusive now, so a younger reader dies on what used to be a shared lock.
  EXPECT_FALSE(Lock(PAGE_A, 300, false));
  EXPECT_EQ(LiveEntryCount(), 1);  // and the failed request left nothing behind

  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
  EXPECT_TRUE(Lock(PAGE_A, 300, false));
  Release(300, {});
}

TEST_F(TestPrecisionLockManager, UpgradedEntryStillMakesAnOlderRequesterWait) {
  // The upgrade has to change what an older transaction does too, not just what a younger one does:
  // ts=50 is the older transaction here, so it must wait rather than walk in.
  ASSERT_TRUE(Lock(PAGE_A, 300, false));
  ASSERT_TRUE(Lock(PAGE_A, 300, true));

  std::atomic<bool> acquired{true};
  std::thread older([&] {
    LeanStore::worker_thread_id = 1;
    acquired.store(Lock(PAGE_A, 50, true));
    Release(50, {});
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_TRUE(acquired.load());
  EXPECT_EQ(LiveEntryCount(), 2);

  Release(300, {});
  older.join();
  EXPECT_TRUE(acquired.load());
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, UndoLogIsIndependentOfPageLocks) {
  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key, k1, REL_1);
  ASSERT_TRUE(lock_mgr_->TryLockExclusive(100, key));
  std::array<u8, 3> undo{7, 8, 9};
  lock_mgr_->SetUndoPayload(100, key, std::span<const u8>(undo.data(), undo.size()));
  EXPECT_FALSE(lock_mgr_->EmptyLocalSet());

  auto seen = false;
  Release(100, [&](const LockableTuple *, std::span<const u8> payload) {
    seen = true;
    EXPECT_EQ(payload.size(), 3);
    EXPECT_EQ(payload[0], 7);
  });
  EXPECT_TRUE(seen);
  EXPECT_TRUE(lock_mgr_->EmptyLocalSet());
}

TEST_F(TestPrecisionLockManager, EmptyLocalSetHoldsUntilRelease) {
  ASSERT_TRUE(lock_mgr_->EmptyLocalSet());
  ASSERT_TRUE(Lock(PAGE_A, 100));
  EXPECT_FALSE(lock_mgr_->EmptyLocalSet());
  Release(100, {});
  EXPECT_TRUE(lock_mgr_->EmptyLocalSet());
}

TEST_F(TestPrecisionLockManager, ManyPagesReleaseCleanly) {
  for (pageid_t page = 1; page <= 32; page++) { ASSERT_TRUE(Lock(page, 100)); }
  // One logical interval lock, referenced by all 32 pages.
  EXPECT_EQ(LiveEntryCount(), 1);
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
  EXPECT_TRUE(lock_mgr_->EmptyLocalSet());
}

TEST_F(TestPrecisionLockManager, RevisitingAPageDoesNotDuplicateTheEntry) {
  // A scan re-descends onto the same leaf every time it restarts, so locking has to be idempotent.
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  for (int revisit = 0; revisit < 5; revisit++) { ASSERT_TRUE(Lock(PAGE_A, 100, true)); }
  EXPECT_EQ(LiveEntryCount(), 1);
}

TEST_F(TestPrecisionLockManager, SplitFollowsOnlyTheRangesReachingPastTheSeparator) {
  // The separator stays on the left node, so a range reaches the right half only if its upper bound
  // sorts past it. Two disjoint ranges straddling it: [600,1000] crosses and must follow, [0,100] is
  // entirely below and must not.
  ASSERT_TRUE(LockRange(PAGE_A, 0, 100, 100, false));
  ASSERT_TRUE(LockRange(PAGE_A, 600, 1000, 200, false));
  ASSERT_EQ(LiveEntryCount(), 2);

  auto sep         = Key(500);
  auto left_guard  = LockPage(PAGE_A);
  auto right_guard = LockPage(PAGE_B);
  lock_mgr_->OnPageSplit(left_guard, right_guard, AsSpan(sep));
  EXPECT_EQ(LiveEntryCount(), 2);  // the same entry now blocks on the right page - no copy
  // [0,100] stays left only; [600,1000] no longer touches the left's keys, so its ref moved.
  EXPECT_EQ(buffer_->BufferFramePage(PAGE_A).range_refs.size(), 1);
  EXPECT_EQ(buffer_->BufferFramePage(PAGE_B).range_refs.size(), 1);

  // Both halves still enforce their ranges: a younger writer dies on either side.
  EXPECT_FALSE(LockRange(PAGE_A, 50, 80, 500, true));
  EXPECT_FALSE(LockRange(PAGE_B, 700, 800, 500, true));
  EXPECT_EQ(LiveEntryCount(), 2);

  // Release unlinks the moved reference on a page this thread only learned about from the split.
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 1);
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_B).range_refs.empty());
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_A).range_refs.empty());
  EXPECT_TRUE(lock_mgr_->EmptyLocalSet());
}

TEST_F(TestPrecisionLockManager, SplitInFlightRestartsTheRecording) {
  // A claim is recorded against the page state its optimistic view was read under. If the page
  // is being written (a split's copy pass may already have moved keys away) or has changed
  // since, the record is refused with a restart, so the caller re-finds the page and claims it
  // there instead of leaving an entry that misses the keys the split moved.
  ASSERT_TRUE(Lock(PAGE_A, 100, false));  // txn 100 owns [Key(0), Key(1000))
  auto &ps           = buffer_->GetPageState(PAGE_A);
  const u64 snapshot = ps.StateAndVersion().load();
  EnterTxn(200);  // the direct claim below belongs to txn 200

  // In-flight split: the page holds the exclusive latch (the version is not bumped until the
  // splitter releases it) and its copy pass has already run.
  auto left_guard  = LockPage(PAGE_A);
  auto right_guard = LockPage(PAGE_B);
  lock_mgr_->OnPageSplit(left_guard, right_guard, AsSpan(Key(500)));
  EXPECT_THROW(
    lock_mgr_->LockRange(REL_1, PAGE_A, 200, {AsSpan(Key(600))}, {AsSpan(Key(700))}, LockMode::SHARED, snapshot),
    sync::RestartException);
  EXPECT_EQ(LiveEntryCount(), 1);  // refused before anything was recorded
  left_guard.Unlock();

  // Completed split: the bumped version catches it with no latch held.
  EXPECT_THROW(
    lock_mgr_->LockRange(REL_1, PAGE_A, 200, {AsSpan(Key(600))}, {AsSpan(Key(700))}, LockMode::SHARED, snapshot),
    sync::RestartException);
  EXPECT_EQ(LiveEntryCount(), 1);

  // A fresh snapshot records normally.
  ASSERT_TRUE(lock_mgr_->LockRange(REL_1, PAGE_A, 200, {AsSpan(Key(600))}, {AsSpan(Key(700))}, LockMode::SHARED,
                                   ps.StateAndVersion().load()));
  EXPECT_EQ(LiveEntryCount(), 2);

  Release(200, {});
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, DisjointRangesOnTheSamePageDoNotConflict) {
  // The reason the lock extent lives in the entry rather than being the whole page: TPC-C's NewOrder
  // and Payment both update warehouse.w_ytd and district.d_ytd, and page-wide exclusive locks there
  // turn a 99.99% abort rate. Two writers on the same page must be able to pass if their keys differ.
  ASSERT_TRUE(LockRange(PAGE_A, 10, 20, 100, true));
  ASSERT_TRUE(LockRange(PAGE_A, 30, 40, 200, true));
  EXPECT_EQ(LiveEntryCount(), 2);
  // ... and a reader of a third, disjoint range is unaffected as well.
  ASSERT_TRUE(LockRange(PAGE_A, 50, 60, 300, false));
  EXPECT_EQ(LiveEntryCount(), 3);
  // Overlapping is not disjoint, so this one dies.
  EXPECT_FALSE(LockRange(PAGE_A, 15, 35, 400, true));
  EXPECT_EQ(LiveEntryCount(), 3);
  Release(100, {});
  Release(200, {});
  Release(300, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, AWiderRangeIsNotAbsorbedByTheNarrowOneWeHold) {
  // Covering is one-directional: holding [10,20] must not let a later write of [10,200] skip the
  // conflict check against the other holders, so it becomes a second entry rather than an upgrade.
  ASSERT_TRUE(LockRange(PAGE_A, 10, 20, 100, false));
  ASSERT_TRUE(LockRange(PAGE_A, 10, 200, 100, true));
  EXPECT_EQ(LiveEntryCount(), 2);
  // The narrow entry is now exclusive too, so a younger reader of it dies.
  EXPECT_FALSE(LockRange(PAGE_A, 12, 13, 300, false));
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, ExclusiveLowerBoundLetsTheLowerEndpointThrough) {
  // (100, 200]: the lower endpoint is out, the upper endpoint is in.
  ASSERT_TRUE(LockSpan(PAGE_A, 100, false, 200, true, 100, true));
  EXPECT_TRUE(LockSpan(PAGE_A, 100, true, 100, true, 200, true));   // point 100 does not touch it
  EXPECT_FALSE(LockSpan(PAGE_A, 200, true, 200, true, 300, true));  // point 200 does
  EXPECT_EQ(LiveEntryCount(), 2);
  Release(100, {});
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, ExclusiveUpperBoundKeepsAdjacentRangesDisjoint) {
  // [100, 200) and [200, 300): they share no key, so both are granted...
  ASSERT_TRUE(LockSpan(PAGE_A, 100, true, 200, false, 100, true));
  EXPECT_TRUE(LockSpan(PAGE_A, 200, true, 300, true, 200, true));
  EXPECT_EQ(LiveEntryCount(), 2);
  // ... while an inclusive upper bound does reach the next range's first key.
  EXPECT_FALSE(LockSpan(PAGE_A, 150, true, 200, true, 300, true));
  Release(100, {});
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, BoundsBeyondThePooledBudgetUseExactSizeAllocations) {
  // lo + hi exceed RangeEntry::MAX_ENTRY_SIZE, so the entry comes from the exact-size malloc
  // path and is freed (not recycled into the pool) on release.
  std::array<u8, 20> lo{};
  std::array<u8, 20> hi{};
  lo.fill(1);
  hi.fill(2);
  EnterTxn(100);
  ASSERT_TRUE(lock_mgr_->LockRange(REL_1, PAGE_A, 100, {std::span<const u8>(lo)}, {std::span<const u8>(hi)},
                                   LockMode::EXCLUSIVE));
  EXPECT_EQ(LiveEntryCount(), 1);
  Release(100, {});
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, MergeMovesEntriesAndKeepsThemBlocking) {
  ASSERT_TRUE(Lock(PAGE_A, 100, true));
  ASSERT_TRUE(Lock(PAGE_B, 100, true));
  ASSERT_EQ(LiveEntryCount(), 1);

  auto left_guard  = LockPage(PAGE_A);
  auto right_guard = LockPage(PAGE_B);
  lock_mgr_->OnPageMerge(left_guard, right_guard);
  EXPECT_EQ(LiveEntryCount(), 1);  // one logical interval lock
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_A).range_refs.empty());
  EXPECT_EQ(buffer_->BufferFramePage(PAGE_B).range_refs.size(), 1);

  // The merged page still blocks, and the left one no longer does: it is on no list at all.
  EXPECT_FALSE(LockRange(PAGE_B, 0, 1000, 200, true));
  EXPECT_TRUE(LockRange(PAGE_A, 0, 1000, 200, true));

  // A merge moves the entries, so the owner still finds them all through the surviving page.
  Release(100, {});
  Release(200, {});  // txn 200's claim landed on the emptied left page
  EXPECT_EQ(LiveEntryCount(), 0);
  EXPECT_TRUE(lock_mgr_->EmptyLocalSet());
}

TEST_F(TestPrecisionLockManager, ReleaseTombstonePreventsMergeAndSplitFromResurrectingEntry) {
  // t0: one entry spans A and B after a split at 500.
  ASSERT_TRUE(Lock(PAGE_A, 100));
  {
    auto left  = LockPage(PAGE_A);
    auto right = LockPage(PAGE_B);
    lock_mgr_->OnPageSplit(left, right, AsSpan(Key(500)));
  }
  ASSERT_EQ(buffer_->BufferFramePage(PAGE_A).range_refs.size(), 1);
  ASSERT_EQ(buffer_->BufferFramePage(PAGE_B).range_refs.size(), 1);

  int traversals = 0;
  SetTraverse([&](leng_t, std::span<const u8>, std::span<const u8>, const std::function<void(pageid_t)> &visit) {
    traversals++;
    // t1: release cleans A.
    visit(PAGE_A);
    {
      // t2: another transaction merges A and B.
      auto left  = LockPage(PAGE_A);
      auto right = LockPage(PAGE_B);
      lock_mgr_->OnPageMerge(left, right);
    }
    {
      // t3: the merged page splits at 30; a tombstoned entry must not be copied.
      auto left  = LockPage(PAGE_B);
      auto right = LockPage(PAGE_A);
      lock_mgr_->OnPageSplit(left, right, AsSpan(Key(30)));
    }
    // The stale cursor only sees the right half; no ghost may remain on B.
    visit(PAGE_A);
    EXPECT_TRUE(buffer_->BufferFramePage(PAGE_B).range_refs.empty());
  });

  Release(100, {});
  EXPECT_EQ(traversals, 1);
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_A).range_refs.empty());
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_B).range_refs.empty());
  EXPECT_EQ(LiveEntryCount(), 0);
}

TEST_F(TestPrecisionLockManager, EarlyScanStopReleasesSplitCopy) {
  // t0: a per-leaf scan lock [40, 1000] on A.
  ASSERT_TRUE(LockRange(PAGE_A, 40, 1000, 100, false));
  // t1: A splits at 50; the lock overlaps both halves.
  {
    auto left  = LockPage(PAGE_A);
    auto right = LockPage(PAGE_B);
    lock_mgr_->OnPageSplit(left, right, AsSpan(Key(50)));
  }
  EXPECT_EQ(buffer_->BufferFramePage(PAGE_A).range_refs.size(), 1);
  EXPECT_EQ(buffer_->BufferFramePage(PAGE_B).range_refs.size(), 1);
  // t2: LIMIT stops early; bounds are intentionally not narrowed, so release reaches both.
  Release(100, {});
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_A).range_refs.empty());
  EXPECT_TRUE(buffer_->BufferFramePage(PAGE_B).range_refs.empty());
  EXPECT_EQ(LiveEntryCount(), 0);
}

/// No-wait inverts the age test: under wait-die a younger holder leaves an older requester alone,
/// under no-wait every holder blocks every requester regardless of who is older.
class TestPrecisionLockManagerNoWait : public TestPrecisionLockManager {
 protected:
  void SetUp() override {
    FLAGS_txn_locking_policy = 0;
    TestPrecisionLockManager::SetUp();
  }

  void TearDown() override {
    TestPrecisionLockManager::TearDown();
    FLAGS_txn_locking_policy = 1;
  }
};

TEST_F(TestPrecisionLockManagerNoWait, AYoungerExclusiveHolderRefusesAnOlderRequest) {
  ASSERT_TRUE(LockRange(PAGE_A, 0, 100, 200, true));
  // 100 is the OLDER transaction, and wait-die would have waved it through. No-wait refuses it.
  EXPECT_FALSE(LockRange(PAGE_A, 0, 100, 100, true));
  EXPECT_FALSE(LockRange(PAGE_A, 0, 100, 100, false));
  // A disjoint range on the same page is still fine -- only the overlap is a conflict.
  EXPECT_TRUE(LockRange(PAGE_A, 200, 300, 100, true));
}

TEST_F(TestPrecisionLockManagerNoWait, ARefusedRequestLeavesNothingQueued) {
  ASSERT_TRUE(LockRange(PAGE_A, 0, 100, 200, true));
  EXPECT_FALSE(LockRange(PAGE_A, 0, 100, 100, true));
  // The refusal must not have registered a waiter, so releasing the holder frees the range outright.
  Release(200, {});
  EXPECT_EQ(LiveEntryCount(), 0);
  EXPECT_TRUE(LockRange(PAGE_A, 0, 100, 100, true));
}

TEST_F(TestPrecisionLockManagerNoWait, ReadThenWriteStillUpgradesInPlace) {
  ASSERT_TRUE(LockRange(PAGE_A, 0, 100, 100, false));
  // One entry per transaction: the exclusive request widens the existing one rather than queueing.
  EXPECT_TRUE(LockRange(PAGE_A, 0, 100, 100, true));
  EXPECT_EQ(LiveEntryCount(), 1);
  EXPECT_FALSE(LockRange(PAGE_A, 0, 100, 300, false));
}

}  // namespace leanstore::transaction
