#include "leanstore/leanstore.h"
#include "storage/blob/buddy_allocator.h"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

namespace leanstore::storage::blob {

static constexpr u64 REGION_START      = 1024;
static constexpr u64 REGION_PAGES      = 1 << 20;  // 1M pages
static constexpr u64 NO_THREADS        = 8;
static constexpr u64 NO_OPS_PER_THREAD = 2000;

TEST(TestBuddyAllocator, BasicAllocation) {
  BuddyAllocator alloc(REGION_START, REGION_PAGES);

  // Allocate one extent of every tier; results must be distinct, in-region, aligned
  std::vector<pageid_t> pids;
  for (extidx_t tier = 0; tier < BuddyAllocator::BUDDY_TIERS; tier++) {
    auto pid = alloc.AllocExtent(tier);
    EXPECT_NE(pid, 0);
    EXPECT_GE(pid, REGION_START);
    EXPECT_LT(pid, REGION_START + REGION_PAGES);
    EXPECT_EQ((pid - REGION_START) % (1UL << tier), 0) << "tier " << tier << " misaligned";
    pids.push_back(pid);
  }
  std::set<pageid_t> unique_pids(pids.begin(), pids.end());
  EXPECT_EQ(unique_pids.size(), pids.size());

  // Exhaust the allocator: tier-0 allocations must eventually fail
  auto exhausted = false;
  for (auto i = 0U; i < REGION_PAGES + 1; i++) {
    if (alloc.AllocExtent(0) == 0) {
      exhausted = true;
      break;
    }
  }
  EXPECT_TRUE(exhausted) << "allocator should run out of space";

  // Free one block; the allocator must hand it right back
  EXPECT_EQ(alloc.AllocExtent(0), 0);
  alloc.FreeExtent(pids[0], 0);
  EXPECT_EQ(alloc.AllocExtent(0), pids[0]);
}

TEST(TestBuddyAllocator, SplitAndCoalesce) {
  BuddyAllocator alloc(REGION_START, REGION_PAGES);

  // A big block should split to serve a small request, then coalesce back on free
  auto big1 = alloc.AllocExtent(10);
  auto big2 = alloc.AllocExtent(10);
  auto big3 = alloc.AllocExtent(10);
  ASSERT_NE(big1, 0);
  ASSERT_NE(big2, 0);
  ASSERT_NE(big3, 0);

  alloc.FreeExtent(big2, 10);
  // Freeing all three should coalesce back into larger blocks
  alloc.FreeExtent(big1, 10);
  alloc.FreeExtent(big3, 10);

  // After coalescing, a full-tier allocation must succeed again
  auto big = alloc.AllocExtent(BuddyAllocator::BUDDY_TIERS - 1);
  EXPECT_NE(big, 0) << "coalescing should restore the maximum-tier block";
}

TEST(TestBuddyAllocator, ConcurrentAllocFree) {
  BuddyAllocator alloc(REGION_START, REGION_PAGES);

  std::vector<std::thread> threads;
  std::vector<std::vector<pageid_t>> acquired(NO_THREADS);

  // Each thread allocates and frees its own extents; no double-grant allowed
  for (u64 t_id = 0; t_id < NO_THREADS; t_id++) {
    threads.emplace_back([&, t_id]() {
      LeanStore::worker_thread_id = t_id;
      auto rng_seed               = t_id + 1;
      for (u64 op = 0; op < NO_OPS_PER_THREAD; op++) {
        auto tier = static_cast<extidx_t>((op * rng_seed) % 6);  // tiers 0..5
        auto pid  = alloc.AllocExtent(tier);
        if (pid == 0) {
          alloc.FreeExtent(acquired[t_id].back(), tier);
          acquired[t_id].pop_back();
          continue;
        }
        acquired[t_id].push_back(pid);
      }
    });
  }
  for (auto &t : threads) { t.join(); }

  // All acquired PIDs must be distinct across threads
  std::set<pageid_t> all_pids;
  for (auto &vec : acquired) {
    for (auto pid : vec) { EXPECT_TRUE(all_pids.insert(pid).second) << "double allocation of pid " << pid; }
  }

  // Free everything; a max-tier allocation must then succeed (full coalescing)
  for (auto &vec : acquired) {
    for (auto pid : vec) { alloc.FreeExtent(pid, 0); }
  }
  EXPECT_NE(alloc.AllocExtent(BuddyAllocator::BUDDY_TIERS - 1), 0);
}

TEST(TestBuddyAllocator, ConcurrentExclusiveRegions) {
  BuddyAllocator alloc(REGION_START, REGION_PAGES);

  // Threads repeatedly grab the same tier and verify no overlap while held
  std::atomic<bool> stop(false);
  std::vector<std::thread> threads;
  std::atomic<u64> total_allocs(0);

  for (u64 t_id = 0; t_id < NO_THREADS; t_id++) {
    threads.emplace_back([&, t_id]() {
      LeanStore::worker_thread_id = t_id;
      std::vector<pageid_t> held;
      while (!stop.load()) {
        auto pid = alloc.AllocExtent(3);
        if (pid == 0) {
          // Out of space: release one and retry
          if (!held.empty()) {
            alloc.FreeExtent(held.back(), 3);
            held.pop_back();
          }
          continue;
        }
        held.push_back(pid);
        total_allocs.fetch_add(1);
      }
      for (auto pid : held) { alloc.FreeExtent(pid, 3); }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  stop.store(true);
  for (auto &t : threads) { t.join(); }

  EXPECT_GT(total_allocs.load(), 0U);
  // Region must be fully free again
  EXPECT_NE(alloc.AllocExtent(BuddyAllocator::BUDDY_TIERS - 1), 0);
}

}  // namespace leanstore::storage::blob