#include "recovery/checkpoint.h"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <thread>

namespace leanstore::recovery {

static constexpr u64 kTestWalBlockSize = 4096ULL * 1024;          // 4 MB
static constexpr u64 kTestWalRegionEnd = kTestWalBlockSize * 20;  // enough room for all tests

TEST(TestWALBlock, RingWrapAround) {
  auto num_blocks = 16U;
  WALBlockManager blk_mgr(num_blocks, kTestWalBlockSize, kTestWalRegionEnd);
  EXPECT_EQ(blk_mgr.Capacity(), num_blocks);

  /* Open more blocks than the ring capacity to force wrap-around.
   * Each iteration: claim a slot, fill it, mark DONE so the slot is reusable. */
  constexpr auto TOTAL = 40UL;
  for (auto i = 0UL; i < TOTAL; i++) {
    auto &blk     = blk_mgr.OpenNext();
    blk.block_gsn = i + 7;
    blk.state.store(WALBlock::DONE);
  }
  /* If we got here without blocking, the ring wrapped successfully. */
}

TEST(TestWALBlock, StampingAndReuseState) {
  WALBlockManager blk_mgr(4, kTestWalBlockSize, kTestWalRegionEnd);

  /* Open slot 0, fill it, mark DONE so it becomes recyclable */
  auto &b0     = blk_mgr.OpenNext();
  b0.block_gsn = 10;
  b0.state.store(WALBlock::DONE, std::memory_order_release);

  /* Open the remaining 3 slots and mark them DONE too */
  for (u64 i = 0; i < 3; i++) {
    auto &blk = blk_mgr.OpenNext();
    blk.state.store(WALBlock::DONE, std::memory_order_release);
  }

  /* All 4 slots are DONE; next OpenNext reclaims one. The reclaimed block
   * must have its state reset to OPEN and block_gsn zeroed. */
  auto &reclaimed = blk_mgr.OpenNext();
  EXPECT_EQ(reclaimed.state.load(std::memory_order_acquire), WALBlock::OPEN);
  EXPECT_EQ(reclaimed.block_gsn, 0u);
}

TEST(TestWALBlock, BackpressureWaitsForDone) {
  WALBlockManager blk_mgr(4, kTestWalBlockSize, kTestWalRegionEnd);

  /* Fill the four fresh slots — all become OPEN (not DONE) */
  for (u64 i = 0; i < 4; i++) { blk_mgr.OpenNext(); }

  /* Next OpenNext must block because no slot is DONE */
  std::atomic<bool> opened{false};
  std::thread writer([&]() {
    blk_mgr.OpenNext();
    opened.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(opened.load());

  /* Mark one slot DONE → the blocked thread unblocks */
  blk_mgr.blocks_[0].state.store(WALBlock::DONE, std::memory_order_release);
  writer.join();
  EXPECT_TRUE(opened.load());
}

TEST(TestWALBlock, BackpressureProceedsWhenDone) {
  WALBlockManager blk_mgr(4, kTestWalBlockSize, kTestWalRegionEnd);

  /* Open, fill, and checkpoint every slot so the ring is fully reusable */
  for (u64 i = 0; i < 4; i++) {
    auto &blk     = blk_mgr.OpenNext();
    blk.block_gsn = 100 + i;
    blk.state.store(WALBlock::DONE, std::memory_order_release);
  }

  /* Wrapping must not block: every slot is already DONE */
  auto &b4 = blk_mgr.OpenNext();
  EXPECT_EQ(b4.state.load(std::memory_order_acquire), WALBlock::OPEN);
  EXPECT_EQ(b4.block_gsn, 0u);
}

TEST(TestWALBlock, MarkDoneThroughGsnRecyclesCoveredPrefix) {
  WALBlockManager blk_mgr(4, kTestWalBlockSize, kTestWalRegionEnd);

  /* Open 3 blocks with increasing watermarks; slot 3 stays DONE (untouched) */
  for (u64 i = 0; i < 3; i++) {
    auto &blk     = blk_mgr.OpenNext();
    blk.block_gsn = 100 + i;
    blk.state.store(WALBlock::CLOSED, std::memory_order_release);
  }

  /* Pool: 4 original - 3 opened = 1 available (slot 3) */
  EXPECT_EQ(blk_mgr.BlockCycleCnt(), 1u);

  /* A watermark covering the first two blocks recycles exactly those two.
   * Pool goes from 1 to 3 (slot 3 + 2 newly recycled). */
  blk_mgr.MarkDoneThroughGsn(101);
  EXPECT_EQ(blk_mgr.BlockCycleCnt(), 3u);
  EXPECT_EQ(blk_mgr.blocks_[0].state.load(std::memory_order_acquire), WALBlock::DONE);
  EXPECT_EQ(blk_mgr.blocks_[1].state.load(std::memory_order_acquire), WALBlock::DONE);
  EXPECT_EQ(blk_mgr.blocks_[2].state.load(std::memory_order_acquire), WALBlock::CLOSED);  // gsn 102 > 101
  EXPECT_EQ(blk_mgr.blocks_[3].state.load(std::memory_order_acquire), WALBlock::DONE);    // was already DONE

  /* Advancing the watermark past every block recycles the remaining CLOSED one.
   * Pool goes from 3 to 4 (all blocks available). */
  blk_mgr.MarkDoneThroughGsn(102);
  EXPECT_EQ(blk_mgr.BlockCycleCnt(), 4u);
  EXPECT_EQ(blk_mgr.blocks_[2].state.load(std::memory_order_acquire), WALBlock::DONE);
}

}  // namespace leanstore::recovery
