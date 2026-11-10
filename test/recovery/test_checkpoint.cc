#include "leanstore/leanstore.h"
#include "recovery/log_manager.h"
#include "test/base_test.h"

#include "gtest/gtest.h"

namespace leanstore::recovery {

class TestCheckpointer : public BaseTest {
 protected:
  void SetUp() override {
    FLAGS_worker_count          = 2;
    FLAGS_txn_commit_group_size = 1;
    FLAGS_wal_checkpoint_shards = 128;
    SetupTestFile();  // Just to initialize the test db file
  }

  void SetCommitStateGsn(timestamp_t gsn) {
    for (wid_t w = 0; w < FLAGS_worker_count; w++) { log_->w_last_gsn_[w].value.store(gsn, std::memory_order_release); }
  }
};

/**
 * @brief The checkpointer recycles only CLOSED blocks whose WAL is durable
 * (block_gsn <= durable frontier). OPEN and DONE blocks are never touched, and
 * CLOSED blocks still beyond the durable frontier must survive every shard visit.
 */
TEST_F(TestCheckpointer, RespectsDurabilityBarrier) {
  constexpr u64 TOTAL               = 10;
  constexpr timestamp_t DURABLE_GSN = 1002;

  /* Give every shard a dirty page whose image GSN equals the durable frontier,
   * so each shard watermark can advance to DURABLE_GSN once that shard is
   * visited by the checkpointer. */
  buffer_->FixExclusive(0);
  buffer_->ToPtr(0)->p_gsn = DURABLE_GSN;
  buffer_->UnfixExclusive(0);
  for (u64 pid = 1; pid <= FLAGS_wal_checkpoint_shards; pid++) {
    auto *page  = buffer_->AllocPage(buffer_->GetMetadataPtr()->GetNewPagePID());
    page->p_gsn = DURABLE_GSN;
    buffer_->UnfixExclusive(buffer_->ToPID(page));
  }

  auto &mgr = log_->WALBlocks();

  /* Block 0: OPEN, still being written -> must not be recycled. */
  auto &open_blk     = mgr.OpenNext();
  open_blk.block_gsn = 1000;
  /* Blocks 1..2: CLOSED with durable WAL (gsn <= DURABLE_GSN) -> recyclable. */
  auto &covered_1     = mgr.OpenNext();
  covered_1.block_gsn = 1001;
  covered_1.state.store(WALBlock::CLOSED, std::memory_order_release);
  auto &covered_2     = mgr.OpenNext();
  covered_2.block_gsn = DURABLE_GSN;
  covered_2.state.store(WALBlock::CLOSED, std::memory_order_release);
  /* Block 3: CLOSED but WAL not yet durable (gsn > DURABLE_GSN) -> must stay. */
  auto &barrier_blk     = mgr.OpenNext();
  barrier_blk.block_gsn = DURABLE_GSN + 1;
  barrier_blk.state.store(WALBlock::CLOSED, std::memory_order_release);
  /* Block 4: already DONE -> must stay DONE. */
  auto &done_blk = mgr.OpenNext();
  done_blk.state.store(WALBlock::DONE, std::memory_order_release);

  /* Only the first two closed blocks' WAL is durable. */
  SetCommitStateGsn(DURABLE_GSN);

  auto cnt_before = mgr.BlockCycleCnt();

  for (u64 round = 0; round < FLAGS_wal_checkpoint_shards + TOTAL + 16; round++) { log_->TriggerCheckpoint(); }

  /* Exactly the 2 covered CLOSED blocks (gsn <= DURABLE_GSN) are recycled. */
  auto cnt_after = mgr.BlockCycleCnt();
  EXPECT_EQ(cnt_after - cnt_before, 2u);

  EXPECT_EQ(open_blk.state.load(std::memory_order_acquire), WALBlock::OPEN);
  EXPECT_EQ(covered_1.state.load(std::memory_order_acquire), WALBlock::DONE);
  EXPECT_EQ(covered_2.state.load(std::memory_order_acquire), WALBlock::DONE);
  EXPECT_EQ(barrier_blk.state.load(std::memory_order_acquire), WALBlock::CLOSED);
  EXPECT_EQ(done_blk.state.load(std::memory_order_acquire), WALBlock::DONE);
}

}  // namespace leanstore::recovery
