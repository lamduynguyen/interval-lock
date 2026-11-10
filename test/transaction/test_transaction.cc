#include "common/typedefs.h"
#include "sync/page_state.h"
#include "test/base_test.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <future>
#include <memory>
#include <thread>

namespace leanstore::transaction {

class TestTransaction : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetupTestFile();
    TransactionManager::global_clock = 0;
  }

  void TearDown() override { BaseTest::TearDown(); }
};

TEST_F(TestTransaction, SerializableTransaction) {
  InitRandTransaction();
  auto &txn = Transaction::active_txn;
  txn.gsn_vector.emplace(100, 200);
  txn.gsn_vector.emplace(300, 100);

  // Populate the lists that Construct() serializes into the content buffer.
  // A non-empty GSN vector makes OffsetWritePages() != 0, so any reader that
  // wrongly looks at &content[0] (instead of OffsetWritePages()) returns garbage.
  txn.ToFlushedLargePages().emplace_back(10, 2);
  txn.ToFlushedLargePages().emplace_back(20, 4);
  txn.ToFlushedLargePages().emplace_back(30, 8);
  txn.ToEvictedExtents().emplace_back(40);
  txn.ToEvictedExtents().emplace_back(50);
  txn.ToFreeExtents().emplace_back(60, 0);
  txn.ToFreeExtents().emplace_back(70, 1);

  // Serialization
  u8 buffer[256];
  auto s = new (&buffer) SerializableTransaction;
  s->Construct(txn);
  EXPECT_EQ(s->vector_size, 2);

  // Deserialization
  for (auto &wid : s->Dependencies()) { EXPECT_THAT(wid, ::testing::AnyOf(100, 300)); }
  for (auto &ts : s->DepGSN()) { EXPECT_THAT(ts, ::testing::AnyOf(200, 100)); }

  // The flushed large pages must round-trip exactly; the old code read them from
  // &content[0] (the GSN-vector region) and produced garbage here.
  auto write_pages = s->ToFlushedLargePages();
  EXPECT_EQ(write_pages.size(), 3);
  EXPECT_EQ(write_pages[0].start_pid, 10);
  EXPECT_EQ(write_pages[0].page_cnt, 2);
  EXPECT_EQ(write_pages[1].start_pid, 20);
  EXPECT_EQ(write_pages[1].page_cnt, 4);
  EXPECT_EQ(write_pages[2].start_pid, 30);
  EXPECT_EQ(write_pages[2].page_cnt, 8);

  // Evicted and free extents round-trip too.
  auto evict_extents = s->ToEvictedExtents();
  EXPECT_EQ(evict_extents.size(), 2);
  EXPECT_EQ(evict_extents[0], 40);
  EXPECT_EQ(evict_extents[1], 50);

  auto free_extents = s->ToFreeExtents();
  EXPECT_EQ(free_extents.size(), 2);
  EXPECT_EQ(free_extents[0].start_pid, 60);
  EXPECT_EQ(free_extents[0].tier, 0);
  EXPECT_EQ(free_extents[1].start_pid, 70);
  EXPECT_EQ(free_extents[1].tier, 1);
}

TEST_F(TestTransaction, TransactionLifeTime) {
  InitRandTransaction();
  auto &txn    = Transaction::active_txn;
  auto &logger = log_->LocalLogWorker();
  auto &buffer = logger.log_buffer;

  EXPECT_TRUE(txn.IsRunning());
  EXPECT_FALSE(txn.HasBLOB());
  EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - sizeof(recovery::LogMetaEntry));
  EXPECT_EQ(txn.start_ts, 0);
  EXPECT_EQ(txn.state, Transaction::State::STARTED);
  EXPECT_EQ(&txn.LogWorker(), &logger);

  auto thread = std::thread([&]() {
    // Concurrently init another txn
    InitRandTransaction(1);

    auto &x_txn    = Transaction::active_txn;
    auto &x_logger = log_->LocalLogWorker();

    EXPECT_NE(&txn, &x_txn);
    EXPECT_NE(&logger, &x_logger);
    EXPECT_EQ(x_txn.start_ts, txn.start_ts + 1);
    EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - sizeof(recovery::LogMetaEntry));
    EXPECT_EQ(x_txn.state, Transaction::State::STARTED);
    EXPECT_EQ(&x_txn.LogWorker(), &x_logger);
    txn_man_->CommitTransaction();

    EXPECT_EQ(x_logger.precommitted_queue.CurrentTail(), sizeof(SerializableTransaction));
    EXPECT_EQ(x_txn.is_read_only_, false);
    EXPECT_EQ(x_txn.state, Transaction::State::READY_TO_COMMIT);
  });
  thread.join();

  txn_man_->CommitTransaction();
  EXPECT_EQ(logger.precommitted_queue.CurrentTail(), sizeof(SerializableTransaction));
  EXPECT_EQ(txn.is_read_only_, false);
  EXPECT_FALSE(txn.HasBLOB());
  EXPECT_EQ(txn.state, Transaction::State::READY_TO_COMMIT);
}

}  // namespace leanstore::transaction

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count       = 4;
  FLAGS_wal_enable         = true;
  FLAGS_wal_batch_write_kb = 1024 * 1024;  // Very large to prevent group commit from being triggered

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}