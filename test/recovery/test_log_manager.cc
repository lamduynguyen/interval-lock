#include "common/constants.h"
#include "leanstore/leanstore.h"
#include "recovery/log_entry.h"
#include "recovery/log_manager.h"
#include "test/base_test.h"
#include "transaction/transaction_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <exception>
#include <future>
#include <thread>

namespace leanstore::recovery {

class TestLogManager : public ::testing::Test {
 protected:
  std::atomic<bool> is_running_;

  void SetUp() override {
    is_running_               = true;
    [[maybe_unused]] auto fd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR);
    assert(fd_ >= 0);
    ftruncate(fd_, 1 * GB);
  }
};

TEST_F(TestLogManager, AutonomousCommit) {
  FLAGS_wal_enable            = true;
  LeanStore::worker_thread_id = 0;

  auto log_man = std::make_unique<LogManager>(is_running_);
  auto &logger = log_man->LocalLogWorker();
  logger.backend.Connect();
  auto &buffer = logger.log_buffer;

  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  u64 current_free_space = FLAGS_wal_buffer_size_mb * MB;
  // Simulate TX_START log
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogMetaEntry::Type::TX_START;
  logger.SubmitActiveLogEntry();
  current_free_space -= sizeof(LogMetaEntry);
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // Submit Big Data log
  auto &dt_entry = logger.ReserveDataLog(FLAGS_wal_buffer_size_mb * MB - 200, 0);
  logger.SubmitActiveLogEntry();
  current_free_space -= dt_entry.size;
  // Extra padding might be added to enfore memory-alignment of the log entry
  EXPECT_EQ(logger.active_log->size, FLAGS_wal_buffer_size_mb * MB - 200 + sizeof(DataEntry));
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // EnsureEnoughSpace should complete successfully as worker autonomously write logs,
  //   and wal_cursor should circular back to the beginning
  buffer.EnsureEnoughSpace(&logger, 200);
  EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - current_free_space);
  EXPECT_EQ(buffer.wal_cursor, 0);
  // Submit TX_COMMIT log, it should be right after the data log
  current_free_space = FLAGS_wal_buffer_size_mb * MB - current_free_space;
  auto &commit_entry = logger.ReserveLogMetaEntry();
  commit_entry.type  = LogMetaEntry::Type::TX_COMMIT;
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(buffer.wal_cursor, commit_entry.size);
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space - sizeof(LogMetaEntry));
  EXPECT_EQ(buffer.ContiguousFreeSpaceForNewEntry(), buffer.write_cursor.load() - buffer.wal_cursor);
}

}  // namespace leanstore::recovery