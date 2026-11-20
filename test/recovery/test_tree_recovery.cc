#include "leanstore/env.h"
#include "storage/btree/tree.h"
#include "storage/page.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <unordered_map>

namespace leanstore::recovery {

static constexpr u32 NO_RECORDS = 20000;

/// The exact page count of the 20k-record bulk load:
/// 342 for FASTER/STAGED/PRECISION (same record layout),
/// 354 for inline because of additional inline locks.
static u64 ExpectedPageCount() {
  switch (static_cast<transaction::LockManagerVariant>(FLAGS_txn_locking_variant)) {
    case transaction::LockManagerVariant::BASELINE_INLINE: return 354;
    default: return 342;
  }
}

class TestTreeRecovery : public BaseTest {
 protected:
  std::unique_ptr<storage::BTree> tree_;
  LogWorker *logger_;

  void SetUp() override {
    BaseTest::SetupTestFile(false, true);
    LeanStore::worker_thread_id = 0;
    logger_                     = &(log_->LocalLogWorker());
  }

  void InitializeTree() {
    InitRandTransaction();
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), 0);
    txn_man_->CommitTransaction();
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  void ForcePersistLogs() {
    if (logger_->log_buffer.TotalFreeSpace() > 0) { logger_->log_buffer.LogFlush(logger_); }
    if (FLAGS_wal_fsync) { fdatasync(log_->wal_fd_); }
  }

  template <typename value_t>
  void PrepareData(std::vector<std::pair<int, value_t>> &data, bool get_permutation = false, bool prefill_tree = true) {
    for (size_t idx = 0; idx < NO_RECORDS; idx++) {
      data.emplace_back(static_cast<int>(__builtin_bswap32(idx + 1)), static_cast<value_t>(idx * 100));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % NO_RECORDS + NO_RECORDS / 2; idx++) {
        std::next_permutation(data.begin(), data.end());
      }
    }
    ConvenientTxnWrapper([&]() { Ensure(!tree_->IsNotEmpty()); });
    if (prefill_tree) {
      txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::NO_LOCK,
                                 transaction::Transaction::Mode::OLTP);
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        tree_->Insert(key, payload);
        if (logger_->log_buffer.TotalFreeSpace() >= FLAGS_wal_batch_write_kb) { logger_->log_buffer.LogFlush(logger_); }
      }
      txn_man_->CommitTransaction();
      ForcePersistLogs();
      ConvenientTxnWrapper([&]() { Ensure(tree_->IsNotEmpty()); });
    }
  }

  void EvaluateLogData(u64 no_pages) {
    for (auto pid = 1U; pid <= no_pages; pid++) {
      auto buffer   = AlignedPtr(PAGE_SIZE);
      auto prev_gsn = 1;
      LogIOSegment::LazyGenerator log_generator;
      for (auto &buffer : recovery_->page_log_[0][pid]) { log_generator.ReceiveInput(&buffer); }
      log_generator.Iterate([&](const LogEntry *log) {
        log->ValidateChksum();
        auto dt = reinterpret_cast<const DataEntry *>(log);
        EXPECT_GT(dt->rc.gsn, prev_gsn);
        /* First step should always be a page initialization */
        auto entry = reinterpret_cast<const storage::WALEntry *>(log);
        if (prev_gsn == 0) {
          EXPECT_TRUE((entry->wal_type == storage::WalType::NEW_PAGE) ||
                      (entry->wal_type == storage::WalType::NEW_ROOT) ||
                      (entry->wal_type == storage::WalType::FRESH_PAGE));
        }
        EXPECT_EQ(reinterpret_cast<const DataEntry *>(log)->prev_gsn, prev_gsn);
        prev_gsn = dt->rc.gsn;
        return true;
      });
    }
  }
};

TEST_F(TestTreeRecovery, BulkInsertRecovery) {
  FLAGS_wal_enable_recovery = false;
  ResetDBFile();

  /* Initialize recovery env */
  log_->WriteMasterRecord();
  LOG_INFO("signature {}", leanstore::LeanStore::signature);
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);
  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, ExpectedPageCount());

  /* Make a clone of the current buffer pool */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Reset the state */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  /* Load written log segments into the recovery manager */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->page_log_[0].size(), no_pages + 1);
  // Only the two write txns (tree init + bulk load) log;
  // the read-only CountPages txn is acked without a WAL record.
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 2);
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  auto max_pid = std::max_element(std::begin(recovery_->page_log_[0]), std::end(recovery_->page_log_[0]),
                                  [](const auto &p1, const auto &p2) { return p1.first < p2.first; });
  EXPECT_EQ((*max_pid).first, no_pages);

  /* Try to evaluate recovered page data to the current page data */
  EvaluateLogData(no_pages);

  /* Try to redo every page */
  InitRandTransaction();
  recovery_->PrepareRedoEnv();
  auto metadata_page      = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
  metadata_page->roots[0] = 1;
  for (auto pid = 1U; pid <= no_pages; pid++) {
    sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
    recovery_->PerPageRedo(page, pid);
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
  }
}

TEST_F(TestTreeRecovery, MixWorkloads) {
  FLAGS_wal_enable_recovery = false;
  ResetDBFile();

  /* Initialize recovery env */
  log_->WriteMasterRecord();
  LOG_INFO("signature {}", leanstore::LeanStore::signature);
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);
  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, ExpectedPageCount());
  const u64 planted_pages = no_pages;

  /* Random UpdateInPlace */
  InitRandTransaction();
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS / 100); idx++) {
    auto start_tid = RandomGenerator::GetRand<int>(idx + 1, NO_RECORDS - 50);
    for (auto tid = start_tid; tid < start_tid + 50; tid++) {
      auto &[key, value] = data[tid];
      tree_->UpdateInPlace(
        {reinterpret_cast<u8 *>(&key), sizeof(int)},
        [&](auto payload) {
          std::span new_w{reinterpret_cast<u8 *>(&value), sizeof(__uint128_t)};
          EXPECT_EQ(payload.size(), sizeof(__uint128_t));
          auto cmp = std::memcmp(payload.data(), new_w.data(), sizeof(__uint128_t));
          EXPECT_EQ(cmp, 0);
          std::memcpy(payload.data(), new_w.data(), new_w.size());
        },
        nullptr);
    }
  }
  txn_man_->CommitTransaction();

  /* Should be no change from UpdateInPlace */
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, planted_pages);

  /* Random deletion */
  InitRandTransaction();
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS / 100); idx++) {
    auto start_tid = RandomGenerator::GetRand<int>(idx + 1, NO_RECORDS);
    for (auto tid = start_tid; tid < start_tid + 50; tid++) {
      int key = __builtin_bswap32(tid);
      tree_->Remove({reinterpret_cast<u8 *>(&key), sizeof(int)});
    }
  }
  no_pages = tree_->CountPages();
  EXPECT_LT(no_pages, planted_pages);
  txn_man_->CommitTransaction();

  /* Force write logs if there are dirty ones */
  ForcePersistLogs();

  /* Make a clone of the current buffer pool */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Reset the state */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  /* Load written log segments into the recovery manager */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  // Every page the bulk phase touched must be in the log: metadata + all planted pages. Updates
  // and deletes only re-log existing pages -- same-size payloads cannot split, merges cannot
  // allocate.
  EXPECT_EQ(recovery_->page_log_[0].size(), planted_pages + 1);
  // Only the write txns (tree init, bulk load, update, delete) log;
  // the four read-only txns are acked without WAL records.
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 4);
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  auto max_pid = std::max_element(std::begin(recovery_->page_log_[0]), std::end(recovery_->page_log_[0]),
                                  [](const auto &p1, const auto &p2) { return p1.first < p2.first; });
  // Updates and deletes never allocate: the bulk load's dense pid sequence ends at
  // planted_pages, the last page it created.
  EXPECT_EQ((*max_pid).first, planted_pages);

  /* Try to evaluate recovered page data to the current page data */
  EvaluateLogData(no_pages);

  /* Try to redo every page */
  InitRandTransaction();
  recovery_->PrepareRedoEnv();
  auto metadata_page      = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
  metadata_page->roots[0] = 1;
  for (auto pid = 1U; pid <= no_pages; pid++) {
    sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
    recovery_->PerPageRedo(page, pid);
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
  }
}

TEST_F(TestTreeRecovery, CheckpointAwareRecovery) {
  FLAGS_wal_enable_recovery = false;
  ResetDBFile();

  /* Session 1, batch 1: insert the data set. */
  log_->WriteMasterRecord();
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);

  /* Drive one full shard cycle: each increment flushes a 1/S fraction of the
   * buffer pool (round-robin), so after S = wal_checkpoint_shards increments
   * every dirty page at or below the durable frontier is persisted. */
  ForcePersistLogs();
  auto shards = (FLAGS_wal_checkpoint_shards > 0) ? FLAGS_wal_checkpoint_shards : FLAGS_worker_count;
  shards      = std::max<u64>(1, shards);
  for (u64 inc = 0; inc < shards; inc++) { log_->TriggerCheckpoint(); }

  /* Sanity: the checkpoint must have persisted page images to disk. */
  auto disk_page = AlignedPtr(PAGE_SIZE);
  int fd         = open(FLAGS_db_path.c_str(), O_RDONLY | O_DIRECT, 0);
  auto cnt       = pread(fd, disk_page.get(), PAGE_SIZE, PAGE_SIZE);
  close(fd);
  ASSERT_EQ(cnt, static_cast<ssize_t>(PAGE_SIZE));
  EXPECT_EQ(std::memcmp(disk_page.get(), buffer_->ToPtr(1), PAGE_SIZE), 0);

  /* Session 1, batch 2: modifications that only exist in the log, whose GSNs are
   * above the checkpointed frontier. */
  std::vector<std::pair<int, __uint128_t>> more_data;
  for (auto idx = 0UL; idx < NO_RECORDS / 2; idx++) {
    more_data.emplace_back(__builtin_bswap32(idx + NO_RECORDS + 1), static_cast<__uint128_t>((idx + NO_RECORDS) * 100));
  }
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::NO_LOCK,
                             transaction::Transaction::Mode::OLTP);
  for (auto &pair : more_data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};
    tree_->Insert(key, payload);
    if (logger_->log_buffer.TotalFreeSpace() >= FLAGS_wal_batch_write_kb) { logger_->log_buffer.LogFlush(logger_); }
  }
  txn_man_->CommitTransaction();
  ForcePersistLogs();

  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });

  /* Clone the current buffer pool (batch 1 + batch 2) as the expected result. */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Simulate crash & restart. */
  TearDown();
  FLAGS_wal_enable_recovery   = true;
  FLAGS_wal_enable_checkpoint = false;
  SetUp();

  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  InitRandTransaction();
  recovery_->PrepareRedoEnv();

  /* Checkpoint-aware recovery: each page is loaded from its flushed disk image,
   * then only the log records above the page's GSN are reapplied. The result
   * must match the state built by the original session. */
  for (auto pid = 1U; pid <= no_pages; pid++) {
    sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
    recovery_->PerPageRedo(page, pid);
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
  }

  /* The metadata page's root survives via its disk image + NEW_ROOT redo. */
  auto lhs_meta = reinterpret_cast<storage::MetadataPage *>(&expected_result[0]);
  auto rhs_meta = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
  EXPECT_EQ(lhs_meta->roots[0], rhs_meta->roots[0]);
}

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count          = 1;
  FLAGS_wal_enable            = true;
  FLAGS_wal_debug             = true;
  FLAGS_wal_enable_recovery   = false;
  FLAGS_wal_fsync             = true;
  FLAGS_wal_block_size_mb     = 2;
  FLAGS_wal_batch_write_kb    = 16;
  FLAGS_txn_commit_group_size = 1;
  FLAGS_wal_checkpoint_shards = 32;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
