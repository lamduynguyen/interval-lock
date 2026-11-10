#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <span>
#include <vector>

namespace leanstore::recovery {

using leanstore::storage::blob::BlobManager;
using leanstore::storage::blob::BlobState;

/**
 * @brief BLOB recovery tests: crash & restart, then verify that
 *  - the tree is reconstructed correctly (BlobState values + blob content)
 *  - the free-space state is reconstructed from FREE_EXTENT/REUSE_EXTENT records:
 *      extents of surviving blobs are locked, freed extents are free again
 */
class TestBlobRecovery : public BaseTest {
 protected:
  static constexpr u64 BLOB_SIZE = 18432;  // 4.5 * PAGE_SIZE -> 3 extents [1, 2, 4]
  static constexpr u64 NO_BLOBS  = 8;

  std::unique_ptr<storage::BTree> tree_;
  std::unique_ptr<BlobManager> blob_manager_;
  LogWorker *logger_;

  std::vector<std::vector<u8>> blob_payload_;

  void SetUp() override {
    BaseTest::SetupTestFile(false, true);
    LeanStore::worker_thread_id = 0;
    logger_                     = &(log_->LocalLogWorker());
  }

  void TearDown() override {
    tree_.reset();
    blob_manager_.reset();
    BaseTest::TearDown();
  }

  void ForcePersistLogs() {
    if (logger_->log_buffer.TotalFreeSpace() > 0) { logger_->log_buffer.LogFlush(logger_); }
    if (FLAGS_wal_fsync) { fdatasync(log_->wal_fd_); }
  }

  void InitializeTree() {
    InitRandTransaction();
    tree_         = std::make_unique<storage::BTree>(buffer_.get(), 0);
    blob_manager_ = std::make_unique<BlobManager>(buffer_.get());
    txn_man_->CommitTransaction();
  }

  void MakePayload(u64 idx) {
    blob_payload_.resize(NO_BLOBS);
    blob_payload_[idx].resize(BLOB_SIZE);
    for (size_t i = 0; i < BLOB_SIZE; i++) { blob_payload_[idx][i] = static_cast<u8>((idx + 1) * 17 + i % 251); }
  }

  /* Insert a blob (raw BlobState as the tree value) and commit it durably:
   * group commit drains the queue (WAL flush + blob content write) before we fsync again. */
  void InsertBlob(u64 key) {
    MakePayload(key);
    InitRandTransaction();
    auto blob = blob_manager_->AllocateBlob(blob_payload_[key]);
    std::span<u8> key_span{reinterpret_cast<u8 *>(&key), sizeof(key)};
    tree_->Insert(key_span, {reinterpret_cast<u8 *>(blob), blob->MallocSize()});
    txn_man_->CommitTransaction();
    log_->TriggerGroupCommit(0);
    ForcePersistLogs();
  }

  /* Free the blob's extents and remove its key, then commit durably. */
  void RemoveBlob(u64 key) {
    InitRandTransaction();
    auto blob_bytes = CopyBlobState(key);
    blob_manager_->RemoveBlob(reinterpret_cast<BlobState *>(blob_bytes.data()));
    std::span<u8> key_span{reinterpret_cast<u8 *>(&key), sizeof(key)};
    tree_->Remove(key_span);
    txn_man_->CommitTransaction();
    log_->TriggerGroupCommit(0);
    ForcePersistLogs();
  }

  auto CopyBlobState(u64 key) -> std::vector<u8> {
    std::vector<u8> out;
    std::span<u8> key_span{reinterpret_cast<u8 *>(&key), sizeof(key)};
    auto ret = tree_->LookUp(key_span, [&](std::span<const u8> payload) {
      auto bs = reinterpret_cast<const BlobState *>(payload.data());
      out.resize(bs->MallocSize());
      std::memcpy(out.data(), payload.data(), out.size());
    });
    Ensure(ret == OpResult::OK);
    return out;
  }

  /* Recover the previous session's state. */
  void Recover() {
    recovery_->RecoveryPreparation();
    recovery_->MaterializeLogs();
    recovery_->Analysis();
    InitRandTransaction();
    recovery_->PrepareRedoEnv();  // also runs RedoFreeExtents()
    auto metadata_page      = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
    metadata_page->roots[0] = 1;
    for (auto pid = 1U; pid <= recovery_->max_logged_pid_.load(); pid++) {
      sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
      recovery_->PerPageRedo(page, pid);
    }
    /* With FLAGS_wal_enable_recovery=true the BTree ctor is a no-op: it re-reads
     * roots[0] from the (redone) metadata page at traversal time. */
    tree_         = std::make_unique<storage::BTree>(buffer_.get(), 0);
    blob_manager_ = std::make_unique<BlobManager>(buffer_.get());
  }

  /* Verify that the recovered blob matches the expected payload. */
  void VerifyBlobContent(u64 key) {
    auto blob_bytes = CopyBlobState(key);
    auto bs         = reinterpret_cast<const BlobState *>(blob_bytes.data());
    blob_manager_->LoadBlob(bs, bs->blob_size, [&](std::span<const u8> content) {
      EXPECT_EQ(content.size(), blob_payload_[key].size());
      EXPECT_EQ(std::memcmp(content.data(), blob_payload_[key].data(), blob_payload_[key].size()), 0);
    });
  }

  /* Verify that every extent of the blob is locked (expect_locked == true) or free (false). */
  void VerifyExtentState(u64 key, bool expect_locked) {
    auto blob_bytes = CopyBlobState(key);
    auto bs         = reinterpret_cast<const BlobState *>(blob_bytes.data());
    for (auto &extent : bs->extents) {
      auto is_free = buffer_->FreeStorageManager()->IsFree(extent.start_pid, extent.page_cnt);
      EXPECT_EQ(is_free, !expect_locked) << "extent " << extent.start_pid;
    }
  }
};

TEST_F(TestBlobRecovery, FullDurabilityRecovery) {
  FLAGS_wal_enable_recovery = false;
  ResetDBFile();

  /* Workload session: insert blobs, then remove some. */
  log_->WriteMasterRecord();
  InitializeTree();
  for (u64 i = 0; i < NO_BLOBS; i++) { InsertBlob(i); }
  RemoveBlob(2);
  RemoveBlob(5);

  /* Simulate crash & restart. */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  /* Materialize the WAL: FREE/REUSE records must be collected, not rejected. */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  EXPECT_EQ(recovery_->free_extent_logs_[0].size(), NO_BLOBS * 3 + 2 * 3);  // 24 REUSE + 6 FREE
  recovery_->Analysis();
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);

  Recover();

  /* Tree state: surviving keys present, removed keys absent. */
  for (u64 key = 0; key < NO_BLOBS; key++) {
    std::span<u8> key_span{reinterpret_cast<u8 *>(&key), sizeof(key)};
    auto found = (tree_->LookUp(key_span, [](std::span<const u8>) {}) == OpResult::OK);
    EXPECT_EQ(found, (key != 2 && key != 5));
  }

  /* Content of surviving blobs is intact. */
  for (u64 key = 0; key < NO_BLOBS; key++) {
    if (key != 2 && key != 5) { VerifyBlobContent(key); }
  }

  /* Free-space state: extents of surviving blobs are locked, freed extents are free. */
  for (u64 key = 0; key < NO_BLOBS; key++) {
    if (key != 2 && key != 5) { VerifyExtentState(key, true); }
  }
}

TEST_F(TestBlobRecovery, ReuseAfterFree) {
  FLAGS_wal_enable_recovery = false;
  ResetDBFile();

  /* Workload session: insert A(0), B(1), C(2); remove B; insert D(3) which may
   * reuse B's extents. */
  log_->WriteMasterRecord();
  InitializeTree();
  for (u64 i = 0; i < 3; i++) { InsertBlob(i); }
  std::vector<u8> blob_b_extents;
  ConvenientTxnWrapper([&]() { blob_b_extents = CopyBlobState(1); });
  RemoveBlob(1);
  InsertBlob(3);

  /* Simulate crash & restart. */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  EXPECT_EQ(recovery_->free_extent_logs_[0].size(), 4 * 3 + 3);  // 12 REUSE + 3 FREE
  recovery_->Analysis();
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);

  Recover();

  /* Every surviving blob's extents must be locked after recovery. */
  for (u64 key = 0; key < 4; key++) {
    if (key != 1) { VerifyExtentState(key, true); }
  }

  /* Each of B's extents is either reused by D (hence locked) or still free. */
  auto blob_d_extents = CopyBlobState(3);
  auto bs_b           = reinterpret_cast<const BlobState *>(blob_b_extents.data());
  auto bs_d           = reinterpret_cast<const BlobState *>(blob_d_extents.data());
  for (auto &ext_b : bs_b->extents) {
    auto reused = false;
    for (auto &ext_d : bs_d->extents) {
      if (ext_d.start_pid == ext_b.start_pid) { reused = true; }
    }
    EXPECT_EQ(buffer_->FreeStorageManager()->IsFree(ext_b.start_pid, ext_b.page_cnt), !reused);
  }
}

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count            = 1;
  FLAGS_wal_enable              = true;
  FLAGS_wal_debug               = true;
  FLAGS_wal_enable_recovery     = false;
  FLAGS_wal_fsync               = true;
  FLAGS_wal_block_size_mb       = 2;
  FLAGS_wal_batch_write_kb      = 16;
  FLAGS_txn_commit_group_size   = 1;
  FLAGS_wal_checkpoint_shards   = 32;
  FLAGS_blob_enable             = true;
  FLAGS_blob_normal_buffer_pool = true;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}