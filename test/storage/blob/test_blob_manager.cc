#include "storage/blob/aliasing_guard.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <utility>

using leanstore::storage::ExtentList;
using leanstore::storage::blob::BlobManager;
using leanstore::storage::blob::BlobState;

namespace leanstore::buffer {

#define IS_EMPTY(blob_ptr, test_size)                                 \
  ({                                                                  \
    u8 tmp[BlobState::PageCount(test_size) * PAGE_SIZE];              \
    std::fill_n(tmp, BlobState::PageCount(test_size) * PAGE_SIZE, 0); \
    EXPECT_EQ(std::memcmp(blob_ptr, tmp, test_size), 0);              \
  })

#define REQUEST_TIER(idx)                                                                        \
  ({                                                                                             \
    this->buffer_->FreeStorageManager()->RequestFreeExtent(storage::ExtentList::ExtentSize(idx), \
                                                           this->accept_all_lbd_, start_pid);    \
  })

class TestBlobManager : public BaseTest {
 protected:
  static constexpr u64 BLOB_SIZE = 18432;  // 4.5 * PAGE_SIZE,  3 Extents < Blob < 4 Extents
  u8 *random_blob_[2];
  std::unique_ptr<BlobManager> blob_manager_;

  void SetUp() override {
    BaseTest::SetupTestFile(true);
    for (auto idx = 0; idx < 2; idx++) {
      random_blob_[idx] = static_cast<u8 *>(malloc(BLOB_SIZE));
      for (size_t i = 0; i < BLOB_SIZE; i++) { random_blob_[idx][i] = (idx + 1) * 111; }
    }
    InitRandTransaction();
    blob_manager_ = std::make_unique<BlobManager>(buffer_.get());
  }

  void TearDown() override {
    for (auto &blob : random_blob_) { free(blob); }
    buffer_->AliasArea()->ReleaseAliasingArea();
    blob_manager_.reset();
    BaseTest::TearDown();
  }

  void WriteBlobState(BlobState *blob_h) {
    // Mock: write BLOB to disk
    auto ret = 0;
    for (auto &extent : blob_h->extents) {
      ret += pwrite(this->test_file_fd_, buffer_->ToPtr(extent.start_pid), extent.page_cnt * PAGE_SIZE,
                    extent.start_pid * PAGE_SIZE);
    }
    Ensure(ret >= static_cast<int>(blob_h->PageCount() * PAGE_SIZE));
  }
};

TEST_F(TestBlobManager, InsertNewBlob) {
  FLAGS_blob_normal_buffer_pool = true;  // Safer testing, i.e. no possible OS kernel panic from AliasingGuard

  auto &txn = transaction::Transaction::active_txn;

  auto blob_payload = std::span<u8>{this->random_blob_[0], this->BLOB_SIZE};
  auto blob_h       = this->blob_manager_->AllocateBlob(blob_payload);
  // 4.5 pages -> corresponds to three extents [1, 2, 4]
  ASSERT_EQ(blob_h->extents.extent_cnt, 3);
  EXPECT_EQ(blob_h->PageCount(), std::ceil(static_cast<float>(this->BLOB_SIZE) / PAGE_SIZE));
  for (auto idx = 0; idx < blob_h->extents.NumberOfExtents(); idx++) {
    auto data_ptr   = &blob_payload[(idx == 0) ? 0 : ExtentList::ExtentSize(idx - 1)];
    auto extent_ptr = reinterpret_cast<u8 *>(this->buffer_->ToPtr(blob_h->extents.extent_pid[idx]));
    EXPECT_EQ(std::memcmp(extent_ptr, data_ptr, ExtentList::ExtentSize(idx)), 0);
    EXPECT_EQ(blob_h->extents.extent_pid[idx], txn.ToFlushedLargePages()[idx].start_pid);
  }

  // All states should be UNLOCKED - check `prevent_evict` description
  for (auto &extent : blob_h->extents) {
    CHECK_EXTENT_PAGE_STATE(sync::PageState::UNLOCKED, extent.start_pid, extent.page_cnt);
  }

  auto expected_phys_cnt = this->buffer_->physical_used_cnt_.load();
  this->WriteBlobState(blob_h);

  // Evict all extents
  u64 idx = 0;
  for (auto &extent : blob_h->extents) {
    EXPECT_EQ(extent.start_pid, txn.ToEvictedExtents()[idx]);
    this->buffer_->EvictExtent(extent.start_pid);
    idx++;
  }
  EXPECT_TRUE(txn.HasBLOB());
  ASSERT_EQ(txn.ToEvictedExtents().size(), 3);
  EXPECT_EQ(this->buffer_->physical_used_cnt_.load(), expected_phys_cnt);

  auto expected_state = sync::PageStateMode::UNLOCKED;
  for (auto &extent : blob_h->extents) { CHECK_EXTENT_PAGE_STATE(expected_state, extent.start_pid, extent.page_cnt); }

  // Partially loaded -> only PageState[start_pid] is in SHARED state
  this->blob_manager_->LoadBlob(BlobManager::active_blob, PAGE_SIZE, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), PAGE_SIZE);
    EXPECT_EQ(std::memcmp(blob.data(), this->random_blob_[0], PAGE_SIZE), 0);
  });
  EXPECT_FALSE(BlobManager::extent_loaded.isEmpty());

  auto first_extent = blob_h->extents[0];
  CHECK_EXTENT_PAGE_STATE(1, first_extent.start_pid, first_extent.page_cnt);
  auto second_extent = blob_h->extents[1];
  CHECK_EXTENT_PAGE_STATE(expected_state, second_extent.start_pid, second_extent.page_cnt);

  this->blob_manager_->LoadBlob(BlobManager::active_blob, BlobManager::active_blob->blob_size,
                                [&](std::span<const u8> blob) {
                                  EXPECT_EQ(blob.size(), this->BLOB_SIZE);
                                  EXPECT_EQ(std::memcmp(blob.data(), this->random_blob_[0], this->BLOB_SIZE), 0);
                                });

  for (auto &extent : blob_h->extents) { CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt); }
  EXPECT_THAT(blob_h->sha2_val,
              ::testing::ElementsAre(240, 55, 63, 44, 138, 135, 12, 194, 146, 233, 55, 136, 33, 194, 149, 128, 34, 237,
                                     144, 228, 21, 199, 111, 124, 237, 167, 242, 34, 246, 249, 33, 250));
  this->blob_manager_->UnloadAllBlobs();
  for (auto &extent : blob_h->extents) {
    CHECK_EXTENT_PAGE_STATE(sync::PageStateMode::UNLOCKED, extent.start_pid, extent.page_cnt);
  }

  txn.ToFreeExtents().clear();
  this->blob_manager_->RemoveBlob(blob_h);
  EXPECT_EQ(txn.ToFreeExtents().size(), 3);
  EXPECT_EQ(first_extent.start_pid, txn.ToEvictedExtents()[0]);
}

}  // namespace leanstore::buffer