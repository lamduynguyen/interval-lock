#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"
#include "transaction/lock_manager/precision_lock_manager.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <unordered_map>
#include <unordered_set>

namespace leanstore::storage {

static constexpr u32 NO_RECORDS = 10000;
static constexpr u32 NO_THREADS = 10;

class TestBTree : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get(), 0);
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
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
    Ensure(!tree_->IsNotEmpty());
    if (prefill_tree) {
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        tree_->Insert(key, payload);
      }
      Ensure(tree_->IsNotEmpty());
    }
  }
};

TEST_F(TestBTree, InsertAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};

    auto found = tree_->LookUp(key, [&payload](std::span<const u8> data) {
      EXPECT_EQ(payload.size(), data.size());
      for (size_t idx = 0; idx < data.size(); idx++) { EXPECT_EQ(payload[idx], data[idx]); }
    });
    ASSERT_EQ(found, OpResult::OK);
  }
}

TEST_F(TestBTree, RemoveAndQuery) {
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, false);
  std::array<bool, NO_RECORDS + 1> removed_f = {false};

  ASSERT_TRUE(tree_->IsNotEmpty());

  // Remove random
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS); idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};

    if (!removed_f[int_key]) {
      auto success = tree_->Remove(key);
      ASSERT_EQ(success, OpResult::OK);
      removed_f[int_key] = true;
    } else {
      auto success = tree_->Remove(key);
      ASSERT_NE(success, OpResult::OK);
    }

    auto found = tree_->LookUp(key, {});
    ASSERT_NE(found, OpResult::OK);
  }

  // Now remove all
  for (auto idx = 1; idx <= static_cast<int>(NO_RECORDS); idx++) {
    if (!removed_f[idx]) {
      int ordered_key = __builtin_bswap32(idx);
      std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
      auto success = tree_->Remove(key);
      ASSERT_EQ(success, OpResult::OK);
      auto found = tree_->LookUp(key, {});
      ASSERT_NE(found, OpResult::OK);
    }
  }

  ASSERT_FALSE(tree_->IsNotEmpty());
}

TEST_F(TestBTree, UpdateAndQuery) {
  std::vector<std::pair<int, int>> data;
  PrepareData<int>(data, false);

  ASSERT_TRUE(tree_->IsNotEmpty());
  std::unordered_map<int, int> validation;
  for (size_t idx = 0; idx < NO_RECORDS; idx++) { validation[idx + 1] = idx * 100; }

  // Remove random
  for (auto idx = 0ULL; idx < NO_RECORDS; idx++) {
    int int_key     = rand() % NO_RECORDS + 1;
    int ordered_key = __builtin_bswap32(int_key);
    std::span key{reinterpret_cast<u8 *>(&ordered_key), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&idx), sizeof(int)};

    auto success        = tree_->Update(key, payload, [&](std::span<const u8> prev) {
      int prev_num;
      std::memcpy(&prev_num, prev.data(), prev.size());
      EXPECT_EQ(prev_num, validation[int_key]);
    });
    validation[int_key] = idx;
    ASSERT_EQ(success, OpResult::OK);

    auto found = tree_->LookUp(key, [&payload](std::span<const u8> found_payload) {
      EXPECT_EQ(payload.size(), found_payload.size());
      for (size_t idx = 0; idx < found_payload.size(); idx++) { EXPECT_EQ(payload[idx], found_payload[idx]); }
    });
    ASSERT_EQ(found, OpResult::OK);
  }

  ASSERT_EQ(tree_->CountEntries(), NO_RECORDS);
}

TEST_F(TestBTree, TreeScan) {
  std::vector<std::pair<int, int>> data;
  PrepareData<int>(data, false);
  std::unordered_map<int, int> scan_result;

  auto read_cb = [&scan_result](std::span<u8> key, std::span<u8> payload) -> bool {
    scan_result[LoadUnaligned<int>(key.data())] = LoadUnaligned<int>(payload.data());
    return true;
  };

  // Scan Asc
  int start_key = 0;
  std::span key{reinterpret_cast<u8 *>(&start_key), sizeof(int)};
  tree_->ScanAscending({key, {}}, read_cb);
  EXPECT_EQ(scan_result.size(), data.size());
  for (auto &pair : data) {
    ASSERT_TRUE(scan_result.find(pair.first) != scan_result.end());
    ASSERT_TRUE(scan_result[pair.first] == pair.second);
  }

  // Scan Desc: start above every key and walk down past the lowest one
  start_key   = NO_RECORDS + 1;
  auto hi_key = __builtin_bswap32(start_key);
  start_key   = 0;
  key         = std::span<u8>{reinterpret_cast<u8 *>(&hi_key), sizeof(int)};
  scan_result.clear();
  tree_->ScanDescending({std::span<u8>{reinterpret_cast<u8 *>(&start_key), sizeof(int)}, key}, read_cb);
  for (auto &pair : data) {
    EXPECT_TRUE(scan_result.find(pair.first) != scan_result.end());
    EXPECT_TRUE(scan_result[pair.first] == pair.second);
  }
  EXPECT_EQ(scan_result.size(), data.size());
}

TEST_F(TestBTree, ConcurrentInsertAndSearch) {
  std::vector<std::pair<int, int>> data;
  std::array<std::atomic<bool>, NO_RECORDS + 1> inserted_f;
  std::atomic<size_t> insert_idx = 0;
  std::thread threads[NO_THREADS];

  PrepareData<int>(data, true, false);
  for (u32 t_id = 0; t_id < NO_THREADS; t_id++) {
    threads[t_id] = std::thread([&, t_id]() {
      InitRandTransaction(t_id);

      // it would take like a year to complete the tests for NO_RECORDS operations
      //  thus, we only run NO_RECORDS / 10 ops
      for (size_t idx = 0; idx < NO_RECORDS / 10; idx++) {
        int r = rand() % 2;
        if (r == 0 && insert_idx < NO_RECORDS) {
          // Insert
          auto index_pos = insert_idx++;
          if (index_pos < data.size()) {
            std::span key{reinterpret_cast<u8 *>(&data[index_pos].first), sizeof(int)};
            std::span payload{reinterpret_cast<u8 *>(&data[index_pos].second), sizeof(int)};
            tree_->Insert(key, payload);
            inserted_f[index_pos] = true;
          }
        } else if (insert_idx.load() > 0) {
          // Search
          auto search_pos = rand() % insert_idx.load();
          while (!inserted_f[search_pos].load()) { search_pos = rand() % insert_idx.load(); }
          std::span key{reinterpret_cast<u8 *>(&data[search_pos].first), sizeof(int)};

          int found_data;
          auto found = tree_->LookUp(
            key, [&found_data](std::span<const u8> data) { std::memcpy(&found_data, data.data(), data.size()); });
          EXPECT_EQ(found_data, data[search_pos].second);
          ASSERT_EQ(found, OpResult::OK);
        }
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

class TestBTreePrecisionScan : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;
  u32 saved_variant_{0};
  std::string saved_iso_;

  void SetUp() override {
    saved_variant_                    = FLAGS_txn_locking_variant;
    saved_iso_                        = FLAGS_txn_default_isolation_level;
    FLAGS_txn_locking_variant         = 3;
    FLAGS_txn_default_isolation_level = "nl";
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get(), 0);
    catalog.push_back(tree_.get());
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
    FLAGS_txn_locking_variant         = saved_variant_;
    FLAGS_txn_default_isolation_level = saved_iso_;
  }
};

TEST_F(TestBTreePrecisionScan, EarlyScanStopDoesNotNarrowLockedRange) {
  constexpr u32 no_records = 10000;
  for (u32 i = 1; i <= no_records; i++) {
    int key     = static_cast<int>(__builtin_bswap32(i));
    int payload = static_cast<int>(i);
    tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(int)}, {reinterpret_cast<u8 *>(&payload), sizeof(int)});
  }
  txn_man_->CommitTransaction();
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);

  // Trace 3: scan from 5000 with LIMIT 3, stopping inside the first leaf.
  int start_key = static_cast<int>(__builtin_bswap32(5000));
  std::span<u8> no_hi;
  ScanRange range{{reinterpret_cast<u8 *>(&start_key), sizeof(int)}, no_hi};
  int read = 0;
  auto ret = tree_->ScanAscending(range, [&](std::span<u8>, std::span<u8>) {
    read++;
    return read < 3;
  });
  ASSERT_EQ(ret, OpResult::OK);
  ASSERT_EQ(read, 3);

  // The held lock starts at the scan's lower bound and reaches the leaf fence, not the last
  // read key; otherwise a later split could create a ref outside the release range.
  pageid_t locked_pid   = 0;
  size_t locked_entries = 0;
  for (pageid_t pid = 1; pid < N_PAGES; pid++) {
    auto &refs = buffer_->BufferFramePage(pid).range_refs;
    if (!refs.empty()) {
      locked_pid = pid;
      locked_entries += refs.size();
    }
  }
  ASSERT_EQ(locked_entries, 1);
  ASSERT_NE(locked_pid, 0);
  auto *entry = buffer_->BufferFramePage(locked_pid).range_refs[0];
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Lo(), {reinterpret_cast<u8 *>(&start_key), sizeof(int)}), 0);
  EXPECT_FALSE(entry->LoAfter());
  EXPECT_TRUE(entry->HiAfter());
  auto *leaf = reinterpret_cast<BTreeNode *>(buffer_->ToPtr(locked_pid));
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Hi(), leaf->GetUpperFence()), 0);

  txn_man_->CommitTransaction();
  for (pageid_t pid = 1; pid < N_PAGES; pid++) { EXPECT_TRUE(buffer_->BufferFramePage(pid).range_refs.empty()); }
}

TEST_F(TestBTreePrecisionScan, ScanBoundsAreInclusive) {
  for (u32 i = 1; i <= 100; i++) {
    int key     = static_cast<int>(__builtin_bswap32(i));
    int payload = static_cast<int>(i);
    tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(int)}, {reinterpret_cast<u8 *>(&payload), sizeof(int)});
  }
  txn_man_->CommitTransaction();

  int lo_key = static_cast<int>(__builtin_bswap32(10));
  int hi_key = static_cast<int>(__builtin_bswap32(20));
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  std::vector<u32> ascending;
  ScanRange asc{{reinterpret_cast<u8 *>(&lo_key), sizeof(int)}, {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}};
  ASSERT_EQ(
    tree_->ScanAscending(asc,
                         [&](std::span<u8> key, std::span<u8>) {
                           ascending.push_back(__builtin_bswap32(static_cast<u32>(LoadUnaligned<int>(key.data()))));
                           return true;
                         }),
    OpResult::OK);
  ASSERT_EQ(ascending.size(), 11);
  EXPECT_EQ(ascending.front(), 10);
  EXPECT_EQ(ascending.back(), 20);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  std::vector<u32> descending;
  ScanRange desc{{reinterpret_cast<u8 *>(&lo_key), sizeof(int)}, {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}};
  ASSERT_EQ(
    tree_->ScanDescending(desc,
                          [&](std::span<u8> key, std::span<u8>) {
                            descending.push_back(__builtin_bswap32(static_cast<u32>(LoadUnaligned<int>(key.data()))));
                            return true;
                          }),
    OpResult::OK);
  ASSERT_EQ(descending.size(), 11);
  EXPECT_EQ(descending.front(), 20);
  EXPECT_EQ(descending.back(), 10);
  txn_man_->CommitTransaction();
}

TEST_F(TestBTreePrecisionScan, ScanBoundsRespectExclusiveFlags) {
  for (u32 i = 1; i <= 100; i++) {
    int key     = static_cast<int>(__builtin_bswap32(i));
    int payload = static_cast<int>(i);
    tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(int)}, {reinterpret_cast<u8 *>(&payload), sizeof(int)});
  }
  txn_man_->CommitTransaction();

  int lo_key = static_cast<int>(__builtin_bswap32(10));
  int hi_key = static_cast<int>(__builtin_bswap32(20));
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  std::vector<u32> ascending;
  ScanRange asc{
    {reinterpret_cast<u8 *>(&lo_key), sizeof(int)}, {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}, true, false};
  ASSERT_EQ(
    tree_->ScanAscending(asc,
                         [&](std::span<u8> key, std::span<u8>) {
                           ascending.push_back(__builtin_bswap32(static_cast<u32>(LoadUnaligned<int>(key.data()))));
                           return true;
                         }),
    OpResult::OK);
  ASSERT_EQ(ascending.size(), 10);
  EXPECT_EQ(ascending.front(), 10);
  EXPECT_EQ(ascending.back(), 19);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  std::vector<u32> descending;
  ScanRange desc{
    {reinterpret_cast<u8 *>(&lo_key), sizeof(int)}, {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}, false, true};
  ASSERT_EQ(
    tree_->ScanDescending(desc,
                          [&](std::span<u8> key, std::span<u8>) {
                            descending.push_back(__builtin_bswap32(static_cast<u32>(LoadUnaligned<int>(key.data()))));
                            return true;
                          }),
    OpResult::OK);
  ASSERT_EQ(descending.size(), 10);
  EXPECT_EQ(descending.front(), 20);
  EXPECT_EQ(descending.back(), 11);
  txn_man_->CommitTransaction();
}

TEST_F(TestBTreePrecisionScan, FixedRangeScanUsesOneEntryAcrossLeaves) {
  constexpr u32 no_records = 10000;
  for (u32 i = 1; i <= no_records; i++) {
    int key     = static_cast<int>(__builtin_bswap32(i));
    int payload = static_cast<int>(i);
    tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(int)}, {reinterpret_cast<u8 *>(&payload), sizeof(int)});
  }
  txn_man_->CommitTransaction();
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);

  int lo_key = static_cast<int>(__builtin_bswap32(4000));
  int hi_key = static_cast<int>(__builtin_bswap32(6000));
  ScanRange range{{reinterpret_cast<u8 *>(&lo_key), sizeof(int)}, {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}};
  u32 read = 0;
  auto ret = tree_->ScanAscending(range, [&](std::span<u8>, std::span<u8>) {
    read++;
    return true;
  });
  ASSERT_EQ(ret, OpResult::OK);
  EXPECT_EQ(read, 2001);

  std::unordered_set<transaction::RangeEntry *> unique_entries;
  size_t refs = 0;
  for (pageid_t pid = 1; pid < N_PAGES; pid++) {
    auto &page_refs = buffer_->BufferFramePage(pid).range_refs;
    refs += page_refs.size();
    for (auto *entry : page_refs) { unique_entries.insert(entry); }
  }
  EXPECT_EQ(unique_entries.size(), 1);
  EXPECT_GT(refs, 1);
  auto *entry = *unique_entries.begin();
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Lo(), {reinterpret_cast<u8 *>(&lo_key), sizeof(int)}), 0);
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Hi(), {reinterpret_cast<u8 *>(&hi_key), sizeof(int)}), 0);

  txn_man_->CommitTransaction();
  for (pageid_t pid = 1; pid < N_PAGES; pid++) { EXPECT_TRUE(buffer_->BufferFramePage(pid).range_refs.empty()); }
}

TEST_F(TestBTreePrecisionScan, OpenEndedDescendingScanLocksEachVisitedLeaf) {
  constexpr u32 no_records = 10000;
  for (u32 i = 1; i <= no_records; i++) {
    int key     = static_cast<int>(__builtin_bswap32(i));
    int payload = static_cast<int>(i);
    tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(int)}, {reinterpret_cast<u8 *>(&payload), sizeof(int)});
  }
  txn_man_->CommitTransaction();
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);

  int start_key = static_cast<int>(__builtin_bswap32(5000));
  std::span<u8> no_lo;
  ScanRange range{no_lo, {reinterpret_cast<u8 *>(&start_key), sizeof(int)}};
  int read = 0;
  auto ret = tree_->ScanDescending(range, [&](std::span<u8>, std::span<u8>) {
    read++;
    return read < 3;
  });
  ASSERT_EQ(ret, OpResult::OK);
  ASSERT_EQ(read, 3);

  pageid_t locked_pid   = 0;
  size_t locked_entries = 0;
  for (pageid_t pid = 1; pid < N_PAGES; pid++) {
    auto &refs = buffer_->BufferFramePage(pid).range_refs;
    if (!refs.empty()) {
      locked_pid = pid;
      locked_entries += refs.size();
    }
  }
  ASSERT_EQ(locked_entries, 1);
  ASSERT_NE(locked_pid, 0);
  auto *entry = buffer_->BufferFramePage(locked_pid).range_refs[0];
  EXPECT_TRUE(entry->LoAfter());
  EXPECT_TRUE(entry->HiAfter());
  auto *leaf = reinterpret_cast<BTreeNode *>(buffer_->ToPtr(locked_pid));
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Lo(), leaf->GetLowerFence()), 0);
  EXPECT_EQ(transaction::RangeEntry::Cmp(entry->Hi(), {reinterpret_cast<u8 *>(&start_key), sizeof(int)}), 0);

  txn_man_->CommitTransaction();
  for (pageid_t pid = 1; pid < N_PAGES; pid++) { EXPECT_TRUE(buffer_->BufferFramePage(pid).range_refs.empty()); }
}

class TestBTreeInline : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;
  u32 saved_variant_{0};
  u32 saved_policy_{0};
  std::string saved_iso_;

  void SetUp() override {
    saved_variant_                    = FLAGS_txn_locking_variant;
    saved_policy_                     = FLAGS_txn_locking_policy;
    saved_iso_                        = FLAGS_txn_default_isolation_level;
    FLAGS_txn_locking_variant         = 2;
    FLAGS_txn_locking_policy          = 0;
    FLAGS_txn_default_isolation_level = "nl";
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get(), 0);
    catalog.push_back(tree_.get());
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
    FLAGS_txn_locking_variant         = saved_variant_;
    FLAGS_txn_locking_policy          = saved_policy_;
    FLAGS_txn_default_isolation_level = saved_iso_;
  }

  static auto OrderedKey(u32 value) -> int { return static_cast<int>(__builtin_bswap32(value)); }
};

TEST_F(TestBTreeInline, MissingLookUpDoesNotLeakLockOnSuccessor) {
  for (u32 value : {1U, 3U}) {
    int key     = OrderedKey(value);
    int payload = static_cast<int>(value * 10);
    ASSERT_EQ(
      tree_->Insert({reinterpret_cast<u8 *>(&key), sizeof(key)}, {reinterpret_cast<u8 *>(&payload), sizeof(payload)}),
      OpResult::OK);
  }
  txn_man_->CommitTransaction();

  // Missing key 2 falls between keys 1 and 3. Inline used to take the shared lock of the successor
  // (key 3) under key 2, so commit could not find and release it.
  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  int missing = OrderedKey(2);
  ASSERT_EQ(tree_->LookUp({reinterpret_cast<u8 *>(&missing), sizeof(missing)}, {}), OpResult::NOT_FOUND);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::SERIALIZABLE,
                             transaction::Transaction::Mode::OLTP);
  int key         = OrderedKey(3);
  int new_payload = 99;
  auto ret        = tree_->UpdateInPlace(
    {reinterpret_cast<u8 *>(&key), sizeof(key)},
    [&](std::span<u8> payload) { std::memcpy(payload.data(), &new_payload, sizeof(new_payload)); }, nullptr);
  ASSERT_EQ(ret, OpResult::OK);
  txn_man_->CommitTransaction();
}

}  // namespace leanstore::storage

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count = 12;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
