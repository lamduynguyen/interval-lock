#include "test/base_test.h"

#include "storage/btree/tree.h"
#include "storage/btree/wal.h"
#include "transaction/transaction_manager.h"

#include "gtest/gtest.h"

#include <thread>
#include <vector>

namespace leanstore::test {

using transaction::IsolationLevel;

/**
 * @brief Whether record-level serialization catches the pattern TPC-C's payment uses: LookUp the
 * customer, compute from the payload, and only afterwards write the precomputed value.
 *
 * N workers each commit the same increment 600 times: read `k` at the start of a transaction,
 * write `value + 1` a beat later, commit. Nothing aborts if it works as intended, so `k` must end
 * up at `N * 600`. If a worker's read slips past a concurrent transaction's write without being
 * caught, the last writer overwrites with an increment derived from a stale value.
 */
class TestLostUpdate : public BaseTest {
 protected:
  static constexpr u64 DEFAULT_LIMIT = 600;

  std::unique_ptr<storage::BTree> tree_;

  void SetUp() override {
    // 4 concurrent workers, so build the env for that
    FLAGS_worker_count = 6;
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), 0);
    catalog.push_back(tree_.get());
    txn_man_->CommitTransaction();
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  static auto AsKey(u64 k) -> std::vector<u8> { return {static_cast<u8>(k)}; }

  static auto Encode(u64 v) -> std::vector<u8> {
    std::vector<u8> out(sizeof(u64));
    std::memcpy(out.data(), &v, sizeof(u64));
    return out;
  }

  static auto Unwrap(std::span<const u8> payload) -> u64 {
    u64 out;
    std::memcpy(&out, payload.data(), sizeof(u64));
    return out;
  }

  /// Reads `k` in its own committed transaction (RU: whatever is committed is what we check).
  auto FinalValue() -> u64 {
    u64 v = 0;
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, IsolationLevel::NO_LOCK,
                               transaction::Transaction::Mode::OLTP);
    auto key = AsKey(KEY_ID);
    auto ret = tree_->LookUp(key, [&](std::span<const u8> payload) {
      v = Unwrap(payload);
      return true;
    });
    txn_man_->CommitTransaction();
    EXPECT_EQ(ret, OpResult::OK);
    return v;
  }

 public:
  /// One worker commits `left` increments; on ABORT_TX (record lock refused) it starts over.
  void RunWorker(wid_t wid, u64 left, IsolationLevel iso) {
    LeanStore::worker_thread_id = wid;
    log_->LocalLogWorker().backend.Connect();
    while (left > 0) {
      txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, iso, transaction::Transaction::Mode::OLTP);
      // The payment pattern: read here ...
      u64 v    = 0;
      auto key = AsKey(KEY_ID);
      auto ret = tree_->LookUp(key, [&](std::span<const u8> payload) { v = Unwrap(payload); });
      if (static_cast<int>(ret) != 0) {
        txn_man_->AbortTransaction();
        continue;
      }

      // ... and write a beat later, from the value captured above. Between the two ops, every
      // other worker's committed increment made the one this txn read stale, and serialization
      // must refuse to install the stale-based value.
      SpinDelay(50000);
      auto payload = Encode(v + 1);
      auto upd     = tree_->Update(key, payload, {});
      if (upd != OpResult::OK) {
        txn_man_->AbortTransaction();
        continue;  // died and must restart
      }
      txn_man_->CommitTransaction();
      left--;
    }
  }

 protected:
  static void SpinDelay(u32 cycles) {
    for (u32 i = 0; i < cycles; i++) { AsmYield(); }
  }

  static constexpr u64 KEY_ID = 99;
};

TEST_F(TestLostUpdate, ReadComputeWriteIncrementsMustNotBeLost) {
  {
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, IsolationLevel::NO_LOCK,
                               transaction::Transaction::Mode::OLTP);
    auto key     = AsKey(KEY_ID);
    auto payload = Encode(0);
    tree_->Insert(key, payload);
    txn_man_->CommitTransaction();
  }

  constexpr int WORKERS    = 4;
  const u64 EXPECTED_TOTAL = WORKERS * DEFAULT_LIMIT;
  std::vector<std::thread> workers;
  for (int w = 0; w < WORKERS; w++) {
    workers.emplace_back(&TestLostUpdate::RunWorker, this, static_cast<wid_t>(w + 1), static_cast<u64>(DEFAULT_LIMIT),
                         IsolationLevel::SERIALIZABLE);
  }
  for (auto &t : workers) { t.join(); }

  EXPECT_EQ(FinalValue(), EXPECTED_TOTAL);
}

}  // namespace leanstore::test
