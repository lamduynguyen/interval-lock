#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

#include <cstring>
#include <functional>
#include <span>
#include <utility>

namespace leanstore::storage {

class BTree : public KVInterface {
 public:
  BTree(buffer::BufferManager *buffer_pool, u32 tree_slot);
  ~BTree() override = default;

  /* BTree config*/
  void SetComparisonOperator(ComparisonLambda cmp) override;

  /* All BTree operators */
  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> OpResult override;
  auto Insert(std::span<u8> key, std::span<const u8> payload) -> OpResult override;
  auto Remove(std::span<u8> key) -> OpResult override;
  auto Upsert(std::span<u8> key, std::span<const u8> payload) -> OpResult;
  auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult override;
  auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> OpResult override;
  auto ScanAscending(ScanRange range, const AccessRecordFunc &fn) -> OpResult override;
  auto ScanDescending(ScanRange range, const AccessRecordFunc &fn) -> OpResult override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;
  void UpdateTupleInlineLock(std::span<u8> key, const std::function<void(leng_t, BTreeNode &)> &fn);
  void VisitLeaves(std::span<const u8> lo, std::span<const u8> hi, const std::function<void(pageid_t)> &fn);

 private:
  /* Iterate all pages utilities */
  auto IterateAllNodes(sync::OptimisticGuard<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                       const std::function<u64(BTreeNode &)> &leaf_fn) -> u64;
  auto IterateUntils(sync::OptimisticGuard<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                     const std::function<bool(BTreeNode &)> &leaf_fn) -> bool;

  /* Find Leaf Node storing the key */
  auto FindLeafOptimisticWithParent(std::span<u8> key)
    -> std::pair<sync::OptimisticGuard<BTreeNode>, sync::OptimisticGuard<BTreeNode>>;
  auto FindLeafOptimistic(std::span<u8> key) -> sync::OptimisticGuard<BTreeNode>;
  auto LockLeaf(sync::OptimisticGuard<BTreeNode> &node, const ScanRange &range) -> bool;

  /* Split/Merge utilities */
  void TrySplit(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&node);
  void EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key);
  void TryMerge(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&left,
                sync::ExclusiveGuard<BTreeNode> &&right, leng_t left_pos);
  void EnsureUnderfullInnersForMerge(BTreeNode *to_merge);

  /* Insert/Update utilities */
  auto InsertIntoLeaf(sync::OptimisticGuard<BTreeNode> &parent, sync::OptimisticGuard<BTreeNode> &node,
                      std::span<u8> key, std::span<const u8> payload) -> OpResult;
  auto UpdateLeafEntry(sync::OptimisticGuard<BTreeNode> &parent, sync::OptimisticGuard<BTreeNode> &node, u16 slot_id,
                       std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult;

  /* Access record utility for scan. */
  template <typename PageGuard>
  inline auto AccessRecord(PageGuard &node, u64 pos, const AccessRecordFunc &fn, const std::span<u8> &bound,
                           bool bound_inclusive, bool isScanAsc) -> OpResult {
    u64 key_len = node->header.prefix_len + node->slots[pos].key_length;
    u8 key[key_len];
    std::memcpy(key, node->GetPrefix(), node->header.prefix_len);
    std::memcpy(key + node->header.prefix_len, node->GetKey(pos), node->slots[pos].key_length);
    auto key_span = std::span<u8>{key, key_len};
    const int c   = cmp_lambda_.func(key, bound.data(), std::min<u64>(key_len, bound.size()));
    if ((isScanAsc ? c > 0 : c < 0) || (!bound_inclusive && c == 0)) { return OpResult::STOP_SCAN; }

    // Concurrency Control
    if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::BASELINE_INLINE)) {
      // The inline claim upgrades under a shared latch;
      // An optimistic view alone cannot keep the lock idempotent.
      auto pid      = node.PageID();
      auto read_ret = false;
      {
        sync::SharedGuard<BTreeNode> latched(std::move(node));
        if (!latched.TryLockShared(metadata_slotid_, key_span, static_cast<leng_t>(pos))) { return OpResult::ABORT_TX; }
        read_ret = fn(key_span, latched->GetPayload(static_cast<leng_t>(pos)));
      }
      // Regain an optimistic view on the same page for the next record.
      node = sync::OptimisticGuard<BTreeNode>(buffer_, pid);
      return read_ret ? OpResult::OK : OpResult::STOP_SCAN;
    }
    // PRECISION_RANGE should already acquire range lock here.
    if (FLAGS_txn_locking_variant != static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
      if (!node.TryLockShared(metadata_slotid_, key_span, static_cast<leng_t>(pos))) { return OpResult::ABORT_TX; }
    }

    // Actual scan read
    auto payload = node->GetPayload(pos);
    auto ret     = fn(key_span, payload);
    node.ValidateOrRestart(false);
    return (ret) ? OpResult::OK : OpResult::STOP_SCAN;
  }

  /* Core properties */
  buffer::BufferManager *buffer_;
  leng_t metadata_slotid_;

  /* Comparison properties */
  ComparisonLambda cmp_lambda_{ComparisonOperator::MEMCMP, std::memcmp};
};

}  // namespace leanstore::storage
