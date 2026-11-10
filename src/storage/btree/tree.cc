#include "storage/btree/tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "recovery/log_worker.h"
#include "storage/blob/blob_manager.h"
#include "transaction/lock_manager/precision_lock_manager.h"
#include "transaction/transaction.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <tuple>
#include <vector>

using leanstore::sync::DeferLog;
using leanstore::sync::ExclusiveGuard;
using leanstore::sync::OptimisticGuard;
using leanstore::sync::SharedGuard;
using leanstore::transaction::Transaction;

namespace leanstore::storage {

BTree::BTree(buffer::BufferManager *buffer_pool, u32 tree_slot) : buffer_(buffer_pool), metadata_slotid_(tree_slot) {
  if (!FLAGS_wal_enable_recovery) {
    ExclusiveGuard<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
    ExclusiveGuard<BTreeNode> root_page(buffer_, buffer_->AllocPage(meta_page->GetNewPagePID()));
    new (root_page.Ptr()) storage::BTreeNode(true);
    meta_page->roots[metadata_slotid_] = root_page.PageID();
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      auto &logger = transaction::Transaction::active_txn.LogWorker();
      logger.ReserveAllocNewPageLogEntry(root_page.PageID());
      logger.SubmitActiveLogEntry();
      GenerateWALNewRoot(logger, meta_page, metadata_slotid_, root_page.PageID());
      GenerateWALFreshPage(logger, root_page, true);
    }
  }
}

void BTree::SetComparisonOperator(ComparisonLambda cmp_op) { cmp_lambda_ = cmp_op; }

auto BTree::IterateAllNodes(OptimisticGuard<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                            const std::function<u64(BTreeNode &)> &leaf_fn) -> u64 {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->header.count; idx++) {
    auto child_pid = node->GetChild(idx);
    OptimisticGuard<BTreeNode> child(buffer_, child_pid);
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  OptimisticGuard<BTreeNode> child(buffer_, node->header.right_most_child);
  res += IterateAllNodes(child, inner_fn, leaf_fn);
  return res;
}

auto BTree::IterateUntils(OptimisticGuard<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                          const std::function<bool(BTreeNode &)> &leaf_fn) -> bool {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  auto res = inner_fn(*(node.Ptr()));
  if (!res) {
    for (auto idx = 0; idx < node->header.count; idx++) {
      OptimisticGuard<BTreeNode> child(buffer_, node->GetChild(idx));
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
      if (res) { break; }
    }
    if (!res) {
      OptimisticGuard<BTreeNode> child(buffer_, node->header.right_most_child);
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
    }
  }

  return res;
}

// -------------------------------------------------------------------------------------
auto BTree::FindLeafOptimisticWithParent(std::span<u8> key)
  -> std::pair<sync::OptimisticGuard<BTreeNode>, sync::OptimisticGuard<BTreeNode>> {
  OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                                  parent);

  while (node->IsInner()) {
    parent = std::move(node);
    node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
  }

  return {std::move(parent), std::move(node)};
}

auto BTree::FindLeafOptimistic(std::span<u8> key) -> OptimisticGuard<BTreeNode> {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  auto root_pid = meta->GetRoot(metadata_slotid_);
  OptimisticGuard<BTreeNode> node(buffer_, root_pid, meta);

  while (node->IsInner()) {
    auto next_pid = node->FindChild(key, cmp_lambda_);
    node          = OptimisticGuard<BTreeNode>(buffer_, next_pid, node);
  }
  return node;
}

/// Runs `fn(slot_id, node)` on the record for `key`, already holding its inline EXCLUSIVE lock.
/// Only work for BASELINE_LINE lock manager.
void BTree::UpdateTupleInlineLock(std::span<u8> key, const std::function<void(leng_t, BTreeNode &)> &fn) {
  for (int attempt = 0; attempt < 3; attempt++) {
    try {
      auto node = FindLeafOptimistic(key);
      ExclusiveGuard<BTreeNode> locked(std::move(node));
      bool found = false;
      auto slot  = locked->LowerBound(key, found, cmp_lambda_);
      if (!found) { continue; }
      fn(slot, *locked.Ptr());
      return;
    } catch (const sync::RestartException &) {}
  }
}

void BTree::TrySplit(ExclusiveGuard<BTreeNode> &&parent, ExclusiveGuard<BTreeNode> &&node) {
  // create new root if necessary
  if (parent.PageID() == METADATA_PAGE_ID) {
    auto meta_p = reinterpret_cast<MetadataPage *>(parent.Ptr());
    // Root node is full, alloc a new root
    ExclusiveGuard<BTreeNode> new_root(buffer_, buffer_->AllocPage(meta_p->GetNewPagePID()));
    new (new_root.Ptr()) storage::BTreeNode(false);
    new_root->header.right_most_child = node.PageID();
    if (FLAGS_wal_enable) {
      auto &logger = transaction::Transaction::active_txn.LogWorker();
      logger.ReserveAllocNewPageLogEntry(new_root.PageID());
      logger.SubmitActiveLogEntry();
      GenerateWALNewRoot(logger, parent, metadata_slotid_, new_root.PageID());
      GenerateWALFreshPage(logger, new_root, false, node.PageID());
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent                          = std::move(new_root);
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    auto new_pid = buffer_->GetMetadataPtr()->GetNewPagePID();
    ExclusiveGuard<BTreeNode> new_child(buffer_, buffer_->AllocPage(new_pid));
    new (new_child.Ptr()) storage::BTreeNode(!node->IsInner());
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(), new_child.PageID(), sep_info.slot,
                    {sep_key, sep_info.len}, cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL the new page allocation on the metadata page (no latch required)
      auto &logger = transaction::Transaction::active_txn.LogWorker();
      logger.ReserveAllocNewPageLogEntry(new_pid);
      logger.SubmitActiveLogEntry();
      // WAL new node
      GenerateWALNewPage(logger, new_child);
      // WAL separator to parent
      {
        auto &entry      = parent.PrepareWalEntry<WALInsertSep>(sep_info.len);
        entry.left_pid   = node.PageID();
        entry.right_pid  = new_child.PageID();
        entry.sep_length = sep_info.len;
        std::memcpy(entry.sep_key, sep_key, sep_info.len);
        logger.SubmitActiveLogEntry();
      }
      // WAL logical split
      {
        auto &entry = node.PrepareWalEntry<WALLogicalSplit>(0);
        entry.sep   = sep_info;
        if (!node->IsInner()) { entry.next_leaf_node = node->header.next_leaf_node; }
        logger.SubmitActiveLogEntry();
      }
    }

    // The keys above the separator now live on the new page, so we need to move the ranges to the new page too.
    // No-op for the tuple-granularity variants.
    if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
      reinterpret_cast<transaction::PrecisionLockManager *>(transaction::Transaction::active_txn.LockManager())
        ->OnPageSplit(node, new_child, {sep_key, sep_info.len});
    }
    return;
  }

  // must split parent to make space for separator, restart from root to do this
  node.Unlock();
  EnsureSpaceForSplit(parent.UnlockAndGetPtr(), {sep_key, sep_info.len});
}

void BTree::EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key) {
  assert(to_split->IsInner());
  while (true) {
    try {
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner() && (node.Ptr() != to_split)) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      if (node.Ptr() == to_split) {
        if (node->HasSpaceForKV(key.size(), sizeof(pageid_t))) {
          // someone else did split concurrently
          return;
        }

        ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        TrySplit(std::move(parent_locked), std::move(node_locked));
      }

      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

/** Try merging `left` into `right` node */
void BTree::TryMerge(ExclusiveGuard<BTreeNode> &&parent, ExclusiveGuard<BTreeNode> &&left,
                     ExclusiveGuard<BTreeNode> &&right, leng_t left_pos) {
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    // Merge all locks from left node with the right node
    if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
      reinterpret_cast<transaction::PrecisionLockManager *>(transaction::Transaction::active_txn.LockManager())
        ->OnPageMerge(left, right);
    }
    // TODO(XXX): Free page left.PageID()
    if (FLAGS_wal_enable) {
      auto &logger = transaction::Transaction::active_txn.LogWorker();
      // WAL parent remove slot
      {
        auto &entry   = parent.PrepareWalEntry<WALRemove>(0);
        entry.slot_id = left_pos;
        logger.SubmitActiveLogEntry();
      }
      // WAL new page -- it is simpler to implement, but probably slightly inefficient
      GenerateWALNewPage(logger, right);
    }
    if (parent->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
      left.Unlock();
      right.Unlock();
      // Parent node is underfull, try merge this inner node
      EnsureUnderfullInnersForMerge(parent.UnlockAndGetPtr());
    }
  }
}

void BTree::EnsureUnderfullInnersForMerge(BTreeNode *to_merge) {
  assert(to_merge->IsInner());
  auto rep_key = to_merge->GetUpperFence();
  while (true) {
    try {
      // TODO(XXX): Implement tree-level compression
      //  i.e. parent of parent is page 0 (i.e. metadata page) and we can compress the inner nodes
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner() && (node.Ptr() != to_merge)) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(rep_key, node_pos, cmp_lambda_), parent);
      }

      if (parent.PageID() != METADATA_PAGE_ID &&  // Root node can't be merged
          node.Ptr() == to_merge &&               // Found the correct node to be merged
          node_pos < parent->header.count &&      // Current node is not the right most child
          parent->header.count >= 1 &&            // Parent has more than one children
          (node->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL)  // Current node is underfull
      ) {
        // underfull
        auto right_pid =
          (node_pos < parent->header.count - 1) ? parent->GetChild(node_pos + 1) : parent->header.right_most_child;
        OptimisticGuard<BTreeNode> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
          ExclusiveGuard<BTreeNode> node_locked(std::move(node));
          ExclusiveGuard<BTreeNode> right_locked(std::move(right));
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      }
      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::InsertIntoLeaf(OptimisticGuard<BTreeNode> &parent, OptimisticGuard<BTreeNode> &node, std::span<u8> key,
                           std::span<const u8> payload) -> OpResult {
  if (!node->HasSpaceForKV(key.size(), payload.size())) { return OpResult::NEED_SPLIT; }

  /* Check if current key exists or not */
  bool found;
  auto _ = node->LowerBound(key, found, cmp_lambda_);
  if (found) { return OpResult::DUPLICATE; }

  /* Leaf node has enough space -> only latch leaf node */
  auto defer_log = DeferLog<BTreeNode>(); /* Alternative WAL cycle - automatically append log when the scope ends */
  {
    ExclusiveGuard<BTreeNode> node_locked(std::move(node));
    parent.ValidateOrRestart();

    /* Insert Key-Value & Generate the log entry */
    auto slot_id = node_locked->InsertKeyValue(key, payload, cmp_lambda_);
    if (FLAGS_wal_enable) { defer_log.Construct<WALInsert>(node_locked, key, payload); }
    // Inline variant: register the empty-image undo that makes an abort delete this row.
    node_locked.RegisterInsertedRecord(metadata_slotid_, key, slot_id);
  }

  return OpResult::OK;
}

auto BTree::UpdateLeafEntry(OptimisticGuard<BTreeNode> &parent, OptimisticGuard<BTreeNode> &node, u16 slot_id,
                            std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult {
  auto curr_payload = node->GetPayload(slot_id);
  if (payload.size() > curr_payload.size() && !node->HasSpaceForKV(key.size(), payload.size() - curr_payload.size())) {
    return OpResult::NEED_SPLIT;
  }

  // Taken while still optimistic: a wait-die manager may block  on the record,
  // and blocking must not hold this leaf's exclusive latch.
  if (!node.TryLockExclusive(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

  ExclusiveGuard<BTreeNode> node_locked(std::move(node));
  parent.ValidateOrRestart();
  // Inline: the word can only flip under this exclusive latch.
  if (!node_locked.TryLockExclusiveInPlace(metadata_slotid_, key, slot_id)) { return OpResult::ABORT_TX; }
  node_locked.SetUndoPayload(metadata_slotid_, curr_payload, key);
  if (FLAGS_wal_enable) {
    auto &logger  = transaction::Transaction::active_txn.LogWorker();
    auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
    entry.slot_id = slot_id;
    logger.SubmitActiveLogEntry();
  }
  if (func) { func(curr_payload); }
  node_locked->RemoveSlot(slot_id);
  slot_id = node_locked->InsertKeyValue(key, payload, cmp_lambda_);
  if (FLAGS_wal_enable) {
    auto &logger = transaction::Transaction::active_txn.LogWorker();
    GenerateWAL<ExclusiveGuard<BTreeNode>, WALInsert>(logger, node_locked, key, payload);
  }
  // --------------------------------------------------------------------------
  return OpResult::OK;  // success
}

// -------------------------------------------------------------------------------------
auto BTree::LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> OpResult {
  while (true) {
    try {
      OptimisticGuard<BTreeNode> node = FindLeafOptimistic(key);

      // The precision variant conflicts on page ranges instead of records, so a point read also
      // goes through the range claim.
      if (!node.LockRange(metadata_slotid_, {key}, {key}, transaction::LockMode::SHARED)) { return OpResult::ABORT_TX; }

      bool found;
      leng_t pos = node->LowerBound(key, found, cmp_lambda_);
      // Managers that lock the record need the slot; a key that is not there has nothing to lock.
      if (FLAGS_txn_locking_variant == static_cast<u32>(transaction::LockManagerVariant::BASELINE_INLINE)) {
        // The conversion validates the latch and takes the shared latch before the lock acq.
        SharedGuard<BTreeNode> latched(std::move(node));
        if (!found) { return OpResult::NOT_FOUND; }
        if (!latched.TryLockShared(metadata_slotid_, key, pos)) { return OpResult::ABORT_TX; }
        read_cb(latched->GetPayload(pos));
        return OpResult::OK;
      }
      if (FLAGS_txn_locking_variant != static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) {
        if (!node.TryLockShared(metadata_slotid_, key, pos)) { return OpResult::ABORT_TX; }
      }
      if (!found) { return OpResult::NOT_FOUND; }
      read_cb(node->GetPayload(pos));
      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Insert(std::span<u8> key, std::span<const u8> payload) -> OpResult {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      auto [parent, node] = FindLeafOptimisticWithParent(key);

      if (!node.TryLockExclusive(metadata_slotid_, key)) { return OpResult::ABORT_TX; }
      auto op_ret = InsertIntoLeaf(parent, node, key, payload);
      if (op_ret != OpResult::NEED_SPLIT) { return op_ret; }

      ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));
    } catch (const sync::RestartException &) {}
  }
}

// This API is mostly used for UNDO during transaction abort
// It has many duplicated code with Insert/Update -- we should refactor it later
auto BTree::Upsert(std::span<u8> key, std::span<const u8> payload) -> OpResult {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      auto [parent, node] = FindLeafOptimisticWithParent(key);

      // Lookup key
      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) {
        if (!node.TryLockExclusive(metadata_slotid_, key)) { return OpResult::ABORT_TX; }
        // Try to insert the key-value
        auto op_ret = InsertIntoLeaf(parent, node, key, payload);
        if (op_ret != OpResult::NEED_SPLIT) { return op_ret; }
      } else {
        auto op_ret = UpdateLeafEntry(parent, node, slot_id, key, payload, {});
        if (op_ret != OpResult::NEED_SPLIT) { return op_ret; }
      }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Remove(std::span<u8> key) -> OpResult {
  while (true) {
    try {
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
      }

      // Lookup key
      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      // Concurrency control
      auto payload = node->GetPayload(slot_id);
      // Taken while the guard is optimistic.
      if (!node.TryLockExclusive(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

      // Removal
      leng_t entry_size = node->slots[slot_id].key_length + payload.size();
      if ((node->FreeSpaceAfterCompaction() + entry_size >=
           BTreeNodeHeader::SIZE_UNDER_FULL) &&     // new node is under full
          (parent.PageID() != METADATA_PAGE_ID) &&  // current node is not the root node
          (parent->header.count >= 2) &&            // parent has more than one children
          ((node_pos + 1) < parent->header.count)   // current node has a right sibling
      ) {
        // underfull
        ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        ExclusiveGuard<BTreeNode> right_locked(buffer_, parent_locked->GetChild(node_pos + 1));

        // Inline: the word can only flip under this exclusive latch.
        if (!node_locked.TryLockExclusiveInPlace(metadata_slotid_, key, slot_id)) { return OpResult::ABORT_TX; }
        node_locked.SetUndoPayload(metadata_slotid_, node_locked->GetPayload(slot_id), key);
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &logger  = transaction::Transaction::active_txn.LogWorker();
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          logger.SubmitActiveLogEntry();
        }
        // --------------------------------------------------------------------------
        // right child is also under full
        if (right_locked->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      } else {
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        // Inline: the word can only flip under this exclusive latch.
        if (!node_locked.TryLockExclusiveInPlace(metadata_slotid_, key, slot_id)) { return OpResult::ABORT_TX; }
        node_locked.SetUndoPayload(metadata_slotid_, node_locked->GetPayload(slot_id), key);
        // Update by remove then insert
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &logger  = transaction::Transaction::active_txn.LogWorker();
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          logger.SubmitActiveLogEntry();
        }
        // --------------------------------------------------------------------------
      }
      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

/**
 * @brief Atomically update the key-payload pair
 * Return true/false whether the key exists and is updated successfully
 *
 * If `func` is provided, then func(previous payload) is triggered
 */
auto BTree::Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      auto [parent, node] = FindLeafOptimisticWithParent(key);

      // Lookup key
      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      // Try update
      auto op_ret = UpdateLeafEntry(parent, node, slot_id, key, payload, func);
      if (op_ret != OpResult::NEED_SPLIT) { return op_ret; }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> OpResult {
  while (true) {
    try {
      auto node = FindLeafOptimistic(key);
      bool found;
      auto pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }
      if (!node.TryLockExclusive(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

      /* Alternative WAL cycle - automatically append WAL entry when the scope ends */
      auto defer_log = DeferLog<BTreeNode>();
      {
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        auto payload = node_locked->GetPayload(pos);
        // Inline: the word can only flip under this exclusive latch.
        if (!node_locked.TryLockExclusiveInPlace(metadata_slotid_, key, pos)) { return OpResult::ABORT_TX; }
        node_locked.SetUndoPayload(metadata_slotid_, payload, key);
        /* Modify the record, and store the after-value */
        func(payload);
        if (FLAGS_wal_enable && delta != nullptr) { delta->UpdateDeltaPayload(payload); }

        /* Generate the delta record */
        if (FLAGS_wal_enable) {
          if (delta != nullptr) {
            defer_log.Construct<WALDeltaImage>(node_locked, key,
                                               std::span<u8>(reinterpret_cast<u8 *>(delta), delta->size));
          } else {
            defer_log.Construct<WALAfterImage>(node_locked, key, payload);
          }
        }
      }

      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

/// Runs `fn(page_id)` for every leaf whose key range intersects [lo, hi], in ascending order,
/// under an optimistic guard and no page latch; an empty `hi` (+infinity) runs to the chain end.
/// Restarts on concurrent splits/merges, so `fn` must be idempotent.
void BTree::VisitLeaves(std::span<const u8> lo, std::span<const u8> hi, const std::function<void(pageid_t)> &fn) {
  // Boundary of the next leaf, copied out of the page while its view is valid.
  // A restart re-traverses here instead of redoing the scan.
  // The `fn` needs to be idempotent-safe due to optimistic restart.
  std::vector<u8> seek(lo.begin(), lo.end());
  while (true) {
    try {
      auto node = FindLeafOptimistic(seek);
      while (true) {
        fn(node.PageID());
        if (!node->header.HasRightNeighbor()) { return; }
        const auto upper = node->GetUpperFence();
        // Stopping only on a strictly greater fence
        if (!hi.empty() && cmp_lambda_.func(upper.data(), hi.data(), std::min<u64>(upper.size(), hi.size())) > 0) {
          return;
        }
        seek.assign(upper.begin(), upper.end());
        node = OptimisticGuard<BTreeNode>(buffer_, node->header.next_leaf_node);
      }
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::LockLeaf(OptimisticGuard<BTreeNode> &node, const ScanRange &range) -> bool {
  assert(!range.lo.empty() || !range.hi.empty());
  if (FLAGS_txn_locking_variant != static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE)) { return true; }

  // A fixed-range scan is one interval lock; PLM reuses it across all overlapping leaves.
  if (!range.lo.empty() && !range.hi.empty()) {
    auto lo = transaction::RangeBound{range.lo, true};
    auto hi = transaction::RangeBound{range.hi, true};
    return node.LockRange(metadata_slotid_, lo, hi, transaction::LockMode::SHARED,
                          (transaction::RangeEntry::Cmp(range.lo, range.hi) != 0));
  }

  auto page_lo = node->GetLowerFence();
  auto page_hi = node->GetUpperFence();
  auto lo      = (!range.lo.empty() && (page_lo.empty() || cmp_lambda_.func(range.lo.data(), page_lo.data(),
                                                                            std::min(range.lo.size(), page_lo.size())) > 0))
                   ? transaction::RangeBound{range.lo, range.lo_inclusive}
                   : transaction::RangeBound{page_lo, false};
  auto hi      = (!range.hi.empty() && (page_hi.empty() || cmp_lambda_.func(page_hi.data(), range.hi.data(),
                                                                            std::min(page_hi.size(), range.hi.size())) > 0))
                   ? transaction::RangeBound{range.hi, range.hi_inclusive}
                   : transaction::RangeBound{page_hi, true};
  return node.LockRange(metadata_slotid_, lo, hi, transaction::LockMode::SHARED);
}

// Short-cut: every normal scan termination finalizes the interval lock and returns OK.
static auto FinalizeScanRange(leng_t metadata_slotid, const ScanRange &range) -> OpResult {
  if (FLAGS_txn_locking_variant != static_cast<u32>(transaction::LockManagerVariant::PRECISION_RANGE) ||
      range.lo.empty() || range.hi.empty() || transaction::RangeEntry::Cmp(range.lo, range.hi) == 0) {
    return OpResult::OK;
  }
  auto *lock_manager =
    reinterpret_cast<transaction::PrecisionLockManager *>(transaction::Transaction::active_txn.LockManager());
  lock_manager->GrantScanRange(metadata_slotid, transaction::Transaction::active_txn.start_ts,
                               {range.lo, range.lo_inclusive}, {range.hi, range.hi_inclusive});
  return OpResult::OK;
}

auto BTree::ScanAscending(ScanRange range, const AccessRecordFunc &fn) -> OpResult {
  while (true) {
    try {
      auto node = FindLeafOptimistic(range.lo);
      bool found;
      auto pos = node->LowerBound(range.lo, found, cmp_lambda_);
      if (!range.lo.empty() && !range.lo_inclusive && found) { pos++; }
      if (!LockLeaf(node, range)) { return OpResult::ABORT_TX; }
      while (true) {
        if (pos < node->header.count) {
          auto op_ret = AccessRecord(node, pos, fn, range.hi, range.hi_inclusive, true);
          if (op_ret == OpResult::ABORT_TX) { return op_ret; }
          if (op_ret == OpResult::STOP_SCAN) { return FinalizeScanRange(metadata_slotid_, range); }
          pos++;
        } else {
          if (!node->header.HasRightNeighbor()) { return FinalizeScanRange(metadata_slotid_, range); }
          if (!range.hi.empty() && transaction::RangeEntry::Cmp(node->GetUpperFence(), range.hi) >= 0) {
            return FinalizeScanRange(metadata_slotid_, range);
          }
          pos  = 0;
          node = OptimisticGuard<BTreeNode>(buffer_, node->header.next_leaf_node);
          if (!LockLeaf(node, range)) { return OpResult::ABORT_TX; }
        }
      }
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::ScanDescending(ScanRange range, const AccessRecordFunc &fn) -> OpResult {
  while (true) {
    try {
      // a descending scan starts at the top of its range and walks down to `lo`
      auto node = FindLeafOptimistic(range.hi);
      if (!LockLeaf(node, range)) { return OpResult::ABORT_TX; }
      bool found;
      int pos = static_cast<int>(node->LowerBound(range.hi, found, cmp_lambda_));
      if (!found || !range.hi_inclusive) { pos--; }
      while (true) {
        while (pos >= 0) {
          if (pos < node->header.count) {
            auto op_ret = AccessRecord(node, pos, fn, range.lo, range.lo_inclusive, false);
            if (op_ret == OpResult::ABORT_TX) { return op_ret; }
            if (op_ret == OpResult::STOP_SCAN) { return FinalizeScanRange(metadata_slotid_, range); }
          }
          pos--;
        }
        if (node->header.IsLowerFenceInfinity()) { return FinalizeScanRange(metadata_slotid_, range); }
        if (!range.lo.empty() && transaction::RangeEntry::Cmp(node->GetLowerFence(), range.lo) <= 0) {
          return FinalizeScanRange(metadata_slotid_, range);
        }
        node = FindLeafOptimistic(node->GetLowerFence());
        if (!LockLeaf(node, range)) { return OpResult::ABORT_TX; }
        pos = node->header.count - 1;
      }
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::CountEntries() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  auto root_pid = meta->GetRoot(metadata_slotid_);
  OptimisticGuard<BTreeNode> node(buffer_, root_pid, meta);

  return IterateAllNodes(
    node, [](BTreeNode &) { return 0; },
    [](BTreeNode &node) {
      auto ret = node.header.count;
      for (auto idx = 0; idx < node.header.count; idx++) {
        if (node.slots[idx].payload_length == 0) { ret--; }
      }
      return ret;
    });
}

// -------------------------------------------------------------------------------------

auto BTree::IsNotEmpty() -> bool {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateUntils(
    node, [](BTreeNode &) { return false; }, [](BTreeNode &node) { return (node.header.count > 0); });
}

auto BTree::CountPages() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(node, [](BTreeNode &) { return 1; }, [](BTreeNode &) { return 1; });
}

auto BTree::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

}  // namespace leanstore::storage
