#include "transaction/lock_manager/inline_lock_manager.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "leanstore/config.h"
#include "leanstore/kv_interface.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "storage/btree/tree.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/shared_guard.h"
#include "transaction/lock_manager/utils/lock_tlb.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <stdexcept>
#include <vector>

namespace leanstore::transaction {

InlineLockManager::InlineLockManager(buffer::BufferManager *buffer)
    : local_states_(FLAGS_worker_count), buffer_(buffer) {
  // Only support No-wait for inline lock manager
  Ensure(static_cast<LockPolicy>(FLAGS_txn_locking_policy) == LockPolicy::NO_WAIT);
}

auto InlineLockManager::Local() -> LocalState & {
  assert(LeanStore::worker_thread_id < FLAGS_worker_count);
  return local_states_[LeanStore::worker_thread_id];
}

InlineLockManager::~InlineLockManager() {
  for (size_t w = 0; w < FLAGS_worker_count; ++w) {
    auto &state = local_states_[w];
    for (size_t i = state.held_.Size(); i > 0; --i) { ReleaseOne(state.held_.At(i - 1)); }
    state.held_.Clear();
    state.write_set_.Clear();
    state.undo_arena_.Clear();
  }
}

auto InlineLockManager::EmptyLocalSet() -> bool {
  auto &state = Local();
  return state.held_.Empty() && state.write_set_.Empty();
}

// -------------------------------------------------------------------------------------
/// The fp index answers in O(1); the byte check catches the (very rare) fp collision.
auto InlineLockManager::FindHeld(const LockableTuple *key) -> HeldLock * {
  auto &state  = Local();
  const u64 fp = LockableTuple::Fingerprint(key);
  auto *e      = state.held_.Find(fp);
  if (e != nullptr && e->tree_id == key->tree_id && e->key.size() == key->key_len &&
      std::memcmp(e->key.data(), key->key, key->key_len) == 0) {
    return e;
  }
  // collision: verify bytes entry by entry
  for (size_t i = 0; i < state.held_.Size(); ++i) {
    auto &cand = state.held_.At(i);
    if (cand.fingerprint == fp && cand.tree_id == key->tree_id && cand.key.size() == key->key_len &&
        std::memcmp(cand.key.data(), key->key, key->key_len) == 0) {
      return &cand;
    }
  }
  return nullptr;
}

auto InlineLockManager::AddUndo(const LockableTuple *key) -> void {
  auto &state   = Local();
  const auto fp = LockableTuple::Fingerprint(key);
  if (state.write_set_.Find(fp) != nullptr) { return; }
  // SetUndoPayload fills the real undo payload later.
  state.write_set_.Add(UndoSlot{fp, NO_UNDO_OFF, 0});
}

void InlineLockManager::SetUndoPayload(timestamp_t, const LockableTuple *key, std::span<const u8> payload) {
  auto &state = Local();
  auto *slot  = state.write_set_.Find(LockableTuple::Fingerprint(key));
  if (slot == nullptr) { throw std::runtime_error("SetUndoPayload called without holding an exclusive lock"); }
  const auto [off, len] = state.undo_arena_.Append(key, payload);
  slot->undo_off        = off;
  slot->undo_len        = len;
}

// -------------------------------------------------------------------------------------
// Record-granularity lock APIs
auto InlineLockManager::TryLockShared(timestamp_t, const LockableTuple *key, pageid_t page_id, leng_t slot_id) -> bool {
  // The caller holds the shared latch, so the record cannot move while we bump its shared
  // count. The claim lives until the txn ends.
  if (FindHeld(key) != nullptr) { return true; }  // one claim per txn and key: scan restarts re-enter here
  auto *node = reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(page_id));
  auto *word = node->GetLockByte(slot_id);
  if (word == nullptr) { return true; }  // a miss resolved here: no record, nothing to claim
  std::atomic_ref<u8> b(*word);
  if (b.fetch_add(1, std::memory_order_acquire) >= MAX_SHARED) {
    // Counting must never reach the exclusive value: roll back the bump, then refuse.
    b.fetch_sub(1, std::memory_order_relaxed);
    return false;  // no-wait refuses
  }
  Local().held_.Add(HeldLock{
    LockableTuple::Fingerprint(key), key->tree_id, page_id, slot_id, {key->key, key->key + key->key_len}, false});
  return true;
}

auto InlineLockManager::TryLockExclusive(timestamp_t, const LockableTuple *key) -> bool {
  // Prepare the write-set slot before the page latch: only the lock-byte flip needs the latch.
  AddUndo(key);
  return true;
}

auto InlineLockManager::TryLockExclusiveInPlace(timestamp_t, const LockableTuple *key, pageid_t page_id, leng_t slot_id)
  -> bool {
  auto *mine = FindHeld(key);
  if (mine != nullptr && mine->exclusive) { return true; }  // our earlier claim is already in the byte
  // The caller holds this page exclusively, so no reader claim can be inside: a plain store is safe.
  auto *node = reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(page_id));
  auto *word = node->GetLockByte(slot_id);
  if (word == nullptr) { return false; }
  std::atomic_ref<u8> b(*word);
  auto cur = b.load(std::memory_order_relaxed);

  if (cur >= 2) { return false; }  // co-readers: a write must not wipe their counts
  if (cur == 1) {
    // The one shared claimant has to be our own read-claim, held since earlier; refuse anything
    // else, so nobody else's count gets wiped.
    if (mine == nullptr || mine->exclusive) { return false; }
    mine->exclusive = true;
  }
  b.store(EXCLUSIVE_HELD, std::memory_order_release);
  if (mine == nullptr || !mine->exclusive) {
    Local().held_.Add(HeldLock{
      LockableTuple::Fingerprint(key), key->tree_id, page_id, slot_id, {key->key, key->key + key->key_len}, true});
  }
  return true;
}

void InlineLockManager::RegisterInsertedRecord(timestamp_t, const LockableTuple *key, leng_t tree_id, pageid_t page_id,
                                               leng_t slot_id) {
  auto &state   = Local();
  const auto fp = LockableTuple::Fingerprint(key);
  auto *slot    = state.write_set_.Find(fp);
  // A retry loop may have landed the row a second time; keep the first claim only.
  // TryLockExclusive already inserted a placeholder for this attempt; it becomes the empty
  // undo image here.
  if (slot == nullptr) {
    const auto [off, len] = state.undo_arena_.Append(key, {});
    state.write_set_.Add(UndoSlot{fp, off, len});
  } else if (slot->undo_off == NO_UNDO_OFF) {
    const auto [off, len] = state.undo_arena_.Append(key, {});
    slot->undo_off        = off;
    slot->undo_len        = len;
  } else {
    return;
  }
  state.held_.Add(HeldLock{fp, tree_id, page_id, slot_id, {key->key, key->key + key->key_len}, true});
}

// -------------------------------------------------------------------------------------
// Release
void InlineLockManager::ReleaseOne(HeldLock &held) {
  auto *tree = reinterpret_cast<storage::BTree *>(catalog[held.tree_id]);
  if (tree == nullptr) { return; }
  const bool exclusive = held.exclusive;

  // Fast path: the record is where we left it.
  bool released = false;
  if (exclusive) {
    // Hold X-latch so a fresh reader cannot bump before the zeroing.
    sync::ExclusiveGuard<storage::BTreeNode> node(buffer_, held.page_id);
    if (node->SlotMatchesKey(held.slot_id, held.key)) {
      auto *byte = node->GetLockByte(held.slot_id);
      if (byte != nullptr) { std::atomic_ref<u8>(*byte).store(UNLOCKED, std::memory_order_release); }
      released = true;
    }
  } else {
    // Decrease the shared count
    sync::SharedGuard<storage::BTreeNode> node(buffer_, held.page_id);
    if (node->SlotMatchesKey(held.slot_id, held.key)) {
      auto *byte = node->GetLockByte(held.slot_id);
      if (byte != nullptr) {
        auto cur = std::atomic_ref<u8>(*byte).load(std::memory_order_relaxed);
        if (cur >= 1 && cur <= MAX_SHARED) { std::atomic_ref<u8>(*byte).fetch_sub(1, std::memory_order_release); }
      }
      released = true;
    }
  }
  if (released) { return; }

  // The slot went stale: the record moved. Re-lookup the record.
  tree->UpdateTupleInlineLock(held.key, [&](leng_t slot, storage::BTreeNode &leaf) {
    auto *byte = leaf.GetLockByte(slot);
    if (byte != nullptr) {
      if (exclusive) {
        std::atomic_ref<u8>(*byte).store(UNLOCKED, std::memory_order_release);
      } else {
        auto cur = std::atomic_ref<u8>(*byte).load(std::memory_order_relaxed);
        if (cur >= 1 && cur <= MAX_SHARED) { std::atomic_ref<u8>(*byte).fetch_sub(1, std::memory_order_release); }
      }
    }
  });
}

void InlineLockManager::ReleaseAllLocks(timestamp_t, const WriteSetCallback &write_set_cb) {
  auto &state = Local();

  // Hand the undo images over before the callback: a nested write may re-lock and
  // append new undo records, and those must not move the bytes we are replaying.
  UndoArena undo;
  undo.data_.swap(state.undo_arena_.data_);
  const size_t num_write_slots = state.write_set_.Size();
  for (size_t i = 0; i != num_write_slots; ++i) {
    const UndoSlot slot = state.write_set_.At(i);
    if (write_set_cb && slot.undo_off != NO_UNDO_OFF) {
      write_set_cb(undo.TupleAt(slot.undo_off), undo.PayloadAt(slot.undo_off, slot.undo_len));
    }
  }

  state.write_set_.Clear();
  if (undo.data_.capacity() > state.undo_arena_.data_.capacity()) { undo.data_.swap(state.undo_arena_.data_); }
  state.undo_arena_.Clear();

  // Release the claims only after the restore above finishes: it rewrites those very records.
  for (size_t i = state.held_.Size(); i > 0; --i) { ReleaseOne(state.held_.At(i - 1)); }
  state.held_.Clear();
}

}  // namespace leanstore::transaction
