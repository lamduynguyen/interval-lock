#include "transaction/lock_manager/utils/undo_arena.h"

#include <cstring>
#include <utility>

namespace leanstore::transaction {

auto UndoArena::Append(const LockableTuple *key, std::span<const u8> payload) -> std::pair<u32, u32> {
  const size_t off = (data_.size() + alignof(leng_t) - 1) & ~(alignof(leng_t) - 1);
  const size_t rec = offsetof(LockableTuple, key) + key->key_len + payload.size();
  data_.resize(off + rec);
  auto *storage                                      = data_.data() + off;
  *reinterpret_cast<leng_t *>(storage)               = key->tree_id;
  *reinterpret_cast<u16 *>(storage + sizeof(leng_t)) = key->key_len;
  std::memcpy(storage + offsetof(LockableTuple, key), key->key, key->key_len);
  std::memcpy(storage + offsetof(LockableTuple, key) + key->key_len, payload.data(), payload.size());
  return {static_cast<u32>(off), static_cast<u32>(payload.size())};
}

auto UndoArena::TupleAt(u32 off) -> const LockableTuple * {
  return reinterpret_cast<const LockableTuple *>(data_.data() + off);
}

auto UndoArena::PayloadAt(u32 off, u32 len) -> std::span<const u8> {
  return std::span<const u8>(data_.data() + off + offsetof(LockableTuple, key) + TupleAt(off)->key_len, len);
}

void UndoArena::Clear() { data_.clear(); }

}  // namespace leanstore::transaction
