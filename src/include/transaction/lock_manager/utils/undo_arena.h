#pragma once

#include "common/typedefs.h"
#include "transaction/lockable_tuple.h"

#include <cstddef>
#include <span>
#include <utility>
#include <vector>

namespace leanstore::transaction {

/// No undo image yet for this held lock, filled later by SetUndoPayload
constexpr u32 NO_UNDO_OFF = 0xFFFFFFFFu;

/// Write-set row: undo image slot for one fingerprint
struct UndoSlot {
  fingerprint_t fingerprint;
  u32 undo_off;
  u32 undo_len;
};

/**
 * @brief Bump-pointer arena of LockableTuple-shaped undo records;
 * Append returns (record offset, payload length).
 */
struct UndoArena {
  std::vector<u8> data_;

  auto Append(const LockableTuple *key, std::span<const u8> payload) -> std::pair<u32, u32>;
  auto TupleAt(u32 off) -> const LockableTuple *;
  auto PayloadAt(u32 off, u32 len) -> std::span<const u8>;
  void Clear();
};

}  // namespace leanstore::transaction
