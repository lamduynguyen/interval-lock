#pragma once

#include "common/typedefs.h"

#include <vector>

namespace leanstore::transaction {
/**
 * @brief Thread-local held-entry set: item vector + open-addressing index (fingerprint -> slot).
 *
 * Index is power-of-two, 0.5 load, tombstone probing.
 * Growth amortizes into the vector;
 * Clear() frees both buffers.
 * Entries either expose a `fingerprint_t` or are the fingerprint itself.
 */
template <class Entry>
class FingerprintSet {
 public:
  auto Size() const -> size_t;
  auto Empty() const -> bool;
  auto At(size_t idx) -> Entry &;
  auto Find(fingerprint_t fingerprint) -> Entry *;
  void Add(const Entry &entry);
  void Remove(Entry *entry);
  void Clear();

 private:
  struct IndexEntry {
    fingerprint_t fingerprint;
    u32 slot;  // NO_SLOT when empty, TOMBSTONE when deleted
    static constexpr u32 NO_SLOT   = 0xFFFFFFFFu;
    static constexpr u32 TOMBSTONE = 0xFFFFFFFEu;
  };

  auto SlotOf(fingerprint_t fingerprint) -> IndexEntry *;
  void RebuildIndex(size_t min_capacity);

  static constexpr float MAX_INDEX_LOAD = 0.5f;

  std::vector<Entry> items_;
  std::vector<IndexEntry> index_;  // power of two, may be empty
};

}  // namespace leanstore::transaction
