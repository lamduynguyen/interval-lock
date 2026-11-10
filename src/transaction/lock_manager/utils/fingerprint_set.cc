#include "transaction/lock_manager/utils/fingerprint_set.h"
#include "transaction/lock_manager/faster_lock_manager.h"
#include "transaction/lock_manager/inline_lock_manager.h"

#include <type_traits>

namespace leanstore::transaction {

template <class Entry>
static auto FingerprintOf(const Entry &entry) -> fingerprint_t {
  if constexpr (std::is_same_v<Entry, fingerprint_t>) {
    return entry;
  } else {
    return entry.fingerprint;
  }
}

static auto BucketOf(fingerprint_t fingerprint, size_t capacity) -> size_t {
  return (fingerprint * 0x9E3779B97F4A7C15ULL) & (capacity - 1);
}

template <class Entry>
auto FingerprintSet<Entry>::Size() const -> size_t {
  return items_.size();
}

template <class Entry>
auto FingerprintSet<Entry>::Empty() const -> bool {
  return items_.empty();
}

template <class Entry>
auto FingerprintSet<Entry>::At(size_t idx) -> Entry & {
  return items_[idx];
}

template <class Entry>
auto FingerprintSet<Entry>::SlotOf(fingerprint_t fingerprint) -> IndexEntry * {
  if (index_.empty()) { return nullptr; }
  const size_t mask = index_.size() - 1;
  auto bucket       = static_cast<size_t>(BucketOf(fingerprint, index_.size()));
  while (true) {
    auto &entry = index_[bucket];
    if (entry.slot == IndexEntry::NO_SLOT) { return nullptr; }  // miss
    if (entry.slot == IndexEntry::TOMBSTONE) {
      // deleted: keep probing
      bucket = (bucket + 1) & mask;
      continue;
    }
    if (entry.fingerprint == fingerprint) { return &entry; }  // hit
    bucket = (bucket + 1) & mask;
  }
}

template <class Entry>
void FingerprintSet<Entry>::RebuildIndex(size_t min_capacity) {
  size_t capacity = 64;
  while (capacity < min_capacity / MAX_INDEX_LOAD) { capacity <<= 1; }
  index_.assign(capacity, IndexEntry{0, IndexEntry::NO_SLOT});
  for (size_t slot = 0; slot < items_.size(); ++slot) {
    auto bucket = static_cast<size_t>(BucketOf(FingerprintOf(items_[slot]), capacity));
    while (index_[bucket].slot != IndexEntry::NO_SLOT) { bucket = (bucket + 1) & (capacity - 1); }
    index_[bucket] = IndexEntry{FingerprintOf(items_[slot]), static_cast<u32>(slot)};
  }
}

template <class Entry>
auto FingerprintSet<Entry>::Find(fingerprint_t fingerprint) -> Entry * {
  if (auto *entry = SlotOf(fingerprint); entry != nullptr) { return &items_[entry->slot]; }
  return nullptr;
}

template <class Entry>
void FingerprintSet<Entry>::Add(const Entry &entry) {
  items_.push_back(entry);
  const size_t load_floor = static_cast<size_t>(static_cast<float>(index_.size()) * MAX_INDEX_LOAD);
  if (items_.size() > load_floor) {
    RebuildIndex(items_.size());
  } else {
    auto bucket = static_cast<size_t>(BucketOf(FingerprintOf(entry), index_.size()));
    while (index_[bucket].slot != IndexEntry::NO_SLOT && index_[bucket].slot != IndexEntry::TOMBSTONE) {
      bucket = (bucket + 1) & (index_.size() - 1);
    }
    index_[bucket] = IndexEntry{FingerprintOf(entry), static_cast<u32>(items_.size() - 1)};
  }
}

template <class Entry>
void FingerprintSet<Entry>::Remove(Entry *entry) {
  const u32 dead_slot         = static_cast<u32>(entry - items_.data());
  const fingerprint_t dead_fp = FingerprintOf(*entry);
  const Entry moved           = items_.back();
  if (auto *slot = SlotOf(dead_fp); slot != nullptr) { slot->slot = IndexEntry::TOMBSTONE; }
  *entry = moved;
  items_.pop_back();
  if (auto *slot = SlotOf(FingerprintOf(moved)); slot != nullptr) { slot->slot = dead_slot; }
}

template <class Entry>
void FingerprintSet<Entry>::Clear() {
  items_.clear();
  std::vector<IndexEntry>().swap(index_);
  std::vector<Entry>().swap(items_);
}

template class FingerprintSet<FasterHeldLock>;
template class FingerprintSet<InlineHeldLock>;
template class FingerprintSet<fingerprint_t>;
template class FingerprintSet<UndoSlot>;

}  // namespace leanstore::transaction
