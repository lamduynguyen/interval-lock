#include "transaction/lock_manager/precision_lock_manager.h"
#include "buffer/buffer_frame.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "sync/hybrid_guard.h"

#include <algorithm>
#include <cstdlib>
#include <new>
#include <stdexcept>

namespace leanstore::transaction {

// -------------------------------------------------------------------------------------
// RangeEntry lifecycle
// -------------------------------------------------------------------------------------
/// Fast inline memcmp over raw pointers, optimized for short keys (TPC-C: 4-12 bytes)
static __attribute__((always_inline)) inline auto RangeCmpFast(const u8 *a, size_t alen, const u8 *b, size_t blen)
  -> int {
  auto n   = (alen < blen) ? alen : blen;
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    u64 va;
    u64 vb;
    std::memcpy(&va, a + i, 8);
    std::memcpy(&vb, b + i, 8);
    if (va != vb) {
      for (size_t j = i; j < i + 8; j++) {
        if (a[j] != b[j]) { return a[j] < b[j] ? -1 : 1; }
      }
      __builtin_unreachable();  // chunks differ exactly at some byte
    }
  }
  for (; i < n; i++) {
    if (a[i] != b[i]) { return a[i] < b[i] ? -1 : 1; }
  }
  if (alen == blen) { return 0; }
  return alen < blen ? -1 : 1;
}

/// Bound compare over raw pointers, always_inlined: the walk calls it per entry.
static __attribute__((always_inline)) inline auto BoundCmpFast(const u8 *a, size_t alen, bool a_after, const u8 *b,
                                                               size_t blen, bool b_after) -> int {
  const auto c = RangeCmpFast(a, alen, b, blen);
  if (c != 0) { return c; }
  return static_cast<int>(a_after) - static_cast<int>(b_after);
}

/// Cheap range fingerprint; collisions are resolved by SameRange().
static auto RangeFingerprint(leng_t tree_id, const RangeBound &lo, const RangeBound &hi) -> fingerprint_t {
  constexpr fingerprint_t SEED = 0xC1C6C214179CC0F7ULL;
  auto h = SEED ^ tree_id ^ (static_cast<u64>(lo.key.size()) << 48) ^ (static_cast<u64>(hi.key.size()) << 32);
  h ^= static_cast<u64>(lo.key.front()) << 24;
  h ^= static_cast<u64>(lo.key.back()) << 16;
  h ^= static_cast<u64>(hi.key.front()) << 8;
  h ^= static_cast<u64>(hi.key.back());
  return HashFn(h);
}

/// Exact range equality, including both inclusivity bits.
static auto SameRange(const RangeEntry *entry, leng_t tree_id, timestamp_t txn_ts, const RangeBound &lo,
                      const RangeBound &hi) -> bool {
  return entry->tree_id == tree_id && entry->txn_ts == txn_ts &&
         RangeEntry::CmpBound(entry->Lo(), entry->LoAfter(), lo.key, !lo.inclusive) == 0 &&
         RangeEntry::CmpBound(entry->Hi(), entry->HiAfter(), hi.key, hi.inclusive) == 0;
}

auto RangeEntry::Cmp(std::span<const u8> a, std::span<const u8> b) -> int {
  auto common = std::min(a.size(), b.size());
  auto c      = std::memcmp(a.data(), b.data(), common);
  if (c != 0) { return c; }
  if (a.size() == b.size()) { return 0; }
  return a.size() < b.size() ? -1 : 1;
}

auto RangeEntry::CmpBound(std::span<const u8> a, bool a_after, std::span<const u8> b, bool b_after) -> int {
  const auto c = Cmp(a, b);
  if (c != 0) { return c; }
  return static_cast<int>(a_after) - static_cast<int>(b_after);
}

auto RangeEntry::Covers(const RangeBound &other_lo, const RangeBound &other_hi) const -> bool {
  if (BoundCmpFast(data, Lo().size(), LoAfter(), other_lo.key.data(), other_lo.key.size(), !other_lo.inclusive) > 0) {
    return false;
  }
  if (Hi().empty()) { return true; }  // my upper bound is +infinity
  if (other_hi.key.empty()) { return false; }
  return BoundCmpFast(other_hi.key.data(), other_hi.key.size(), other_hi.inclusive, Hi().data(), Hi().size(),
                      HiAfter()) <= 0;
}

auto RangeEntry::Overlaps(const RangeBound &other_lo, const RangeBound &other_hi) const -> bool {
  const bool my_lo_before_their_hi =
    other_hi.key.empty() ||
    BoundCmpFast(data, Lo().size(), LoAfter(), other_hi.key.data(), other_hi.key.size(), other_hi.inclusive) < 0;
  const bool their_lo_before_my_hi =
    Hi().empty() || BoundCmpFast(other_lo.key.data(), other_lo.key.size(), !other_lo.inclusive, Hi().data(),
                                 Hi().size(), HiAfter()) < 0;
  return my_lo_before_their_hi && their_lo_before_my_hi;
}

// -------------------------------------------------------------------------------------
// PrecisionLockManager lifecycle
// -------------------------------------------------------------------------------------
PrecisionLockManager::PrecisionLockManager(buffer::BufferManager *buffer, TraverseFunc traverse)
    : buffer_(buffer),
      traverse_(std::move(traverse)),
      policy_(static_cast<LockPolicy>(FLAGS_txn_locking_policy)),
      tlb_slot_cnt_(FLAGS_txn_lock_tlb_slots == 0 ? 0 : std::bit_ceil(FLAGS_txn_lock_tlb_slots)) {}

PrecisionLockManager::~PrecisionLockManager() {
  for (u32 w = 0; w < FLAGS_worker_count; ++w) { ReleaseStateEntries(local_states_[w]); }
}

// -------------------------------------------------------------------------------------
// Undo log: the write set exists purely for abort (i.e., restoring undo payload).
// Conflict detection should already be done before, i.e., by calling ::LockRange().
bool PrecisionLockManager::TryLockExclusive(timestamp_t, const LockableTuple *key) {
  auto &state  = Local();
  const u64 fp = LockableTuple::Fingerprint(key);
  if (state.write_set_.Find(fp) != nullptr) { return true; }
  state.write_set_.Add(UndoSlot{fp, NO_UNDO_OFF, 0});
  return true;
}

void PrecisionLockManager::SetUndoPayload(timestamp_t, const LockableTuple *key, std::span<const u8> payload) {
  auto &state  = Local();
  const u64 fp = LockableTuple::Fingerprint(key);
  auto *slot   = state.write_set_.Find(fp);
  if (slot == nullptr) { throw std::runtime_error("SetUndoPayload called without holding an exclusive lock"); }
  assert(slot->fingerprint == fp);
  const auto [off, len] = state.undo_arena_.Append(key, payload);
  slot->undo_off        = off;
  slot->undo_len        = len;
}

/// Does my entry already satisfy a claim for [lo, hi] in this mode?
static auto Satisfies(const RangeEntry *e, const RangeBound &lo, const RangeBound &hi, LockMode mode) -> bool {
  return e->Covers(lo, hi) && e->state.Granted() && (mode != LockMode::EXCLUSIVE || e->state.Exclusive());
}

/// Does the entry's key range stay entirely inside one page's (lower, upper] fence interval?
static auto WithinPage(const RangeEntry *e, std::span<const u8> lower, std::span<const u8> upper) -> bool {
  if (!lower.empty()) {
    const auto c = RangeEntry::Cmp(e->Lo(), lower);
    if (c < 0 || (c == 0 && !e->LoAfter())) { return false; }
  }
  if (upper.empty()) { return true; }
  return !e->Hi().empty() && RangeEntry::Cmp(e->Hi(), upper) <= 0;
}

// -------------------------------------------------------------------------------------
// Lock acquisition for both No-Wait and Wait-Die
auto PrecisionLockManager::LockRange(leng_t tree_id, pageid_t page_id, timestamp_t txn_ts, const RangeBound &lo,
                                     const RangeBound &hi, LockMode mode, u64 expected_page_state, bool scan) -> bool {
  if (Local().restoring_) { return true; }
  if (!buffer_->IsRunning()) { return false; }
  auto &state         = Local();
  const bool reusable = !lo.key.empty() && !hi.key.empty() && RangeEntry::Cmp(lo.key, hi.key) != 0;
  const auto range_fp = reusable ? RangeFingerprint(tree_id, lo, hi) : 0;

  RangeEntry *applied = nullptr;
  auto *tlb           = state.TlbSlot(page_id, tlb_slot_cnt_);
  for (;;) {
    if (!buffer_->IsRunning()) { return false; }
    // Fast-path TLB hit: the range request was already satisfied by the TLB entry of current page
    if (tlb != nullptr && tlb->epoch == state.tlb_epoch_ && Satisfies(tlb->entry, lo, hi, mode)) { return true; }

    RangeEntry *indexed = nullptr;
    if (reusable) {
      auto [begin, end] = state.range_index_.equal_range(range_fp);
      for (auto it = begin; it != end; ++it) {
        if (SameRange(it->second, tree_id, txn_ts, lo, hi)) {
          indexed = it->second;
          break;
        }
      }
    }
    auto &frame = buffer_->BufferFramePage(page_id);
    if (expected_page_state != 0) { __builtin_prefetch(&buffer_->GetPageState(page_id).StateAndVersion()); }
    // expected_page_state is only set during optimistic latch coupling,
    // so we need to validate the page version in case the page was splitted.
    if (expected_page_state != 0) {
      const u64 page_state = buffer_->GetPageState(page_id).StateAndVersion().load();
      if (sync::PageState::LockState(page_state) == sync::PageState::EXCLUSIVE ||
          sync::PageState::Version(page_state) != sync::PageState::Version(expected_page_state)) {
        throw sync::RestartException();
      }
    }
    // Prepare the entry before the actual lock acquisition
    RangeEntry *prepared     = MakeEntry(tree_id, page_id, txn_ts, lo, hi);
    RangeEntry *satisfied_by = nullptr;  // the lock ptr that may satisfy this lock request
    bool created             = false;    // `prepared` was linked, so it needs ownership bookkeeping
    bool refused             = false;    // this request dies instead of waiting
    bool must_wait           = false;    // this txn needs to wait or not

    // Latch the per-page range list, and iterate through the list, check possible conflicts,
    // and insert(or update/queue, depending on locking policy) the lock.
    // There is typically a few ranges per page, so simply hold X-latch and iterate through list.
    // It is unnecsary to:
    // - Implement complex data structure like skip list or interval tree
    // - Optimistic walk through the list
    {
      sync::HybridGuard guard(&frame.range_latch, sync::GuardMode::EXCLUSIVE);
      RangeEntry *covering_range_lock = nullptr;  // A larger range, also owned by current txn
      for (auto *e : frame.range_refs) {
        if (e->state.Tombstone()) { continue; }
        if (e->txn_ts == txn_ts) {
          if (e->Covers(lo, hi)) { covering_range_lock = e; }
          continue;
        }
        // Conflict = another txn's overlapping entry in a conflicting mode (both granted/waiting).
        if (!Conflicts(mode, e->state.Exclusive() ? LockMode::EXCLUSIVE : LockMode::SHARED) || !e->Overlaps(lo, hi)) {
          continue;
        }
        // Wait-die: an older conflict is a death verdict, the younger ones make us wait;
        //    waiting entries count too, so a younger txn cannot overtake an older waiter.
        // No-wait: any conflict is a death verdict, so nothing ever waits and must_wait stays false.
        if (e->txn_ts < txn_ts || policy_ == LockPolicy::NO_WAIT) {
          refused = true;
          break;
        }
        must_wait = true;
      }
      if (!refused) {
        // Try to detect a fixed-range scan, in which scenario we can use one interval entry to
        // be shared by all overlapping leaves.
        if (covering_range_lock == nullptr && indexed != nullptr) {
          covering_range_lock = indexed;
          frame.range_refs.push_back(indexed);
          indexed->page_id.store(INVALID_PAGE_ID, std::memory_order_release);
        }
        // Fast path only when this leaf is verifiably conflict-free: an early-promoted grant on
        // the covering entry must not override the conflict verdict of the current leaf.
        if (covering_range_lock != nullptr && !must_wait && Satisfies(covering_range_lock, lo, hi, mode)) {
          satisfied_by = covering_range_lock;
        } else {
          // Inserts/Upgrade-in-place a range entry, call once per Grant pass.
          // - covering_range_lock != nullptr means this txn already holds a prior range lock that covers
          //   the requested [const RangeBound &lo, const RangeBound &hi]
          // - must_wait is only set when policy_ == LockPolicy::WAIT_DIE and one of following is true:
          //   + this is a X lock request with a younger overlapping S or X
          //   + this is a S lock request with a younger overlapping X
          //   + a lock upgrade (S -> X) with a younger overlapping S or X -> requeue the lock upgrade
          if (covering_range_lock != nullptr) {
            // Read-then-write: upgrade in place (one entry per txn and range).
            covering_range_lock->state.SetMode(mode);
            if (policy_ == LockPolicy::WAIT_DIE && must_wait && covering_range_lock->state.Granted()) {
              // this lock upgrade conflicts with a younger overlapping entry, so reset the `granted` flag and re-queued
              covering_range_lock->state.SetGranted(false);
            }
            applied = covering_range_lock;
          } else {
            assert(prepared != nullptr);
            prepared->state.Init(static_cast<LockMode>(mode), !must_wait && !scan);
            frame.range_refs.push_back(prepared);
            applied = prepared;
            created = true;
          }
        }
      }
    }

    // Bookkeeping for lock cache and mem management
    if (!created) { RecycleEntry(prepared); }  // not linked: hand the slot back
    if (refused) { return false; }
    if (satisfied_by != nullptr) {
      if (tlb != nullptr) { *tlb = {state.tlb_epoch_, satisfied_by}; }
      return true;
    }
    if (created) {
      // Ownership bookkeeping is per-worker state; keep it out of the latched region.
      state.entries_.push_back(applied);
      if (reusable) { state.range_index_.emplace(range_fp, applied); }
    }
    if (tlb != nullptr) { *tlb = {state.tlb_epoch_, applied}; }
    if (!must_wait) { return true; }

    // Only wait-die reaches here, no-wait would return above under (must_wait == false).
    // Spinning until either:
    // - another transaction releases and PromoteWaiters grants our queued entry (applied), or
    // - the spin budget expires, so we re-evaluates the lock state and
    //   the death condition (a younger waiter can end up behind an older owner).
    assert(policy_ != LockPolicy::NO_WAIT);
    for (u64 spin = 0; spin < WAIT_SPINS && !applied->state.Granted() && buffer_->IsRunning(); ++spin) { AsmYield(); }
  }
}

void PrecisionLockManager::GrantScanRange(leng_t tree_id, timestamp_t txn_ts, const RangeBound &lo,
                                          const RangeBound &hi) {
  auto &state       = Local();
  const auto fp     = RangeFingerprint(tree_id, lo, hi);
  auto [begin, end] = state.range_index_.equal_range(fp);
  for (auto it = begin; it != end; ++it) {
    auto *entry = it->second;
    if (SameRange(entry, tree_id, txn_ts, lo, hi)) {
      entry->state.SetGranted(true);
      return;
    }
  }
}

void PrecisionLockManager::PromoteWaiters(buffer::BufferFrame &frame) {
  // The caller is this latch's exclusive owner; grants are decided under it.
  assert(frame.range_latch.LockState() == sync::HybridLatchMode::EXCLUSIVE);

  // No-wait never queues an entry, so the O(n^2) sweep below would be pure tax on every release.
  if (policy_ == LockPolicy::NO_WAIT) { return; }

  // For each queued entry, grant it unless a granted entry of another txn conflicts.
  // The scan is O(n^2), which is fine because the lists are usually short.
  // A second pass is unnecessary: a grant can only add more blockers (i.e., more strict blocking),
  //  so it never enables a grant this pass missed.
  // The waiters (i.e., other transactions), who are currently stuck in LockRange()::WAIT_SPINS,
  //  will be unblocked then and restart the locking evaluation.
  for (auto *e : frame.range_refs) {
    if (e->state.Tombstone() || e->state.Granted()) { continue; }
    auto blocked = false;
    for (auto *f : frame.range_refs) {
      if (blocked) { break; }
      if (f == e || f->state.Tombstone() || !f->state.Granted()) { continue; }
      if (f->txn_ts == e->txn_ts) { continue; }
      // An entry's bounds: an inclusive lo sits before its key, an inclusive hi after it.
      if (Conflicts(e->state.Exclusive() ? LockMode::EXCLUSIVE : LockMode::SHARED,
                    f->state.Exclusive() ? LockMode::EXCLUSIVE : LockMode::SHARED) &&
          f->Overlaps({e->Lo(), !e->LoAfter()}, {e->Hi(), e->HiAfter()})) {
        blocked = true;
      }
    }
    if (!blocked) { e->state.SetGranted(true); }
  }
}

// -------------------------------------------------------------------------------------
// Structural changes: split/merge carry the entries along with the keys.
// The page guards are exclusively latched by the caller; the frame latches below protect the ref lists.
// Lock order is page latches -> frame range latches (left page -> right page).
// Nothing takes a page latch while holding a range latch, so the two classes cannot deadlock.
void PrecisionLockManager::OnPageSplit(sync::ExclusiveGuard<storage::BTreeNode> &left,
                                       sync::ExclusiveGuard<storage::BTreeNode> &right, std::span<const u8> split_key) {
  // A range's refs are shared pointers, never copies of the entry.
  auto &left_frame  = buffer_->BufferFramePage(left.PageID());
  auto &right_frame = buffer_->BufferFramePage(right.PageID());
  sync::HybridGuard left_guard(&left_frame.range_latch, sync::GuardMode::EXCLUSIVE);
  sync::HybridGuard right_guard(&right_frame.range_latch, sync::GuardMode::EXCLUSIVE);
  size_t kept = 0;
  // The separator key stays left.
  // A range reaches the right page once it overlaps the separator key,
  // and keeps its left ref only while it also includes a key at or below it.
  for (auto *e : left_frame.range_refs) {
    if (e->state.Tombstone()) { continue; }
    const bool reaches_right = e->Hi().empty() || RangeEntry::CmpBound(e->Hi(), e->HiAfter(), split_key, true) > 0;
    if (!reaches_right) {
      left_frame.range_refs[kept++] = e;  // entirely left
      continue;
    }
    right_frame.range_refs.push_back(e);
    e->page_id.store(INVALID_PAGE_ID, std::memory_order_release);
    if (RangeEntry::CmpBound(e->Lo(), e->LoAfter(), split_key, false) <= 0) { left_frame.range_refs[kept++] = e; }
  }
  left_frame.range_refs.resize(kept);
  std::erase_if(left_frame.range_refs, [](RangeEntry *e) { return e->state.Tombstone(); });
  std::erase_if(right_frame.range_refs, [](RangeEntry *e) { return e->state.Tombstone(); });
  PromoteWaiters(left_frame);
  PromoteWaiters(right_frame);
}

void PrecisionLockManager::OnPageMerge(sync::ExclusiveGuard<storage::BTreeNode> &left,
                                       sync::ExclusiveGuard<storage::BTreeNode> &right) {
  // Merge: the left page's references move to the right page's list;
  auto &left_frame  = buffer_->BufferFramePage(left.PageID());
  auto &right_frame = buffer_->BufferFramePage(right.PageID());
  sync::HybridGuard left_guard(&left_frame.range_latch, sync::GuardMode::EXCLUSIVE);
  sync::HybridGuard right_guard(&right_frame.range_latch, sync::GuardMode::EXCLUSIVE);
  // A range may cover both left and right pages, so its ref may exist on both (see OnPageSplit()).
  // So, we should move only the refs if the right does not have it yet.
  for (auto *e : left_frame.range_refs) {
    if (e->state.Tombstone()) { continue; }
    auto &refs = right_frame.range_refs;
    if (std::find(refs.begin(), refs.end(), e) == refs.end()) { refs.push_back(e); }
  }
  left_frame.range_refs.clear();
  std::erase_if(right_frame.range_refs, [](RangeEntry *e) { return e->state.Tombstone(); });
  PromoteWaiters(right_frame);
  // All refs now live on the merged page.
  // Range entries whose range fits its fences can use the page-id
  // fast path lock release; wider ones stay invalid and fall back to the key-range scan.
  const auto lower = right->GetLowerFence();
  const auto upper = right->GetUpperFence();
  for (auto *e : right_frame.range_refs) {
    e->page_id.store(WithinPage(e, lower, upper) ? right.PageID() : INVALID_PAGE_ID, std::memory_order_release);
  }
}

// -------------------------------------------------------------------------------------
// Release
// -------------------------------------------------------------------------------------
// Entry pool: the shared segment pool, with a per-slot tail budget
auto PrecisionLockManager::MakeEntry(leng_t tree_id, pageid_t page_id, timestamp_t txn_ts, const RangeBound &lo,
                                     const RangeBound &hi) -> RangeEntry * {
  const auto lo_len = static_cast<u16>(lo.key.size());
  const auto hi_len = static_cast<u16>(hi.key.size());
  RangeEntry *entry;
  if (lo_len + hi_len > RangeEntry::MAX_ENTRY_SIZE) {  // oversized: exact-size allocation
    entry = static_cast<RangeEntry *>(std::malloc(sizeof(RangeEntry) + lo_len + hi_len));
    new (entry) RangeEntry{};
  } else {
    entry = pool_.Alloc();
  }
  entry->tree_id = tree_id;
  entry->page_id.store(page_id, std::memory_order_relaxed);
  entry->txn_ts = txn_ts;
  entry->lo_len = lo_len | (lo.inclusive ? RangeEntry::INCLUSIVE_BIT : 0);
  entry->hi_len = hi_len | (hi.inclusive ? RangeEntry::INCLUSIVE_BIT : 0);
  std::memcpy(entry->data, lo.key.data(), lo_len);
  std::memcpy(entry->data + lo_len, hi.key.data(), hi_len);
  return entry;
}

void PrecisionLockManager::RecycleEntry(RangeEntry *entry) {
  if (entry->Lo().size() + entry->Hi().size() > RangeEntry::MAX_ENTRY_SIZE) {
    std::free(entry);
    return;
  }
  pool_.Free(entry);
}

void PrecisionLockManager::UnlinkRef(buffer::BufferFrame &frame, RangeEntry *entry) {
  auto &refs = frame.range_refs;
  for (size_t idx = 0; idx < refs.size();) {  // remove every occurrence (lists are short)
    if (refs[idx] == entry) {
      refs[idx] = refs.back();
      refs.pop_back();
    } else {
      ++idx;
    }
  }
}

void PrecisionLockManager::ReleaseStateEntries(LocalState &state) {
  // Mark every entry dead first. Split/merge either skip tombstones or drop them after copying,
  // so they can never resurrect an entry while release is unlinking it.
  for (auto *e : state.entries_) { e->state.MarkTombstone(); }
  for (auto *e : state.entries_) {
    const auto pid   = e->page_id.load(std::memory_order_acquire);
    bool invalidated = pid == INVALID_PAGE_ID;
    // Common case: the page recorded at acquisition still owns the only ref. If the page was
    // reused meanwhile, its list no longer contains the entry and unlink is a no-op.
    if (!invalidated) {
      auto &frame = buffer_->BufferFramePage(pid);
      sync::HybridGuard guard(&frame.range_latch, sync::GuardMode::EXCLUSIVE);
      invalidated = e->page_id.load(std::memory_order_acquire) == INVALID_PAGE_ID;
      if (!invalidated) {
        UnlinkRef(frame, e);
        PromoteWaiters(frame);
      }
    }
    if (invalidated) {
      // A split/merge moved the ref; the key range is the only stable way back to its pages.
      traverse_(e->tree_id, e->Lo(), e->Hi(), [&](pageid_t pid) {
        auto &frame = buffer_->BufferFramePage(pid);
        sync::HybridGuard guard(&frame.range_latch, sync::GuardMode::EXCLUSIVE);
        UnlinkRef(frame, e);
        PromoteWaiters(frame);
      });
    }
    RecycleEntry(e);
  }
  state.entries_.clear();
  state.range_index_.clear();
}

auto PrecisionLockManager::Local() -> LocalState & {
  assert(LeanStore::worker_thread_id < FLAGS_worker_count);
  return local_states_[LeanStore::worker_thread_id];
}

bool PrecisionLockManager::EmptyLocalSet() {
  auto &state = Local();
  return state.write_set_.Empty() && state.entries_.empty();
}

void PrecisionLockManager::ReleaseAllLocks([[maybe_unused]] timestamp_t txn_ts, const WriteSetCallback &write_set_cb) {
  auto &state = Local();
  ++state.tlb_epoch_;  // invalidate every slot: an abort retry re-claims with the same ts
  UndoArena undo;
  undo.data_.swap(state.undo_arena_.data_);
  state.restoring_ = true;
  // Walk by index: the set has no iterator API, and it is cleared wholesale below, so nothing is
  // erased per entry. Each TLB slot is copied because the callback re-locks its key and
  // SetUndoPayload then rewrites that slot's undo fields in place.
  for (auto i = 0U; i != state.write_set_.Size(); ++i) {
    const auto &slot = state.write_set_.At(i);
    if (write_set_cb && slot.undo_off != NO_UNDO_OFF) {
      write_set_cb(undo.TupleAt(slot.undo_off), undo.PayloadAt(slot.undo_off, slot.undo_len));
    }
  }
  state.restoring_ = false;
  if (undo.data_.capacity() > state.undo_arena_.data_.capacity()) { undo.data_.swap(state.undo_arena_.data_); }
  state.undo_arena_.Clear();
  state.write_set_.Clear();
  ReleaseStateEntries(state);
}

auto PrecisionLockManager::LocalState::TlbSlot(pageid_t page_id, u64 slot_cnt) -> PageTlb * {
  if (tlb_ == nullptr) {
    if (slot_cnt == 0) { return nullptr; }
    tlb_ = new PageTlb[slot_cnt];
  }
  return &tlb_[page_id & (slot_cnt - 1)];
}

}  // namespace leanstore::transaction
