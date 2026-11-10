#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/format.h"
#include "common/rand.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "storage/aio.h"
#include "storage/blob/blob_state.h"
#include "storage/btree/node.h"

#include "cpptrace/cpptrace.hpp"
#include "fmt/ranges.h"
#include "share_headers/logger.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <cassert>

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <utility>
#include <vector>

using leanstore::LeanStore;
using leanstore::storage::ExtentList;

/**
 * @brief There is no need to call GetPageState(pid).Init() during page/extent allocation
 *    because the page state will be automatically X-lock acquired
 */
#define NEW_ALLOC_PAGE(pid)                     \
  ({                                            \
    auto &ps     = GetPageState(pid);           \
    u64 v        = ps.StateAndVersion().load(); \
    bool success = ps.TryLockExclusive(v);      \
    Ensure(success);                            \
    if (!IsExtent(pid)) {                       \
      frame_[pid].Init();                       \
    } else {                                    \
      ext_frame_[pid].Init();                   \
    }                                           \
  })

#define PAGE_FAULT(pid, page_cnt)     \
  ({                                  \
    physical_used_cnt_ += (page_cnt); \
    EnsureFreePages();                \
  })

namespace leanstore::buffer {

BufferManager::BufferManager(std::atomic<bool> &keep_running)
    : virtual_size_(FLAGS_bm_virtual_gb * GB),
      physical_size_(FLAGS_bm_physical_gb * GB),
      alias_size_(FLAGS_bm_alias_block_mb * MB),
      virtual_cnt_(virtual_size_ / PAGE_SIZE),
      physical_cnt_(physical_size_ / PAGE_SIZE),
      alias_pg_cnt_(alias_size_ / PAGE_SIZE),
      evict_batch_(FLAGS_bm_evict_batch_size),
      keep_running_(&keep_running),
      resident_set_(physical_cnt_),
      max_chkpted_in_shard_(FLAGS_wal_checkpoint_shards),
      extent_virtual_size_((FLAGS_blob_db_region_gb == 0) ? virtual_size_ : FLAGS_blob_db_region_gb * GB),
      extent_virtual_cnt_(extent_virtual_size_ / PAGE_SIZE) {
  Construction();
}

BufferManager::BufferManager(u64 virtual_page_count, u64 physical_page_count, u64 extra_page_count, u64 evict_size,
                             std::atomic<bool> &keep_running)
    : virtual_size_(virtual_page_count * PAGE_SIZE),
      physical_size_(physical_page_count * PAGE_SIZE),
      alias_size_(extra_page_count * PAGE_SIZE),
      virtual_cnt_(virtual_page_count),
      physical_cnt_(physical_page_count),
      alias_pg_cnt_(extra_page_count),
      evict_batch_(evict_size),
      keep_running_(&keep_running),
      resident_set_(physical_cnt_),
      max_chkpted_in_shard_(FLAGS_wal_checkpoint_shards),
      extent_virtual_size_((FLAGS_blob_db_region_gb == 0) ? virtual_size_ : FLAGS_blob_db_region_gb * GB),
      extent_virtual_cnt_(extent_virtual_size_ / PAGE_SIZE) {
  Construction();
}

BufferManager::~BufferManager() { close(blockfd_); }

/**
 * @brief Format of the virtual memory buffer pool is as followed:
 *
 * The virtual memory area comprises of three main components
 *
 * - Main buffer manager (virtual_size_ + PAGE_SIZE):
 *  + bm:   the buffer pool manager (vmcache)
 *  + exp:  extra page, to prevent segfaults during optimistic reads
 *
 * - Extent buffer manager for extent accesses/alloc
 *    * random+huge: 1 huge on-disk region of the database file, defined by FLAGS_blob_db_region_gb
 *          During allocation, we randomly pick a free range (synchronized via a range lock) and
 *          assign that PID to the extent. The extent PIDs live in the on-disk region
 *            |--------------------------------|
 *            | FLAGS_blob_db_region_gb * GB   |
 *            |--------------------------------|
 *
 * - Aliasing area (alias_size_ * FLAGS_worker_count + virtual_size_):
 *  + wl:     N worker-local aliasing area for N workers
 *  + shalar: shared-area for aliasing, same size with the main buffer pool
 *
 * The virtual memory area, therefore, looks like the following figure:
 *  |--------------------|-----|-----------|----|----|----|----|--------|
 *  | bm (virtual_size_) | exp | extent-bm | wl | wl |....| wl | shalar |
 *  |--------------------|-----|-----------|----|----|----|----|--------|
 *
 * Note that ptrs belong to `wl` and `shalar` are considered INVALID by all components except BlobManager
 * All of the addresses should be PAGE_SIZE aligned
 */
void BufferManager::Construction() {
  auto total_pg = virtual_cnt_ + extent_virtual_cnt_;

  // Setup disk
  assert(virtual_size_ >= physical_size_);
  blockfd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
  if (blockfd_ < 0) {
    blockfd_ = open(FLAGS_db_path.c_str(), O_CREAT | O_RDWR | O_DIRECT, S_IRWXU);
    assert(blockfd_ >= 0);
    // DB + buffer + WAL region + WAL master record
    auto file_size = total_pg * PAGE_SIZE + 1UL * GB + FLAGS_wal_size_gb * GB + BLK_BLOCK_SIZE;
    assert(file_size % BLK_BLOCK_SIZE == 0);
    LOG_INFO("Allocate file {} -- size {}", FLAGS_db_path.c_str(), file_size);
    [[maybe_unused]] auto ret = ftruncate(blockfd_, file_size);
    assert(ret >= 0);
  }
  Ensure(blockfd_ >= 0);

  // Setup virtual memory for LeanStore
  auto virtual_alloc_size =
    virtual_size_ + PAGE_SIZE + extent_virtual_size_ + alias_size_ * FLAGS_worker_count + virtual_size_;

  // Workers + 1 Checkpoint thread (group commit runs inline on the workers)
  auto no_interfaces = FLAGS_worker_count + 1;
  mem_               = static_cast<storage::Page *>(
    mmap(nullptr, virtual_alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  if (mem_ == MAP_FAILED) { throw leanstore::ex::GenericException("mmap failed"); };

  // Free storage manager for extent management
  free_storage_ = std::make_unique<storage::FreeStorageManager>(virtual_cnt_ + 1, extent_virtual_cnt_);

  // Construct AIO interfaces for writing pages
  aio_interface_.reserve(no_interfaces);
  for (size_t idx = 0; idx < no_interfaces; idx++) { aio_interface_.emplace_back(blockfd_, mem_); }

  // Setup page run-time
  page_state_ = static_cast<sync::PageState *>(AllocHuge((total_pg) * sizeof(sync::PageState)));
  /* A cold pool has nothing resident: every page starts EVICTED (version 0).
   * ReadExtents only reads EVICTED pages, and a raw memset would corrupt the
   * version, so use the encoded constant. */
  std::fill_n(reinterpret_cast<u64 *>(page_state_), total_pg, INITIAL_PAGE_STATE);
  frame_     = static_cast<BufferFrame *>(AllocHuge((virtual_cnt_ + 1) * sizeof(BufferFrame)));
  ext_frame_ = static_cast<ExtentFrame *>(AllocHuge((total_pg) * sizeof(ExtentFrame)));
  area_      = std::make_unique<AliasingArea>(alias_size_, virtual_size_, &mem_[total_pg + 1]);
  Ensure(page_state_ != MAP_FAILED && frame_ != MAP_FAILED);

  // Statistics and summary
  physical_used_cnt_ = 0;
  LOG_INFO(
    "BufferPool: Path({}), AllocSizeGB({}), VirtualGB({}), VirtualCount({}), PhysGB({}), PhysCount({}), "
    "EvictSize({})",
    FLAGS_db_path.c_str(), virtual_alloc_size / GB, virtual_size_ / GB, virtual_cnt_, physical_size_ / GB,
    physical_cnt_, evict_batch_);
}

// ------------------------------------------------------------------------------------------------------

// Alloc page 0 for metadata
void BufferManager::AllocMetadataPage() {
  AllocPage(METADATA_PAGE_ID);
  reinterpret_cast<storage::MetadataPage *>(ToPtr(METADATA_PAGE_ID))->alloc_cnt = 1;  // page 0
  GetPageState(METADATA_PAGE_ID).UnlockExclusive();
}

auto BufferManager::AliasArea() -> AliasingArea * { return area_.get(); }

auto BufferManager::FreeStorageManager() -> storage::FreeStorageManager * { return free_storage_.get(); }

void BufferManager::ConstructLocalRing() {
  int ret = io_uring_queue_init(URING_NO_ENTRIES, &(aio_interface_[LeanStore::worker_thread_id].Ring()),
                                storage::LibaioInterface::uring_flags);
  if (ret != 0) { throw ex::EnsureFailed("BufferManager: io_uring_queue_init error"); }
}

auto BufferManager::RetrieveLocalRing() -> struct io_uring & {
  return aio_interface_[LeanStore::worker_thread_id].Ring();
}

// ------------------------------------------------------------------------------------------------------

auto BufferManager::GetPageState(pageid_t page_id) -> sync::PageState & { return page_state_[page_id]; }

void BufferManager::ValidatePID(pageid_t pid) {
  if (pid >= virtual_cnt_ + extent_virtual_cnt_) {
    throw std::runtime_error("Page id " + std::to_string(pid) + " is invalid");
  }
}

auto BufferManager::IsValidPtr(void *page) -> bool {
  return (page >= mem_) && (page < (mem_ + virtual_size_ + PAGE_SIZE + extent_virtual_size_));
}

auto BufferManager::ToPID(void *page) -> pageid_t { return reinterpret_cast<storage::Page *>(page) - mem_; }

auto BufferManager::GetMetadataPtr() -> storage::MetadataPage * {
  return reinterpret_cast<storage::MetadataPage *>(ToPtr(METADATA_PAGE_ID));
}

auto BufferManager::IsExtent(pageid_t page_id) -> bool { return page_id >= virtual_cnt_ + 1; }

auto BufferManager::PageIsDirty(pageid_t page_id) -> bool {
  return frame_[page_id].last_written_gsn.load() < ToPtr(page_id)->p_gsn;
}

auto BufferManager::BufferFramePage(pageid_t page_id) -> buffer::BufferFrame & { return frame_[page_id]; }

/**
 * @brief Chunk operation to simulate overheads of normal Buffer Manager
 */
void BufferManager::ChunkOperation(pageid_t start_pid, u64 no_bytes,
                                   const std::function<void(u64, std::span<u8>)> &func) {
  Ensure(FLAGS_blob_normal_buffer_pool);
  auto offset = 0UL;
  while (offset < no_bytes) {
    auto to_op_size = std::min(no_bytes - offset, PAGE_SIZE);
    auto to_op_pid  = start_pid + offset / PAGE_SIZE;
    func(offset, {reinterpret_cast<u8 *>(ToPtr(to_op_pid)), to_op_size});
    offset += to_op_size;
  }
  Ensure(offset == no_bytes);
}

// ------------------------------------------------------------------------------------------------------

void BufferManager::EnsureFreePages() {
  while ((physical_used_cnt_ >= physical_cnt_ * 0.9) && (keep_running_->load())) { Evict(); }
}

/**
 * @brief Allocate a single page
 * Always allocate the page at the end of database for simplicity
 */
auto BufferManager::AllocPage(pageid_t new_pid) -> storage::Page * {
  ValidatePID(new_pid);
  PAGE_FAULT(new_pid, 1);
  NEW_ALLOC_PAGE(new_pid);
  resident_set_.Insert(new_pid);
  mem_[new_pid].p_gsn = 1;
  assert(PageIsDirty(new_pid));
  return ToPtr(new_pid);
}

/**
 * @brief Try to ensure that there are free frames (i.e. virtual mem in vmcache) in the buffer manager.
 *        After that, read the page and load it into the buffer pool
 */
void BufferManager::SetRecoveryMode(bool enabled) { recovery_mode_.store(enabled, std::memory_order_relaxed); }

void BufferManager::HandlePageFault(pageid_t page_id) {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);
  physical_used_cnt_++;
  EnsureFreePages();
  ReadPage(page_id);
  resident_set_.Insert(page_id);
  // A page faulted during recovery comes back holding locks from the session that died.
  // We need to clear those locks
  if (recovery_mode_.load(std::memory_order_relaxed)) {
    reinterpret_cast<storage::BTreeNode *>(ToPtr(page_id))->ClearAllLockBytes();
  }
}

void BufferManager::ReadPage(pageid_t page_id) {
  [[maybe_unused]] int ret = pread(blockfd_, ToPtr(page_id), PAGE_SIZE, page_id * PAGE_SIZE);
  assert(ret == PAGE_SIZE);
  statistics::buffer::read_cnt++;
}

void BufferManager::Evict() {
  std::vector<pageid_t> to_evict(0);  // store all clean MARKED pages
  std::vector<pageid_t> to_write(0);  // store all dirty MARKED pages

  // 0. find evict candidates, lock dirty ones in shared mode
  //  we should only loop only 2 circles around the clock replacer
  //  - 1st loop is to mark all UNLOCKED -> MARKED
  //  - 2nd loop is to evaluate those MARKED candidate
  //  if we loop indefinitely, we can run into deadlock situation when the buffer pool is full of EXCLUSIVE pages
  for (u64 idx = 0; idx < resident_set_.Capacity(); idx++) {
    // we gather enough pages for the eviction batch, break
    if (to_evict.size() + to_write.size() >= evict_batch_) { break; }
    auto flushed_log_gsn = recovery::LogManager::global_min_gsn_flushed.load();
    resident_set_.IterateClockBatch(evict_batch_, [this, &to_evict, &to_write, &flushed_log_gsn](pageid_t pid) {
      Ensure(pid != METADATA_PAGE_ID);  // The metadata page always reside in memory
      auto page     = ToPtr(pid);
      auto &ps      = GetPageState(pid);
      u64 v         = ps.StateAndVersion();
      auto pg_count = ExtentPageCount(pid);

      switch (sync::PageState::LockState(v)) {
        /**
         * Finite state machine:
         * - UNLOCKED: The page is free (i.e. unused), marked for possible eviction
         * - SHARED: The page is being read (i.e. hot), ignore
         * - EXCLUSIVE: The page is being modified (i.e. hot), ignore
         * - MARKED: The page is marked (i.e. candidiate for eviction) by clock replacer
         *          Check whether we should write it before evict
         * - EVICTED: Already evicted, ignore
         */
        case sync::PageState::MARKED:
          // If this page is prevented to evict, don't do it
          if (IsExtent(pid) && ext_frame_[pid].prevent_evict.load()) { return; }
          // Whether to evict this page or not according to priority policy
          if (FLAGS_bm_enable_fair_eviction) {
            if (RandomGenerator::GetRandU64(0, maximum_page_size_.load()) > pg_count) { return; }
          }
          /**
           * @brief Add this page to to_write if
           * - This page is a normal page (not a BLOB page) and dirty
           *
           * Extent is always clean, so there is no need to write it to the storage
           */
          if (!IsExtent(pid) && PageIsDirty(pid)) {
            /**
             * @brief 2 conditions:
             * - prevent normal pages to be written to disk before its lastest log has been flushed
             *    (note that blob pages don't associate with any log record, hence this condition doesn't apply)
             * - prevents the page from entering (EXCLUSIVE, MARKED, EVICTED) state
             */
            if ((IsExtent(pid) || (!FLAGS_wal_enable || page->p_gsn <= flushed_log_gsn)) && (ps.TryLockShared(v))) {
              to_write.push_back(pid);
            }
          } else {
            /**
             * @brief In the case of blob page, eviction reaches this stage only when the blob content is flushed
             * That is, if the blob is not flushed yet, its PageState should be EXCLUSIVE,
             *  and all EXCLUSIVE pages can't be moved to any other state before being moved to UNLOCKED
             */
            assert((IsExtent(pid) && sync::PageState::LockState(v) != sync::PageState::EXCLUSIVE) ||
                   (!PageIsDirty(pid) && !IsExtent(pid)));
            to_evict.push_back(pid);
          }
          break;
        case sync::PageState::UNLOCKED: ps.TryMark(v); break;
        default: break;
      };
    });
  }

  // we can't do anything, return
  if (to_evict.size() + to_write.size() == 0) { return; }

  // 1. write dirty pages & mark them as clean
  for (auto &pid : to_write) {
    if (!IsExtent(pid)) { frame_[pid].last_written_gsn = ToPtr(pid)->p_gsn; }
  }
  aio_interface_[LeanStore::worker_thread_id].WritePages(to_write);
  statistics::buffer::write_cnt += to_write.size();

  // 2. try to X lock all clean page candidates
  std::erase_if(to_evict, [&](pageid_t pid) {
    sync::PageState &ps = GetPageState(pid);
    u64 v               = ps.StateAndVersion();
    return (sync::PageState::LockState(v) != sync::PageState::MARKED) || !ps.TryLockExclusive(v);
  });

  // 3. try to upgrade lock for dirty page candidates
  for (auto &pid : to_write) {
    sync::PageState &ps = GetPageState(pid);
    u64 v               = ps.StateAndVersion();
    if (ps.UpgradeLock(v)) {
      // only this evict thread uses this page, we can remove it from buffer pool
      to_evict.push_back(pid);
    } else {
      // others are using this page, so it is possible in hot path, thus we don't evict it from buffer pool
      ps.UnlockShared();
    }
  }

  // 4. remove from page table
  u64 evict_size = 0;
  for (size_t idx = 0; idx < to_evict.size(); idx++) {
    auto pid      = to_evict[idx];
    auto pg_count = ExtentPageCount(pid);
    evict_size    = evict_size + pg_count;
    madvise(&mem_[pid], PAGE_SIZE * pg_count, MADV_DONTNEED);

    // For normal buffer pool, simulate lookup here
    if (FLAGS_blob_normal_buffer_pool) {
      for (auto idx = 0UL; idx < pg_count; idx++) { resident_set_.Contain(pid + idx); }
    }
  }

  // 5. remove from hash table and unlock
  for (auto &pid : to_evict) {
    bool ret = resident_set_.Remove(pid);
    if (!ret) {
      throw leanstore::ex::GenericException(fmt::format("Evict page {} but it not reside in Buffer Pool", pid));
    }
    GetPageState(pid).UnlockExclusiveAndEvict();
  }

  statistics::buffer::evict_cnt += evict_size;
  physical_used_cnt_ -= evict_size;
}

// ------------------------------------------------------------------------------------------------------
/**
 * @brief Try to flush dirty pages of one shard up to target_gsn, and return the minimum on-disk GSN coverage
 * (frame_[pid]last_written_gsn) among all dirty pages in this shard.
 * The caller uses this value as the shard's safe  WAL-pruning frontier, log blocks can only be recycled
 * up to the minimum coverage of every shard, so no page's unflushed WAL records are lost.
 *
 * Previously this function advanced the shard watermark to target_gsn whenever every dirty page in the shard
 * was either flushed or had p_gsn > target_gsn.
 * But a page with p_gsn > target_gsn and frame_[pid].last_written_gsn < target_gsn had records below
 * target_gsn that were *NOT* covered by its on-disk image.
 * As such, upon recovery, recovery redo would operate on a stale image and crash.
 *
 * Solution is to track shard_min_coverage instead, which guarantees the shard watermark
 * never exceeds the actual minimum frame_[pid].last_written_gsn across its dirty pages.
 */
auto BufferManager::FuzzyCheckpointShard(u32 shard, timestamp_t target_gsn) -> u64 {
  auto &aio = aio_interface_[LeanStore::worker_thread_id];

  bool can_advance       = true;
  u64 shard_min_coverage = std::numeric_limits<u64>::max();

  std::vector<pageid_t> to_write;
  to_write.reserve(evict_batch_);
  resident_set_.IterateShard(shard, [&](pageid_t pid) {
    if (IsExtent(pid) || !PageIsDirty(pid)) { return; }

    auto &ps = GetPageState(pid);
    u64 v    = ps.StateAndVersion();
    switch (sync::PageState::LockState(v)) {
      case sync::PageState::EXCLUSIVE:
      case sync::PageState::EVICTED:
        /* Be pessimistic: the page is owned/being written by another thread. */
        can_advance = false;
        return;
      default: break;
    }

    auto page = ToPtr(pid);
    if (ps.TryLockShared(v)) {
      if (PageIsDirty(pid) && ((page->p_gsn <= target_gsn) ||
                               (page->p_gsn > target_gsn && frame_[pid].last_written_gsn.load() <= target_gsn))) {
        /* Flush the page.  Its on-disk image will now cover all records up to p_gsn. */
        frame_[pid].last_written_gsn = page->p_gsn;
        to_write.push_back(pid);
        shard_min_coverage = std::min(shard_min_coverage, page->p_gsn);
        if (to_write.size() >= evict_batch_) {
          aio.WritePages(to_write);
          statistics::buffer::write_cnt += to_write.size();
          for (auto &w_pid : to_write) { GetPageState(w_pid).UnlockShared(); }
          to_write.clear();
        }
      } else {
        /* Dirty page whose image already covers the target (last_written_gsn > target). */
        shard_min_coverage = std::min(shard_min_coverage, frame_[pid].last_written_gsn.load());
        ps.UnlockShared();
      }
    } else {
      /* Couldn't get the shared latch (likely EXCLUSIVE in flight). Be pessimistic. */
      can_advance = false;
    }
  });

  if (!to_write.empty()) {
    aio.WritePages(to_write);
    statistics::buffer::write_cnt += to_write.size();
    for (auto &w_pid : to_write) { GetPageState(w_pid).UnlockShared(); }
  }
  if (FLAGS_wal_fsync && can_advance) { fdatasync(blockfd_); }
  return can_advance ? shard_min_coverage : std::numeric_limits<u64>::max();
}

/* After recovery: scan resident pages and return the minimum last_written_gsn among dirty pages per shard.
 * Used by the checkpointer to bootstrap its per-shard watermarks from the prior session's disk images. */
auto BufferManager::ScanRecoveryCoverage() -> std::vector<timestamp_t> {
  std::vector<timestamp_t> coverage(FLAGS_wal_checkpoint_shards, std::numeric_limits<timestamp_t>::max());
  for (u32 s = 0; s < FLAGS_wal_checkpoint_shards; s++) {
    resident_set_.IterateShard(s, [&](pageid_t pid) {
      if (IsExtent(pid)) { return; }
      u64 coverage_value = frame_[pid].last_written_gsn.load();
      if (coverage_value == 0) { return; }
      coverage[s] = std::min(coverage[s], coverage_value);
    });
    if (coverage[s] == std::numeric_limits<timestamp_t>::max()) { coverage[s] = 0; }
  }
  return coverage;
}

/* Per-shard persisted frontier accessors.
 * The frontier is monotonic (UpdateMaxChkptedInShard) and only ever advances as far as the min on-disk image GSN of
 * the shard's dirty pages, so WAL blocks are never recycled past a page image that predates them. */
auto BufferManager::MaxChkptedInShard(u32 shard) -> timestamp_t {
  return max_chkpted_in_shard_[shard].load(std::memory_order_relaxed);
}

void BufferManager::SetChkptedInShard(u32 shard, timestamp_t coverage) {
  max_chkpted_in_shard_[shard].store(coverage, std::memory_order_relaxed);
}

void BufferManager::UpdateMaxChkptedInShard(u32 shard, timestamp_t coverage) {
  UpdateMax(max_chkpted_in_shard_[shard], coverage);
}

// ------------------------------------------------------------------------------------------------------
/**
 * @brief Allocate a single extent whose id is `extent_id`
 * `fixed_page_cnt` = 0 most of the time, and should only be set for Blob's TailExtent
 */
auto BufferManager::AllocExtent(extidx_t tier) -> pageid_t {
  auto required_page_cnt = ExtentList::ExtentSize(tier);
  Ensure(extent_virtual_cnt_ > required_page_cnt);

  // Use buddy allocator via FreeStorageManager
  pageid_t start_pid = free_storage_->AllocExtent(tier);
  if (start_pid == 0) {
    throw ex::GenericException("extent allocation failed: blob region fragmented or out of space");
  }

  // Prepare run-time env for the (possibly reused) extent.
  // A freed extent stays resident until Evict();
  // reuse it in place (no re-Insert, no page-count) unless it was actually evicted.
  auto &ps = GetPageState(start_pid);
  /* The buddy allocator hands back just-freed extents immediately; their header page may
   * still be latched (SHARED readers or an EXCLUSIVE writer from the prior blob), so spin
   * until the exclusive latch is acquired. */
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    if (ps.TryLockExclusive(v)) {
      if (sync::PageState::LockState(v) == sync::PageState::EVICTED) {
        PAGE_FAULT(start_pid, required_page_cnt);
        resident_set_.Insert(start_pid);
      }
      break;
    }
    AsmYield(cnt);
  }
  /* Only the header (start) page is latched; interior pages carry no page state. */
  ext_frame_[start_pid].Init();
  ext_frame_[start_pid].extent_size = required_page_cnt;
  if (FLAGS_bm_enable_fair_eviction) { UpdateMax(maximum_page_size_, required_page_cnt); }
  return start_pid;
}

/**
 * @brief Prepare necessary env for the extent eviction later
 *
 * The caller must ensure that `start_pid` is the start PID of an actual Extent
 */
void BufferManager::PrepareExtentEviction(pageid_t start_pid) {
  ext_frame_[start_pid].prevent_evict = true;
  UnfixExclusive(start_pid);
}

/**
 * @brief Evict an extent from the buffer manager
 * The caller is responsible for ensuring that [start_pid..start_pid+page_cnt) should refer to an existing extent
 */
void BufferManager::EvictExtent(pageid_t pid) {
  /**
   * It's possible that this Extent was already evicted in a batch
   * The scenario is:
   * - Txn A commits Extent X
   * - Txn B remove X
   * - Txn C reuse X and commits
   * - Group Commit flush (A, B, C)
   * In this scenario, X will be EvictExtent() twice
   * Because a single EvictExtent() is enough to commit all A, B, and C, we shouldn't EvictExtent() 1 more time
   */
  if (ext_frame_[pid].prevent_evict.load()) {
    ext_frame_[pid].prevent_evict = false;

    auto &ps = GetPageState(pid);
    u64 v    = ps.StateAndVersion().load();
    if (ps.TryLockExclusive(v)) { ps.UnlockExclusive(); }
  }
}

/**
 * @brief Try to read one or multiple extents from disk, and set necessary run-time variables
 * The caller is responsible for ensuring that large_pages should all refer to existing extents
 */
void BufferManager::ReadExtents(const storage::LargePageList &large_pages) {
  u64 total_page_cnt = 0;
  storage::LargePageList to_read_lp;
  to_read_lp.reserve(large_pages.size());

  // 1. Acquire necessary lock & space for the large pages before reading them
  for (const auto &lp : large_pages) {
    auto require_read = FixShareImpl(lp.start_pid);
    if (require_read) {
      to_read_lp.emplace_back(lp.start_pid, lp.page_cnt);
      total_page_cnt += lp.page_cnt;
    }
    Ensure((sync::PageState::UNLOCKED < GetPageState(lp.start_pid).LockState()) &&
           (GetPageState(lp.start_pid).LockState() <= sync::PageState::EXCLUSIVE));
  }
  physical_used_cnt_ += total_page_cnt;
  EnsureFreePages();

  // 2. Read them with io_uring
  aio_interface_[LeanStore::worker_thread_id].ReadLargePages(to_read_lp);

  // Mark all huge pages as Blob, and set them to SHARED state
  for (const auto &[start_pid, page_cnt] : to_read_lp) {
    resident_set_.Insert(start_pid);
    GetPageState(start_pid).DowngradeLock();
  }

  // 3. If normal buffer pool, lookup all normal pages
  if (FLAGS_blob_normal_buffer_pool) {
    for (const auto &lp : large_pages) {
      for (auto pid = lp.start_pid; pid < lp.start_pid + lp.page_cnt; pid++) { resident_set_.Contain(pid); }
    }
  }

  // Update statistics
  statistics::buffer::read_cnt += total_page_cnt;
}

auto BufferManager::ExtentPageCount(pageid_t pid) -> u64 {
  if (!IsExtent(pid)) { return 1; }
  return ext_frame_[pid].extent_size;
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Acquire necessary Locks on huge page (either X or S) before reading that page
 */
auto BufferManager::FixShareImpl(pageid_t page_id) -> bool {
  auto &ps = GetPageState(page_id);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
      case sync::PageState::EXCLUSIVE: {
        break;
      }
      case sync::PageState::EVICTED: {
        if (ps.TryLockExclusive(v)) { return true; }
        break;
      }
      default:
        if (ps.TryLockShared(v)) { return false; }
        break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixExclusive(pageid_t page_id) -> storage::Page * {
  ValidatePID(page_id);

  sync::PageState &ps = GetPageState(page_id);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
      case sync::PageState::EVICTED: {
        if (ps.TryLockExclusive(v)) {
          HandlePageFault(page_id);
          return ToPtr(page_id);
        }
        break;
      }
      case sync::PageState::MARKED:
      case sync::PageState::UNLOCKED:
        if (ps.TryLockExclusive(v)) { return ToPtr(page_id); }
        break;
      default: break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixShare(pageid_t page_id) -> storage::Page * {
  ValidatePID(page_id);

  auto required_read = FixShareImpl(page_id);
  if (required_read) {
    HandlePageFault(page_id);
    GetPageState(page_id).DowngradeLock();
  }
  return ToPtr(page_id);
}

void BufferManager::UnfixExclusive(pageid_t page_id) {
  ValidatePID(page_id);
  GetPageState(page_id).UnlockExclusive();
}

void BufferManager::UnfixShare(pageid_t page_id) {
  ValidatePID(page_id);
  GetPageState(page_id).UnlockShared();
}

}  // namespace leanstore::buffer
