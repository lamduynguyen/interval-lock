#include "recovery/checkpoint.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "recovery/log_manager.h"

#include "share_headers/logger.h"

#include <algorithm>
#include <limits>

namespace leanstore::recovery {

// -------------------------------------------------------------------------------------
// WALBlockManager

WALBlockManager::WALBlockManager(u32 num_blocks, u64 wal_block_size, u64 wal_region_end)
    : wal_block_size_(wal_block_size) {
  Ensure(num_blocks > 0);
  blocks_recycled_cnt_ = num_blocks;
  blocks_.reserve(num_blocks);
  /* Pre-compute each block's physical position within the WAL region.
   * Blocks are carved downward from wal_region_end: block 0 sits at the top,
   * block 1 below it, and so on.  These offsets are constant for the entire
   * session lifetime. */
  for (auto i = 0U; i < num_blocks; i++) {
    auto base = wal_region_end - static_cast<u64>(i + 1) * wal_block_size;
    blocks_.emplace_back(base, base + wal_block_size);
  }
}

auto WALBlockManager::OpenNext() -> WALBlock & {
  while (true) {
    for (u32 i = 0; i < blocks_.size(); i++) {
      auto &block = blocks_[i];
      auto state  = block.state.load(std::memory_order_acquire);
      if (state != WALBlock::DONE) { continue; }
      if (!block.state.compare_exchange_strong(state, WALBlock::OPEN, std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
        continue;  // another worker claimed it first
      }
      blocks_recycled_cnt_ -= 1;
      block.block_gsn   = 0;
      block.block_next_ = block.block_base_ + wal_block_size_;
      return block;
    }
    if (!FLAGS_wal_enable_checkpoint) {
      throw leanstore::ex::EnsureFailed("WAL ring wrap requires checkpointing, but wal_enable_checkpoint=false");
    }
    std::this_thread::yield();
  }
}

void WALBlockManager::MarkDoneThroughGsn(timestamp_t gsn) {
  auto cap = static_cast<u32>(blocks_.size());

  for (u32 i = 0; i < cap; i++) {
    auto &block = blocks_[i];
    auto state  = block.state.load(std::memory_order_acquire);
    if (state != WALBlock::CLOSED) { continue; }
    if (block.block_gsn > gsn) { continue; }
    if (!block.state.compare_exchange_strong(state, WALBlock::DONE, std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
      break;
    }
    blocks_recycled_cnt_ += 1;
  }
}

void WALBlockManager::RecoverBlock(u32 idx, timestamp_t gsn) {
  Ensure(idx < blocks_.size());
  auto &block     = blocks_[idx];
  block.block_gsn = gsn;
  block.state.store(WALBlock::CLOSED, std::memory_order_release);
  blocks_recycled_cnt_ -= 1;
  LOG_DEBUG("RecoverBlock idx={} gsn={}", idx, gsn);
}

// -------------------------------------------------------------------------------------
Checkpointer::Checkpointer(buffer::BufferManager *buffer, LogManager *log_manager)
    : buffer_(buffer), log_manager_(log_manager), is_bootstraping_(true) {
  assert(FLAGS_wal_checkpoint_shards >= FLAGS_worker_count);
  assert(FLAGS_wal_enable && FLAGS_wal_enable_checkpoint);
}

void Checkpointer::TryCheckpoint() {
  auto &mgr = log_manager_->WALBlocks();

  // Always recompute the durable frontier from all workers' commit states.
  // Under bulk loading, group commit never runs so global_min_gsn_flushed stay 0.
  timestamp_t min_gsn = std::numeric_limits<timestamp_t>::max();
  for (wid_t w_i = 0; w_i < FLAGS_worker_count; w_i++) {
    min_gsn = std::min(min_gsn, log_manager_->w_last_gsn_[w_i].value.load(std::memory_order_acquire));
  }
  UpdateMax(LogManager::global_min_gsn_flushed, min_gsn);

  /* Flush the next shard (round-robin), gated by that durable frontier (min_gsn).
   * The returned coverage is the min on-disk GSN (last_written_gsn) across the
   * shard's dirty pages, so the monotonic per-shard watermark can only advance
   * as far as the images actually flushed. */
  auto shard   = static_cast<u32>(increment_ % FLAGS_wal_checkpoint_shards);
  u64 coverage = buffer_->FuzzyCheckpointShard(shard, min_gsn);
  if (coverage != std::numeric_limits<u64>::max()) { buffer_->UpdateMaxChkptedInShard(shard, coverage); }
  increment_++;

  /* Whole-DB checkpoint watermark = the minimum persisted GSN over all shards. */
  auto chkpted_gsn = std::numeric_limits<timestamp_t>::max();
  for (auto s = 0U; s < FLAGS_wal_checkpoint_shards; s++) {
    chkpted_gsn = std::min(chkpted_gsn, buffer_->MaxChkptedInShard(s));
  }

  /* Recycle the closed WAL blocks at or below the watermark. */
  mgr.MarkDoneThroughGsn(chkpted_gsn);
}

void Checkpointer::Bootstrap() {
  auto &mgr = log_manager_->WALBlocks();

  /* Rebuild per-shard watermarks from the on-disk images loaded during recovery.
   * Each page's last_written_gsn was set to its on-disk p_gsn via AllocPage(true). */
  auto shard_cov = buffer_->ScanRecoveryCoverage();

  timestamp_t min_gsn = std::numeric_limits<timestamp_t>::max();
  for (u32 s = 0; s < FLAGS_wal_checkpoint_shards; s++) {
    buffer_->SetChkptedInShard(s, shard_cov[s]);
    min_gsn = std::min(min_gsn, shard_cov[s]);
    LOG_DEBUG("Bootstrap shard[{}] coverage={}", s, shard_cov[s]);
  }

  /* Prune CLOSED blocks whose block_gsn <= min coverage. */
  mgr.MarkDoneThroughGsn(min_gsn);
  LOG_INFO("Bootstrap: {} shards, min_coverage={}, DONE_blocks={}/{}", FLAGS_wal_checkpoint_shards, min_gsn,
           mgr.BlockCycleCnt(), mgr.Capacity());
}

}  // namespace leanstore::recovery
