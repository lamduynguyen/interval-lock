/*
 * Continuous fuzzy checkpointing for the circular WAL (LEMM-style, shard-based).
 * Driven by a dedicated background thread (LeanStore::checkpointer); each
 * increment flushes a 1/S buffer-pool shard round-robin; once every shard is
 * checkpointed past a GSN, the closed WAL blocks at or below it can be recycled.
 */

#pragma once

#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "common/typedefs.h"
#include "gtest/gtest_prod.h"
#include "leanstore/config.h"

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::recovery {

class LogManager;

/**
 * @brief One physical log block of the circular WAL region.
 *
 * Lifecycle is one atomic state byte, so the writer-vs-checkpointer claim is atomic:
 *   OPEN -- writer appends;
 *   CLOSED -- writer done, block_gsn final;
 *   DONE -- every shard checkpointed past this block's gsn, slot reusable.
 * Default to DONE, allowing workers to OpenNext() a block successfully
 * Transitions:
 *   OPEN -> CLOSED (writer):         LogBackend::LogFlushOffset
 *   CLOSED -> DONE (checkpointer):   WALBlockManager::MarkDoneThroughGsn
 *   DONE -> OPEN (writer recycle):   WALBlockManager::OpenNext
 * DONE is terminal, so claim vs recycle can't race.
 */
struct WALBlock {
  enum State : u8 { OPEN = 0, CLOSED = 1, DONE = 2 };

  /* Highest GSN of any record written into this block, stamped when CLOSED. */
  timestamp_t block_gsn{0};

  /* Physical WAL-region offsets for this block (written downward):
   * - block_base_: fixed lower bound of the block (never crossed, const)
   * - block_next_: descending write frontier; each append does block_next_ -= chunk_size
   * These live on the block so that recycled blocks get fresh offsets that
   * stay within the circular WAL region. */
  const u64 block_base_;
  u64 block_next_{0};

  /* Slot state machine */
  std::atomic<u8> state{State::DONE};

  WALBlock(u64 block_base, u64 block_next) : block_base_(block_base), block_next_(block_next) {}

  /* Required so std::vector<WALBlock> satisfies MoveInsertable. Only the ring
   * registry constructs these; the atomics are authoritatively read in place. */
  WALBlock(WALBlock &&o) noexcept
      : block_gsn(o.block_gsn), block_base_(o.block_base_), block_next_(o.block_next_), state(o.state.load()) {}
};

/**
 * @brief Fixed-capacity ring registry of WAL blocks.
 *
 * Physical blocks are allocated downward from the WAL region tail via w_offset_.
 * A writer reserves a slot via OpenNext(...) at `global_seq % Capacity()`. A slot
 * is reused only after its prior occupant is DONE; OpenNext enforces that
 * back-pressure, invoking `backpressure_helper` (typically TriggerCheckpoint)
 * while waiting so the checkpointer can make progress.
 *
 * Owns the checkpoint frontier: head (first not-yet-checkpointed block) and the
 * durable watermark (block_gsn of the last done prefix block), advanced by
 * MarkDoneThroughGsn as blocks flip to DONE.
 */
class WALBlockManager {
 public:
  explicit WALBlockManager(u32 num_blocks, u64 wal_block_size, u64 wal_region_end);

  inline auto Capacity() const -> u32 { return static_cast<u32>(blocks_.size()); }

  /* Number of blocks in DONE state */
  inline auto BlockCycleCnt() -> u64 { return blocks_recycled_cnt_.load(std::memory_order_relaxed); }

  /* Reserves a fresh block slot and returns it. Waits for back-pressure if the
   * slot is about to be reused before its previous occupant is checkpointed. */
  auto OpenNext() -> WALBlock &;

  /* Flip the leading CLOSED blocks whose watermark <= gsn to DONE (all buffer
   * shards are checkpointed past them) and advance the frontier over the done
   * prefix, publishing the durable watermark. */
  void MarkDoneThroughGsn(timestamp_t gsn);

  /* During recovery, mark a block as CLOSED with the given block_gsn. */
  void RecoverBlock(u32 idx, timestamp_t gsn);

 private:
  /* TEST_F bodies run in classes derived from the fixture, so the frontier is
   * exposed to the concrete gtest classes, not the fixture itself. */
  FRIEND_TEST(TestWALBlock, RingWrapAround);
  FRIEND_TEST(TestWALBlock, StampingAndReuseState);
  FRIEND_TEST(TestWALBlock, BackpressureWaitsForDone);
  FRIEND_TEST(TestWALBlock, BackpressureProceedsWhenDone);
  FRIEND_TEST(TestWALBlock, MarkDoneThroughGsnRecyclesCoveredPrefix);
  FRIEND_TEST(TestCheckpointer, AdvancesHeadPastDoneBlocks);
  FRIEND_TEST(TestCheckpointer, RespectsDurabilityBarrier);

  std::vector<WALBlock> blocks_;
  std::atomic<u64> blocks_recycled_cnt_{0};
  const u64 wal_block_size_;
};

/**
 * @brief Fuzzy checkpointing. Each increment flushes one 1/S buffer-pool shard,
 * records the durable GSN snapshot as that shard's persisted frontier, and recycles the
 * closed WAL blocks at or below the minimum frontier over all shards.
 */
class Checkpointer {
 public:
  Checkpointer(buffer::BufferManager *buffer, LogManager *log_manager);

  /* One checkpoint increment: flush the next shard, then recycle the done WAL
   * prefix. Serialized so increments are atomic. */
  void TryCheckpoint();

  /* After recovery: scan resident pages to rebuild per-shard coverage watermarks
   * from their on-disk images, then recycle the CLOSED prefix. */
  void Bootstrap();

 private:
  friend class LogManager;

  buffer::BufferManager *buffer_;
  LogManager *log_manager_;
  std::atomic<bool> is_bootstraping_;

  u64 increment_{0};
};

}  // namespace leanstore::recovery
