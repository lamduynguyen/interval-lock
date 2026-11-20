#pragma once

#include "common/typedefs.h"
#include "common/util/small_vector.h"
#include "sync/hybrid_latch.h"

#include <atomic>
#include <bitset>

namespace leanstore::transaction {
struct RangeEntry;
}

namespace leanstore::buffer {

/**
 * @brief Run-time context of all DB pages
 */
struct BufferFrame {
  wid_t last_writer;                  // the id of last worker who modifies this page since its last flush
  std::atomic<u64> last_written_gsn;  // the last gsn that this extent/page was written to storage

  // Per-page range references (precision variant), protect by  `range_latch`
  alignas(64) sync::HybridLatch range_latch;
  common::SmallVector<transaction::RangeEntry *> range_refs;

  void Init() {
    last_writer      = 0;
    last_written_gsn = 0;
    range_latch.Init();
    range_refs.Init();
  }
};

/**
 * @brief Similar to Buffer Frame, but for Extent pages
 */
struct alignas(8) ExtentFrame {
  std::atomic<bool> prevent_evict;
  u64 extent_size : 48;

  void Init() {
    prevent_evict = false;
    extent_size   = 1;
  }
};

static_assert(sizeof(ExtentFrame) == 8);

}  // namespace leanstore::buffer
