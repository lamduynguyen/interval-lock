#pragma once

#include "common/typedefs.h"
#include "common/utils.h"

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::recovery {

class RecoveryManager;

class RecoveryBackend {
 public:
  explicit RecoveryBackend(buffer::BufferManager *buffer);
  void Connect();
  void RetrieveMasterRecord();
  auto ReadLogBlocks(const std::function<void(u8 *buf, u64 len, u32 signature, u64 offset)> &materialize_fn) -> u64;

 private:
  friend class RecoveryManager;

  /* General */
  buffer::BufferManager *buffer_;
  u32 signature_;

  /* Properties for SSD log backend */
  int wal_fd_;
  std::atomic<u64> wal_offset_;
  u64 wal_offset_initial_{0};
  u64 wal_block_size_;
  u32 retries_per_worker_;
  u32 prev_worker_count_{0};
  u32 prev_wal_size_gb_{0};
  u32 prev_wal_block_size_mb_{0};
};

}  // namespace leanstore::recovery