#pragma once

#include "common/typedefs.h"
#include "common/utils.h"
#include "recovery/log_entry.h"

#include "gtest/gtest_prod.h"
#include "liburing.h"

#include <atomic>
#include <functional>
#include <limits>
#include <queue>
#include <unordered_set>
#include <utility>

namespace leanstore::recovery {

class LogManager;
struct LogWorker;
struct WALBlock;

struct LogIOSegment {
  u8 *buffer;  // the buffer used to store temp WAL entries before flushing them to the storage

  struct Buffer {
    u8 *buffer         = nullptr;
    u64 size           = 0;
    bool stolen_buffer = false;

    Buffer() = default;
    explicit Buffer(u64 size);
    Buffer(u8 *stolen_buffer, u64 size);
    Buffer(Buffer &&obj) noexcept; /* Required for std::vector<Buffer> */
    ~Buffer();
    void Deallocate();

    void ResizeBuffer(u64 new_size);
    void InsertNewLog(const u8 *log_buffer, u64 log_size);
    auto IsEmpty() -> bool;
    auto Iterate(const std::function<bool(const LogEntry *)> &fn) const -> u64;
  };

  /* A tournament-tree-like data structure that generates the next smallest-GSN log entry from all `Buffer`s*/
  struct LazyGenerator {
    struct Offset {
      Buffer *log;
      u64 offset;

      explicit Offset(Buffer *log, u64 offset = 0);
      auto operator<=>(const Offset &other) const;
    };

    std::priority_queue<Offset> tournament;

    LazyGenerator() = default;
    void ReceiveInput(Buffer *w_segment);
    auto Iterate(const std::function<bool(const DataEntry *)> &fn) -> u64;
  };

  LogIOSegment();
  ~LogIOSegment();

  void Serialize(u64 log_size, const std::function<void(u8 *, u64 &)> &fill_fn,
                 const std::function<void(u8 *, u64)> &write_fn);
  static auto Deserialize(u8 *buffer, u64 end_offset, u32 required_signature,
                          const std::function<void(u8 *, u64)> &analyze_fn) -> u64;
};

class LogBackend {
 public:
  static constexpr u8 LOGGING_AIO_QD = 8;

  explicit LogBackend(LogManager *log_manager, LogWorker *worker);
  void Connect();

  /* Sync life-cycle */
  void SyncAppend(u8 *buffer, u64 size, const std::function<void()> &async_fn = {});

 private:
  friend class LogManager;

  LogManager *manager_;
  LogWorker *worker_;

  /* Properties for SSD log backend */
  struct io_uring ring_;

  /* WAL-block checkpointing bookkeeping (one entry per worker):
   * cur_block_ = registry slot this worker is filling (nullptr = none); its slot is
   * stable because the registry is fixed-capacity and never reallocates.
   * cur_block_->block_next_ serves as the descending write frontier within the
   * current block; it starts at block_base_ + wal_block_size_ and moves toward
   * block_base_. */
  WALBlock *cur_block_{nullptr};
  timestamp_t cur_block_max_gsn_{0};

  /* Helpers for SSD log backend */
  auto LogFlushOffset(u64 chunk_size) -> u64;
};

}  // namespace leanstore::recovery