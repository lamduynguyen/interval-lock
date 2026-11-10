#pragma once

#include "common/typedefs.h"
#include "common/utils.h"

#include <atomic>
#include <cstdint>
#include <stdexcept>

namespace leanstore::sync {

/// Test-and-test-and-set latch. All critical sections below are short and allocation-free
/// (except the rare owner-vector growth), so a kernel-assisted mutex only adds overhead.
class SpinLatch {
 public:
  void Init() { locked_.store(false, std::memory_order_release); }

  void lock() {
    while (locked_.exchange(true, std::memory_order_acquire)) {
      while (locked_.load(std::memory_order_relaxed)) { AsmYield(); }
    }
  }

  void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> locked_{false};
};

}  // namespace leanstore::sync
