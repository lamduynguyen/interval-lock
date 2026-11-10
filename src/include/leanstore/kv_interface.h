#pragma once

#include "common/delta.h"
#include "common/typedefs.h"

#include <functional>
#include <span>

namespace leanstore {

// All functions should be idempotent + have no side-effect
using AccessPayloadFunc = std::function<void(std::span<const u8>)>;
using ModifyPayloadFunc = std::function<void(std::span<u8>)>;
using AccessRecordFunc  = std::function<bool(std::span<u8>, std::span<u8>)>;

enum class ComparisonOperator : u8 {
  MEMCMP,
  BLOB_HANDLER,
  BLOB_LOOKUP,
};

struct ComparisonLambda {
  ComparisonOperator op;
  std::function<int(const void *, const void *, size_t)> func;
};

enum class OpResult : u8 { OK = 0, NOT_FOUND = 1, DUPLICATE = 2, ABORT_TX = 3, STOP_SCAN = 4, NEED_SPLIT = 5 };

/**
 * @brief Bounds of a scan, in folded key space.
 * An empty `hi` means an open-ended ascending scan;
 * an empty `lo` means an open-ended descending scan.
 * Open-ended scans register one bounded interval per leaf, using the leaf's fence keys,
 * and stop at the leaf where the callback returns false.
 */
struct ScanRange {
  std::span<u8> lo;
  std::span<u8> hi;
  bool lo_inclusive{true};
  bool hi_inclusive{true};
};

class KVInterface {
 public:
  virtual ~KVInterface() = default;

  // Insertion should be append-biases or not
  virtual void SetComparisonOperator(ComparisonLambda cmp) = 0;

  // -------------------------------------------------------------------------------------
  virtual auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> OpResult                            = 0;
  virtual auto Insert(std::span<u8> key, std::span<const u8> payload) -> OpResult                                 = 0;
  virtual auto Remove(std::span<u8> key) -> OpResult                                                              = 0;
  virtual auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult  = 0;
  virtual auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> OpResult = 0;
  virtual auto ScanAscending(ScanRange range, const AccessRecordFunc &fn) -> OpResult                             = 0;
  virtual auto ScanDescending(ScanRange range, const AccessRecordFunc &fn) -> OpResult                            = 0;
  virtual auto CountEntries() -> u64                                                                              = 0;
  virtual auto SizeInMB() -> float                                                                                = 0;
};

using InternalCatalog = std::vector<KVInterface *>;
extern InternalCatalog catalog;

}  // namespace leanstore
