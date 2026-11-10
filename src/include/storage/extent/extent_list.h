#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "storage/extent/large_page.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <iterator>
#include <limits>
#include <ranges>

namespace leanstore::storage {

/**
 * @brief Every BlobState refers to a list of large pages/extents (similar to `extent` in filesystem)
 * When a new Blob (and the representative BlobState) is allocated, its new content must be written somewhere
 * There are two possible solutions:
 * - Allocate a new large page only to store the newly-created content
 * - Append the content to an existing extent, which should be big enough to store existing content + new one
 * In the first approach, the number of extents a Stem Blob can allocate is unbounded,
 *  thus storing extents in BlobState is infeasible as its size is unlimitted and not fit in a single B-Tree page
 * In other words, first approach requires a B-Tree to manage all actives BlobStates in our system,
 *  which makes Blob allocation/garbage collection complicated
 * Therefore, LeanStore advocates for the 2nd solution.
 *
 * A Blob is immutable: once allocated, its content never changes. Extents grow geometrically
 *  (doubling) and are capped at MAX_EXTENT_SIZE, so a Blob of at most 8GB fits in 32 extents.
 */
struct ExtentList {
  // -------------------------------------------------------------------------------------
  static constexpr auto MAX_NO_EXTENTS      = 32UL;            // max # extents per Blob (8GB)
  static constexpr extidx_t EXTENT_CNT_MASK = 0x3F;            // 6-bit extent counter, up to 63 extents
  static constexpr u64 MAX_EXTENT_SIZE      = 131072UL;        // 512MB (2^17 pages)
  static constexpr extidx_t NO_TIERS        = MAX_NO_EXTENTS;  // ramp tiers + capped (flat) tiers

  // Pre-computed size of all tiers, unit is DB page. tier[i] = min(2^i, MAX_EXTENT_SIZE)
  static constexpr auto TIER_SIZE{[]() constexpr {
    std::array<u64, NO_TIERS> result{};
    u64 tier_size = 1;
    for (auto i = 0UL; i < NO_TIERS; ++i) {
      result[i] = std::min(tier_size, MAX_EXTENT_SIZE);
      tier_size <<= 1;
    }
    return result;
  }()};
  static constexpr auto TOTAL_TIER_SIZE{[]() constexpr {
    std::array<u64, NO_TIERS> result{};
    result[0] = TIER_SIZE[0];
    for (int i = 1; i < NO_TIERS; ++i) { result[i] = result[i - 1] + TIER_SIZE[i]; }
    return result;
  }()};

  // Total size of the first NO_TIERS extents == max Blob size in pages (8GB - 1 page)
  static constexpr u64 INITIAL_SIZE = ArraySum<u64, NO_TIERS>(TIER_SIZE);

  // -------------------------------------------------------------------------------------
  /* All attributes */
  extidx_t extent_cnt : 6;  // The number of extents of this Blob
  pageid_t extent_pid[];    // The start PID of every extents

  // -------------------------------------------------------------------------------------
  /**
   * @brief Useful Iterator and operator utilities for accessing the extents
   */
  struct Iterator {
    explicit Iterator(const ExtentList *ext, extidx_t pos = 0) : pos_(pos), data_(0, 0), parent_(ext) {
      RetrieveData();
    }

    ~Iterator() = default;

    auto operator*() const -> const LargePage & { return data_; }

    auto operator++() -> Iterator & {
      pos_++;
      RetrieveData();
      return *this;
    }

    friend auto operator!=(const Iterator &a, const Iterator &b) -> bool {
      return (a.parent_ != b.parent_) || (a.pos_ != b.pos_);
    };

   private:
    extidx_t pos_;
    LargePage data_;
    const ExtentList *parent_;

    void RetrieveData() {
      if (pos_ < parent_->NumberOfExtents()) {
        pageid_t pid = parent_->extent_pid[pos_];
        data_        = LargePage(pid, ExtentList::ExtentSize(pos_));
      }
    }
  };

  // NOLINTBEGIN
  auto begin() const -> Iterator { return Iterator(this, 0); }

  auto end() const -> Iterator { return Iterator(this, NumberOfExtents()); }

  // NOLINTEND

  auto operator[](int idx) const -> LargePage {
    assert(idx < NumberOfExtents());
    return {extent_pid[idx], ExtentSize(idx)};
  }

  // -------------------------------------------------------------------------------------
  /**
   * @brief Number of pages that extent `extent_id` have
   */
  static auto ExtentSize(extidx_t extent_id) -> u64 {
    return (extent_id < NO_TIERS) ? TIER_SIZE[extent_id] : MAX_EXTENT_SIZE;
  }

  /**
   * @brief Total size of all extents of range [0..idx]
   * The size unit is DB pages
   */
  static auto TotalSizeExtents(extidx_t idx) -> u64 {
    if (idx < NO_TIERS) { return TOTAL_TIER_SIZE[idx]; }
    return INITIAL_SIZE + (idx - NO_TIERS + 1) * MAX_EXTENT_SIZE;
  }

  /**
   * @brief Size of an ExtentList with specified number of extents
   */
  static constexpr auto MallocSize(extidx_t no_extents) -> u64 {
    return sizeof(ExtentList) + sizeof(pageid_t) * no_extents;
  }

  /**
   * @brief Number of extents (from tier 0) that a payload of `size` pages spans across on
   */
  static auto NoSpanExtents(pageid_t no_pages) -> extidx_t {
    Ensure(no_pages <= INITIAL_SIZE);
    return std::ranges::find_if_not(TOTAL_TIER_SIZE, [&no_pages](u64 other) { return other < no_pages; }) -
           TOTAL_TIER_SIZE.begin() + 1;
  };

  // -------------------------------------------------------------------------------------
  /**
   * @brief Number of extents, i.e. size of `extent_pid` attribute
   */
  auto NumberOfExtents() const -> extidx_t { return extent_cnt; }
} __attribute__((packed));

// Magic numbers
static_assert(ExtentList::MAX_NO_EXTENTS < ExtentList::EXTENT_CNT_MASK);
static_assert(ExtentList::TIER_SIZE[ExtentList::NO_TIERS - 1] == ExtentList::MAX_EXTENT_SIZE);
static_assert(ExtentList::MAX_EXTENT_SIZE < (1UL << 48) - 1);
static_assert(ExtentList::INITIAL_SIZE == (8UL << 30) / PAGE_SIZE - 1);
static_assert(sizeof(ExtentList) == 1);
static_assert(ExtentList::MallocSize(ExtentList::MAX_NO_EXTENTS) == 257);

}  // namespace leanstore::storage