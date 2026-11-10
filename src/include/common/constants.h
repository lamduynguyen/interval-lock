#pragma once

#include <cstdint>
#include <limits>

namespace leanstore {

static constexpr uint64_t METADATA_PAGE_ID = 0;
static constexpr uint64_t INVALID_PAGE_ID  = std::numeric_limits<uint64_t>::max();
static constexpr uint8_t SAMPLING_SIZE     = 16;
// -------------------------------------------------------------------------------------
static constexpr uint64_t PAGE_SIZE = 4096;
static constexpr uint64_t KB        = 1024ULL;
static constexpr uint64_t MB        = 1024ULL * 1024;
static constexpr uint64_t GB        = 1024ULL * 1024 * 1024;
// -------------------------------------------------------------------------------------
static constexpr uint64_t MSB       = static_cast<uint64_t>(1) << 63;
static constexpr uint64_t MSB_MASK  = ~(MSB);
static constexpr uint64_t MSB2      = static_cast<uint64_t>(1) << 62;
static constexpr uint64_t MSB2_MASK = ~(MSB2);
// -------------------------------------------------------------------------------------
static constexpr uint16_t MAX_NUMBER_OF_WORKER = 256;
// -------------------------------------------------------------------------------------
static constexpr uint32_t CPU_CACHELINE_SIZE = 64;
static constexpr uint32_t BLK_BLOCK_SIZE     = 4096;
// -------------------------------------------------------------------------------------
static constexpr uint32_t YIELDS_BEFORE_SLEEP = 10;

}  // namespace leanstore
