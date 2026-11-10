#pragma once

#include "common/constants.h"
#include "sync/page_state.h"

namespace leanstore::storage {

struct PageHeader {
  timestamp_t p_gsn{1};  // the GSN of this Page
};

class alignas(PAGE_SIZE) Page : public PageHeader {};

struct alignas(PAGE_SIZE) MetadataPage : public PageHeader {
  std::atomic<pageid_t> alloc_cnt;
  pageid_t roots[(PAGE_SIZE - sizeof(timestamp_t) - sizeof(pageid_t)) / sizeof(pageid_t)];

  // This API assumes the caller already hold (either shared/exclusive) lock on metadata page
  auto GetNewPagePID() -> pageid_t {
    auto pid = alloc_cnt.fetch_add(1, std::memory_order_relaxed);
    return pid;
  }

  auto GetRoot(leng_t slot) -> pageid_t { return roots[slot]; }
};

static_assert(sizeof(Page) == PAGE_SIZE);
static_assert(sizeof(MetadataPage) == PAGE_SIZE);

}  // namespace leanstore::storage
