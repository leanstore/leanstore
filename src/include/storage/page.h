#pragma once

#include "common/constants.h"
#include "sync/page_state.h"

namespace leanstore::storage {

struct PageHeader {
  timestamp_t p_gsn{1};  // the GSN of this Page
};

class alignas(PAGE_SIZE) Page : public PageHeader {};

class alignas(PAGE_SIZE) MetadataPage : public PageHeader {
 public:
  pageid_t roots[(PAGE_SIZE - sizeof(timestamp_t)) / sizeof(pageid_t)];

  auto GetRoot(leng_t slot) -> pageid_t { return roots[slot]; }
};

static_assert(sizeof(Page) == PAGE_SIZE);
static_assert(sizeof(MetadataPage) == PAGE_SIZE);

}  // namespace leanstore::storage
