#pragma once

#include "common/constants.h"
#include "sync/page_state.h"

namespace leanstore::storage {

struct PageHeader {
  bool dirty{false};
  indexid_t idx_id{0};  // the id of index of this page
  logid_t p_gsn{0};     // the GSN of this Page
};

class alignas(PAGE_SIZE) Page : public PageHeader {};

class alignas(PAGE_SIZE) MetadataPage : public PageHeader {
 public:
  pageid_t roots[(PAGE_SIZE - sizeof(bool) - sizeof(indexid_t) - sizeof(logid_t)) / sizeof(pageid_t)];

  auto GetRoot(leng_t slot) -> pageid_t { return roots[slot]; }
};

static_assert(sizeof(Page) == PAGE_SIZE);
static_assert(sizeof(MetadataPage) == PAGE_SIZE);

}  // namespace leanstore::storage
