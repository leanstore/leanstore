#pragma once

#include "common/typedefs.h"

#include <vector>

namespace leanstore::storage {

struct LargePage {
  pageid_t start_pid;
  u64 page_cnt;

  LargePage(pageid_t sid, u64 cnt) : start_pid(sid), page_cnt(cnt) {}

  friend auto operator==(const LargePage &a, const LargePage &b) -> bool {
    return (a.start_pid == b.start_pid) && (a.page_cnt == b.page_cnt);
  };
};

struct ExtentTier {
  pageid_t start_pid;
  u8 tier;

  ExtentTier(pageid_t sid, u8 idx) : start_pid(sid), tier(idx) {}
};

using LargePageList = std::vector<LargePage>;
using TierList      = std::vector<ExtentTier>;

}  // namespace leanstore::storage