#include "storage/extent/extent_list.h"

namespace leanstore::storage {

auto TailExtent::SplitToExtents(pageid_t start_pid, u64 page_cnt, const std::function<void(pageid_t, extidx_t)> &cb)
  -> u32 {
  auto index = ExtentList::NO_TIERS - 1;
  u32 count  = 0;

  while (page_cnt > 0) {
    while (page_cnt < ExtentList::TIER_SIZE[index]) { --index; }
    cb(start_pid, index);
    count++;
    page_cnt -= ExtentList::TIER_SIZE[index];
    start_pid += ExtentList::TIER_SIZE[index];
  }

  Ensure(page_cnt == 0);
  return count;
}

auto TailExtent::SplitToExtents(const std::function<void(pageid_t, extidx_t)> &cb) const -> u32 {
  return TailExtent::SplitToExtents(start_pid, page_cnt, cb);
}

}  // namespace leanstore::storage