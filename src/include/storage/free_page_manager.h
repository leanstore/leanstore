#pragma once

#include "buffer/buffer_manager.h"
#include "storage/extent/extent_list.h"
#include "sync/hybrid_latch.h"
#include "transaction/transaction.h"

#include "gtest/gtest_prod.h"
#include "tbb/concurrent_queue.h"

#include <array>
#include <atomic>
#include <deque>
#include <vector>

namespace leanstore::storage {

class FreePageManager {
 public:
  FreePageManager() = default;

  // Public APIs
  auto RequestFreeExtent(u64 required_page_cnt, const std::function<bool(pageid_t)> &validate_extent, pageid_t &out_pid,
                         TierList &out_split_exts) -> bool;
  void PublicFreeRanges(const storage::TierList &free_extents);
  void PrepareFreeTier(pageid_t start_pid, u8 tier_index);
  void PrepareFreePages(pageid_t start_pid, u64 page_cnt, TierList &out_split_exts);

  // Should only be used for testing
  auto NumberOfFreeEntries() -> u64;

 private:
  void LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx);

  std::array<tbb::concurrent_queue<pageid_t>, ExtentList::NO_TIERS> tiers_;
};

}  // namespace leanstore::storage