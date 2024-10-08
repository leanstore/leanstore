#pragma once

#include "storage/extent/extent_list.h"
#include "sync/range_lock.h"

#include "gtest/gtest_prod.h"
#include "tbb/concurrent_map.h"
#include "tbb/concurrent_queue.h"

#include <array>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::storage {

class FreeStorageManager {
 public:
  FreeStorageManager();
  ~FreeStorageManager() = default;

  // Extent APIs
  auto TryLockRange(pageid_t start_pid, u64 page_count) -> bool;
  auto RequestFreeExtent(u64 page_cnt, const std::function<bool(pageid_t)> &validate_extent, pageid_t &out_pid) -> bool;
  void PublicFreeExtents(std::span<storage::ExtentTier> extents);
  void PrepareFreeTier(pageid_t start_pid, u8 tier_index);

  // Should only be used for testing
  auto NumberOfFreeEntries() -> u64;

 private:
  friend class leanstore::buffer::BufferManager;

  template <bool is_extent>
  void LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx);

  tbb::concurrent_queue<pageid_t> free_page_;

  /* Extent management */
  bool enable_extent_tier_;
  std::unique_ptr<sync::RangeLock> free_range_; /* Free range, only used if enable_extent_tier_==false */
  std::array<tbb::concurrent_queue<pageid_t>, ExtentList::NO_TIERS>
    free_tiers_; /* Free extent tier, only used if enable_extent_tier_==true*/
};

}  // namespace leanstore::storage