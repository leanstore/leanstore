#include "storage/free_storage.h"
#include "leanstore/schema.h"
#include "leanstore/statistics.h"
#include "transaction/transaction_manager.h"

namespace leanstore::storage {

FreeStorageManager::FreeStorageManager() : enable_extent_tier_(FLAGS_blob_buffer_pool_gb == 0) {
  if (!enable_extent_tier_) { free_range_ = std::make_unique<sync::RangeLock>(FLAGS_worker_count); }
}

template <bool is_extent>
inline void FreeStorageManager::LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx) {
  auto &txn = transaction::TransactionManager::active_txn;
  Ensure(txn.IsRunning());
  if (!is_free_op) { statistics::storage::free_size -= ExtentList::TIER_SIZE[tier_idx]; }
  if constexpr (is_extent) {
    txn.LogWorker().ReserveFreeExtentLogEntry(is_free_op, start_pid, ExtentList::ExtentSize(tier_idx));
  } else {
    txn.LogWorker().ReserveFreePageLogEntry(is_free_op, start_pid);
  }
}

/**
 * @brief *WARNING*: Should only be used for testing, unsafe in prod env
 */
auto FreeStorageManager::NumberOfFreeEntries() -> u64 {
  auto ret = 0;
  for (auto &tier : free_tiers_) { ret += tier.unsafe_size(); }
  return ret;
}

// ------------------------------------------------------------------------------------------------------
auto FreeStorageManager::TryLockRange(pageid_t start_pid, u64 page_count) -> bool {
  Ensure(!enable_extent_tier_);
  return free_range_->TryLockRange(start_pid, page_count);
}

/**
 * @brief Find a free page that is at least `page_cnt` pages which satisfies `validate_extent`
 * If there is no suitable range -> return `false`
 *
 * @param page_cnt            Number of required page count
 * @param validate_extent     Validation function to check if the free range is suitable
 * @param out_pid             *Output* Start PID of the free extent
 */
auto FreeStorageManager::RequestFreeExtent(u64 page_cnt, const std::function<bool(pageid_t)> &validate_extent,
                                           pageid_t &out_pid) -> bool {
  Ensure(enable_extent_tier_);
  auto tier_index = ExtentList::TierIndex(page_cnt);
  Ensure(ExtentList::ExtentSize(tier_index) == page_cnt);

  /**
   * @brief We should try to pop() at the exact tier index a few times
   */
  for (auto idx = 0; idx < 3; idx++) {
    if (free_tiers_[tier_index].empty()) { break; }
    if (free_tiers_[tier_index].try_pop(out_pid)) {
      if (validate_extent(out_pid)) {
        LogFreePage<true>(false, out_pid, tier_index);
        return true;
      }

      free_tiers_[tier_index].push(out_pid);
      AsmYield(idx);
    }
  }

  return false;
}

/**
 * @brief All the free ranges will be public after the transaction commits
 * If the transaction aborts, then the vector will be clear, i.e. no free range is added to the index
 */
void FreeStorageManager::PrepareFreeTier(pageid_t start_pid, u8 tier) {
  auto &txn = transaction::TransactionManager::active_txn;
  Ensure(txn.IsRunning());
  txn.ToFreeExtents().emplace_back(start_pid, tier);
  LogFreePage<true>(true, start_pid, tier);
}

void FreeStorageManager::PublicFreeExtents(std::span<storage::ExtentTier> extents) {
  if (!extents.empty()) {
    for (auto &ext : extents) {
      if (enable_extent_tier_) {
        free_tiers_[ext.tier].push(ext.start_pid);
      } else {
        free_range_->UnlockRange(ext.start_pid);
      }
      statistics::storage::free_size += ExtentList::TIER_SIZE[ext.tier];
    }
  }
}

}  // namespace leanstore::storage