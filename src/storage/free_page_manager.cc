#include "storage/free_page_manager.h"
#include "leanstore/statistics.h"
#include "transaction/transaction_manager.h"

namespace leanstore::storage {

inline void FreePageManager::LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx) {
  auto &txn = transaction::TransactionManager::active_txn;
  Ensure(txn.IsRunning());
  if (!is_free_op) { statistics::storage::free_size -= ExtentList::TIER_SIZE[tier_idx]; }
  txn.LogWorker().ReserveFreePageLogEntry(is_free_op, start_pid, ExtentList::ExtentSize(tier_idx));
}

/**
 * @brief Find a free page that is at least `required_page_cnt` pages which satisfies `validate_extent`
 * If there is no suitable range -> return `false`
 *
 * @param required_page_cnt   Minimum number of required page count
 * @param validate_extent     Validation function to check if the free range is suitable
 * @param out_pid             *Output* Start PID of the free extent
 * @param out_split_exts      *Output* Information about all the splitted smaller extents
 */
auto FreePageManager::RequestFreeExtent(u64 required_page_cnt, const std::function<bool(pageid_t)> &validate_extent,
                                        pageid_t &out_pid, TierList &out_split_exts) -> bool {
  if (!FLAGS_fp_enable) { return false; }

  auto tier_index = ExtentList::TierIndex(required_page_cnt, false);
  Ensure(ExtentList::ExtentSize(tier_index) >= required_page_cnt);

  /**
   * @brief We should try to pop() at the exact tier index a few times
   */
  for (auto idx = 0; idx < 3; idx++) {
    if (tiers_[tier_index].empty()) { break; }
    if (tiers_[tier_index].try_pop(out_pid)) {
      if (validate_extent(out_pid)) {
        LogFreePage(false, out_pid, tier_index);
        if (ExtentList::TIER_SIZE[tier_index] > required_page_cnt) {
          PrepareFreePages(out_pid + required_page_cnt, ExtentList::TIER_SIZE[tier_index] - required_page_cnt,
                           out_split_exts);
        }
        return true;
      }

      tiers_[tier_index].push(out_pid);
      AsmYield(idx);
    }
  }

  /**
   * @brief Consider the top tier at the end, because the number of biggest extents should be as high as possible
   */
  for (auto i = ExtentList::NO_TIERS - 2; i >= tier_index; i--) {
    auto x_tier = (i == tier_index) ? ExtentList::NO_TIERS - 1 : i;

    if (tiers_[x_tier].try_pop(out_pid)) {
      if (validate_extent(out_pid)) {
        LogFreePage(false, out_pid, x_tier);
        PrepareFreePages(out_pid + required_page_cnt, ExtentList::TIER_SIZE[x_tier] - required_page_cnt,
                         out_split_exts);
        return true;
      }

      tiers_[x_tier].push(out_pid);
    }
  }

  return false;
}

/**
 * @brief All the free ranges will be public after the transaction commits
 * If the transaction aborts, then the vector will be clear, i.e. no free range is added to the index
 */
void FreePageManager::PrepareFreeTier(pageid_t start_pid, u8 tier_index) {
  if (FLAGS_fp_enable) {
    auto &txn = transaction::TransactionManager::active_txn;
    Ensure(txn.IsRunning());
    txn.ToFreeExtents().emplace_back(start_pid, tier_index);
    LogFreePage(true, start_pid, tier_index);
  }
}

void FreePageManager::PublicFreeRanges(const storage::TierList &free_extents) {
  if (!free_extents.empty()) {
    for (auto &range : free_extents) {
      tiers_[range.tier_index].push(range.start_pid);
      statistics::storage::free_size += ExtentList::TIER_SIZE[range.tier_index];
    }
  }
}

/**
 * @brief Free a large page, starting at `start_pid` and lasting for `page_cnt` pages
 *  into multiple extents which fit random tiers
 */
void FreePageManager::PrepareFreePages(pageid_t start_pid, u64 page_cnt, TierList &out_split_exts) {
  storage::TailExtent::SplitToExtents(start_pid, page_cnt, [&](pageid_t start_pid, extidx_t index) {
    PrepareFreeTier(start_pid, index);
    out_split_exts.emplace_back(start_pid, index);
  });
}

/**
 * @brief *WARNING*: Should only be used for testing, unsafe in prod env
 */
auto FreePageManager::NumberOfFreeEntries() -> u64 {
  auto ret = 0;
  for (auto &tier : tiers_) { ret += tier.unsafe_size(); }
  return ret;
}

}  // namespace leanstore::storage