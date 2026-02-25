#include "storage/free_storage.h"
#include "leanstore/schema.h"
#include "leanstore/statistics.h"
#include "recovery/log_worker.h"
#include "transaction/transaction.h"

namespace leanstore::storage {

using TM = transaction::Transaction;

FreeStorageManager::FreeStorageManager() { locked_extent_ = std::make_unique<sync::RangeLock>(FLAGS_worker_count); }

inline void FreeStorageManager::LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  if (!is_free_op) { statistics::storage::free_size -= ExtentList::TIER_SIZE[tier_idx]; }
  txn.LogWorker().ReserveFreeExtentLogEntry(is_free_op, start_pid, ExtentList::ExtentSize(tier_idx));
}

// ------------------------------------------------------------------------------------------------------
auto FreeStorageManager::TryLockRange(pageid_t start_pid, u64 page_count) -> bool {
  return locked_extent_->TryLockRange(start_pid, page_count);
}

/**
 * @brief All the free ranges will be public after the transaction commits
 * If the transaction aborts, then the vector will be clear, i.e. no free range is added to the index
 */
void FreeStorageManager::PrepareFreeTier(pageid_t start_pid, u8 tier) {
  auto &txn = TM::active_txn;
  Ensure(txn.IsRunning());
  txn.ToFreeExtents().emplace_back(start_pid, tier);
  LogFreePage(true, start_pid, tier);
}

void FreeStorageManager::PublicFreeExtents(std::span<const storage::ExtentTier> extents) {
  if (!extents.empty()) {
    for (auto &ext : extents) {
      locked_extent_->UnlockRange(ext.start_pid);
      statistics::storage::free_size += ExtentList::TIER_SIZE[ext.tier];
    }
  }
}

}  // namespace leanstore::storage