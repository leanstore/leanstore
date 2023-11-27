#pragma once

#include "common/exceptions.h"
#include "storage/btree/wal.h"
#include "sync/hybrid_guard.h"
#include "sync/page_state.h"
#include "transaction/transaction_manager.h"

#include <functional>
#include <type_traits>

using TM = leanstore::transaction::TransactionManager;

namespace leanstore::sync {

template <class PageClass>
class SharedGuard;
template <class PageClass>
class ExclusiveGuard;

template <class PageClass>
class PageGuard {
 public:
  PageGuard();
  PageGuard(pageid_t pid, PageClass *ptr, PageState *page_state, GuardMode mode);
  PageGuard(const PageGuard &)      = delete;
  ~PageGuard()                      = default;
  auto operator=(const PageGuard &) = delete;
  auto operator->() -> PageClass *;

  // Access private members
  auto Mode() -> GuardMode;
  auto Ptr() -> PageClass *;
  auto PageID() -> pageid_t;

  /**** Transaction utilities *****/

  // RFA utilities
  void DetectGSNDependency();
  void AdvanceGSN();

  // WAL helpers
  template <typename WalType>
  auto ReserveWalEntry(u64 wal_payload_size) -> WalType & {
    // We have too many WAL types, hence impl this func here helps reduce the code size
    assert(FLAGS_wal_enable);
    // Only write txn + the page pid has been X-locked can reserve WAL log entry
    assert(mode_ == GuardMode::EXCLUSIVE);
    Ensure((TM::active_txn.IsRunning()) && (!TM::active_txn.ReadOnly()));
    // Synchronize GSN before WAL reservation
    AdvanceGSN();
    // Generate new WAL entry
    auto &logger = TM::active_txn.LogWorker();
    auto &entry =
      static_cast<WalType &>(logger.ReserveDataLog(sizeof(WalType) + wal_payload_size, page_id_, page_ptr_->idx_id));
    // Update its WAL type
    if (std::is_same_v<WalType, storage::WALInitPage>) {
      entry.type = storage::WalType::INIT_PAGE;
    } else if (std::is_same_v<WalType, storage::WALNewRoot>) {
      entry.type = storage::WalType::NEW_ROOT;
    } else if (std::is_same_v<WalType, storage::WALLogicalSplit>) {
      entry.type = storage::WalType::LOGICAL_SPLIT;
    } else if (std::is_same_v<WalType, storage::WALMergeNodes>) {
      entry.type = storage::WalType::MERGE_NODES;
    } else if (std::is_same_v<WalType, storage::WALInsert>) {
      entry.type = storage::WalType::INSERT;
    } else if (std::is_same_v<WalType, storage::WALAfterImage>) {
      entry.type = storage::WalType::AFTER_IMAGE;
    } else if (std::is_same_v<WalType, storage::WALRemove>) {
      entry.type = storage::WalType::REMOVE;
    } else {
      UnreachableCode();
    }
    return entry;
  }

  void SubmitActiveWalEntry();

 protected:
  pageid_t page_id_;     // The ID of the Page
  PageClass *page_ptr_;  // Pointer to the Page (to access the data of the Page)
  PageState *ps_;        // The State of the Page
  GuardMode mode_;       // GuardMode of this Page Guard
};

}  // namespace leanstore::sync