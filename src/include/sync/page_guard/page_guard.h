#pragma once

#include "common/exceptions.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "sync/hybrid_guard.h"
#include "sync/page_state.h"
#include "transaction/transaction_manager.h"

#include <functional>
#include <type_traits>

using TM = leanstore::transaction::TransactionManager;

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::sync {

template <class PageClass>
class SharedGuard;
template <class PageClass>
class ExclusiveGuard;

template <class PageClass>
class PageGuard {
 public:
  PageGuard();
  PageGuard(buffer::BufferManager *bm, pageid_t pid, GuardMode mode);
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
  auto PrepareWalEntry(u64 wal_payload_size, u8 *prepared_buffer = nullptr) -> WalType & {
    // We have too many WAL types, hence impl this func here helps reduce the code size
    assert(FLAGS_wal_enable);
    // Only write txn + the page pid has been X-locked can reserve WAL log entry
    assert(mode_ == GuardMode::EXCLUSIVE);
    assert((TM::active_txn.IsRunning()) && (!TM::active_txn.ReadOnly()));
    // Synchronize GSN before WAL reservation
    AdvanceGSN();
    // Generate new WAL entry
    auto &logger = TM::active_txn.LogWorker();
    auto &entry  = (prepared_buffer == nullptr)
                     ? static_cast<WalType &>(logger.ReserveDataLog(sizeof(WalType) + wal_payload_size, page_id_))
                     : static_cast<WalType &>(
                        logger.PrepareDataLogEntry(prepared_buffer, sizeof(WalType) + wal_payload_size, page_id_));
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
    } else if (std::is_same_v<WalType, storage::WALDeltaImage>) {
      entry.type = storage::WalType::DELTA_IMAGE;
    } else {
      UnreachableCode();
    }
    return entry;
  }

  void SubmitActiveWalEntry();

 protected:
  buffer::BufferManager *buffer_;  // Buffer Manager
  pageid_t page_id_;               // The ID of the Page
  GuardMode mode_;                 // GuardMode of this Page Guard
};

/**
 * @brief Defer appending log entries until the ExclusiveLatch is completed
 */
template <class PageClass>
class DeferLog {
 public:
  // Max size of data log entry
  static constexpr auto MAX_LOG_SIZE = sizeof(recovery::DataEntry) + storage::BTreeNode::MAX_RECORD_SIZE;

  DeferLog() = default;

  ~DeferLog() {
    if (has_constructed_) {
      assert((TM::active_txn.IsRunning()) && (!TM::active_txn.ReadOnly()));
      TM::active_txn.LogWorker().SubmitPreparedLogEntry();
    }
  }

  template <typename WalType>
  void Construct(PageGuard<PageClass> &page, std::span<u8> key, std::span<const u8> payload) {
    auto &entry = page.template PrepareWalEntry<WalType>(key.size() + payload.size(), buffer_);
    std::tie(entry.key_length, entry.value_length) = std::make_tuple(key.size(), payload.size());
    std::memcpy(entry.payload, key.data(), key.size());
    std::memcpy(entry.payload + key.size(), payload.data(), payload.size());
    has_constructed_ = true;
  }

 private:
  u8 buffer_[MAX_LOG_SIZE];
  bool has_constructed_ = false;
};

}  // namespace leanstore::sync