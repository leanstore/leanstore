#include "transaction/transaction.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "transaction/transaction_manager.h"

#include <cstring>

namespace leanstore::transaction {

SerializableTransaction::SerializableTransaction(const Transaction &txn) { Construct(txn); }

void SerializableTransaction::Construct(const Transaction &txn) {
  state              = txn.state;
  stats              = txn.stats;
  start_ts           = txn.start_ts;
  commit_ts          = txn.commit_ts;
  needs_remote_flush = txn.needs_remote_flush;
  max_observed_gsn   = txn.max_observed_gsn;
  no_write_pages     = txn.to_write_pages_.size();
  no_evict_extents   = txn.to_evict_extents_.size();
  no_free_extents    = txn.to_free_extents_.size();
  std::memcpy(content, txn.to_write_pages_.data(), no_write_pages * sizeof(storage::LargePage));
  std::memcpy(&content[OffsetEvictExtents()], txn.to_evict_extents_.data(), no_evict_extents * sizeof(pageid_t));
  std::memcpy(&content[OffsetFreeExtents()], txn.to_free_extents_.data(),
              no_free_extents * sizeof(storage::ExtentTier));
}

/** Whether the given byte buffer does not store a valid SerializableTransaction */
auto SerializableTransaction::InvalidByteBuffer(const u8 *buffer) -> bool { return buffer[0] == NULL_ITEM; }

auto SerializableTransaction::MemorySize() -> u16 {
  return sizeof(SerializableTransaction) + no_write_pages * sizeof(storage::LargePage) +
         no_evict_extents * sizeof(pageid_t) + no_free_extents * sizeof(storage::ExtentTier);
}

// -------------------------------------------------------------------------------------

void Transaction::Initialize(TransactionManager *manager, timestamp_t start_timestamp, Type txn_type,
                             IsolationLevel level, Mode txn_mode, bool read_only) {
  //--------------------------
  manager_      = manager;
  is_read_only_ = read_only;
  type_         = txn_type;
  mode_         = txn_mode;
  iso_level_    = level;

  //--------------------------
  state     = State::STARTED;
  start_ts  = start_timestamp;
  commit_ts = 0;

  //--------------------------
  max_observed_gsn   = std::numeric_limits<timestamp_t>::max();
  needs_remote_flush = false;

  //--------------------------
  to_write_pages_   = storage::LargePageList();
  to_evict_extents_ = std::vector<pageid_t>();
  to_free_extents_  = storage::TierList();
}

auto Transaction::SerializedSize() const -> u64 {
  return sizeof(SerializableTransaction) + to_write_pages_.size() * sizeof(storage::LargePage) +
         to_evict_extents_.size() * sizeof(pageid_t) + to_free_extents_.size() * sizeof(storage::ExtentTier);
}

auto Transaction::LogWorker() -> recovery::LogWorker & { return manager_->log_manager_->LocalLogWorker(); }

auto Transaction::ReadOnly() -> bool { return is_read_only_; }

auto Transaction::IsRunning() -> bool { return state == State::STARTED; }

void Transaction::MarkAsWrite() { is_read_only_ = false; }

auto Transaction::HasBLOB() -> bool {
  return !to_write_pages_.empty() || !to_evict_extents_.empty() || !to_free_extents_.empty();
}

auto Transaction::ToFlushedLargePages() -> storage::LargePageList & { return to_write_pages_; }

auto Transaction::ToEvictedExtents() -> std::vector<pageid_t> & { return to_evict_extents_; }

auto Transaction::ToFreeExtents() -> storage::TierList & { return to_free_extents_; }

/**
 * @brief Determine if this txn requires remote flush
 */
template <class PageClass>
void Transaction::DetectGSNDependency(PageClass *page, pageid_t pid) {
  if (FLAGS_wal_enable) {
    auto &logger = LogWorker();
    auto buffer  = manager_->buffer_;
    if (FLAGS_wal_enable_rfa) {
      if (page->p_gsn > logger.rfa_gsn_flushed && buffer->BufferFrame(pid).last_writer != worker_thread_id) {
        needs_remote_flush = true;
      }
    }
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), page->p_gsn));
  }
}

/**
 * @brief Advance the Page's GSN using the active transaction's local copy of GSN
 */
template <class PageClass>
void Transaction::AdvancePageGSN(PageClass *page, pageid_t pid) {
  if (FLAGS_wal_enable) {
    // This transaction should already hold X-lock on this page
    auto &logger = LogWorker();
    auto buffer  = manager_->buffer_;
    //  therefore, it's GSN should not be higher than the local copy of GSN
    Ensure(page->p_gsn <= logger.GetCurrentGSN());
    page->p_gsn                          = logger.GetCurrentGSN() + 1;
    buffer->BufferFrame(pid).last_writer = worker_thread_id;
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), page->p_gsn));
  }
}

template void Transaction::DetectGSNDependency<storage::Page>(storage::Page *, pageid_t);
template void Transaction::DetectGSNDependency<storage::MetadataPage>(storage::MetadataPage *, pageid_t);
template void Transaction::DetectGSNDependency<storage::BTreeNode>(storage::BTreeNode *, pageid_t);

template void Transaction::AdvancePageGSN<storage::Page>(storage::Page *, pageid_t);
template void Transaction::AdvancePageGSN<storage::MetadataPage>(storage::MetadataPage *, pageid_t);
template void Transaction::AdvancePageGSN<storage::BTreeNode>(storage::BTreeNode *, pageid_t);

}  // namespace leanstore::transaction