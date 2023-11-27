#include "transaction/transaction.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "transaction/transaction_manager.h"

#include <cstring>

namespace leanstore::transaction {

void Transaction::Initialize(TransactionManager *manager, timestamp_t start_ts, Type txn_type, IsolationLevel level,
                             Mode txn_mode, bool read_only) {
  //--------------------------
  manager_      = manager;
  is_read_only_ = read_only;
  type_         = txn_type;
  mode_         = txn_mode;
  iso_level_    = level;

  //--------------------------
  state_     = State::STARTED;
  start_ts_  = start_ts;
  commit_ts_ = 0;

  //--------------------------
  wal_start_          = 0;
  max_observed_gsn_   = std::numeric_limits<logid_t>::max();
  needs_remote_flush_ = false;

  //--------------------------
  to_write_pages_   = storage::LargePageList();
  to_evict_extents_ = storage::LargePageList();
  to_free_extents_  = storage::TierList();
}

auto Transaction::ReadOnly() -> bool { return is_read_only_; }

auto Transaction::StartTS() -> timestamp_t { return start_ts_; }

auto Transaction::CommitTS() -> timestamp_t { return commit_ts_; }

auto Transaction::StartWalCursor() -> u64 { return wal_start_; }

auto Transaction::IsRunning() -> bool { return state_ == State::STARTED; }

void Transaction::MarkAsWrite() { is_read_only_ = false; }

auto Transaction::LogWorker() -> recovery::LogWorker & { return manager_->log_manager_->LocalLogWorker(); }

auto Transaction::ToFlushedLargePages() -> storage::LargePageList & { return to_write_pages_; }

auto Transaction::ToEvictedExtents() -> storage::LargePageList & { return to_evict_extents_; }

auto Transaction::ToFreeExtents() -> storage::TierList & { return to_free_extents_; }

void Transaction::LogBlob(std::span<const u8> payload) {
  Ensure((FLAGS_blob_logging_variant == -1) && IsRunning());

  auto &logger    = LogWorker();
  u64 logged_size = 0;
  for (u8 idx = 0; logged_size < payload.size(); idx++) {
    auto new_log_size = std::min<u64>(FLAGS_blob_log_segment_size, payload.size() - logged_size);
    auto &entry       = logger.ReserveBlobLogEntry(new_log_size, idx);
    std::memcpy(entry.payload, payload.data() + logged_size, new_log_size);
    logger.SubmitActiveLogEntry();

    if (FLAGS_wal_debug) { entry.ComputeChksum(); }
    logged_size += new_log_size;
  }
}

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
        needs_remote_flush_ = true;
      }
    }
    logger.SetCurrentGSN(std::max<logid_t>(logger.GetCurrentGSN(), page->p_gsn));
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
    logger.SetCurrentGSN(std::max<logid_t>(logger.GetCurrentGSN(), page->p_gsn));
  }
}

template void Transaction::DetectGSNDependency<storage::Page>(storage::Page *, pageid_t);
template void Transaction::DetectGSNDependency<storage::MetadataPage>(storage::MetadataPage *, pageid_t);
template void Transaction::DetectGSNDependency<storage::BTreeNode>(storage::BTreeNode *, pageid_t);

template void Transaction::AdvancePageGSN<storage::Page>(storage::Page *, pageid_t);
template void Transaction::AdvancePageGSN<storage::MetadataPage>(storage::MetadataPage *, pageid_t);
template void Transaction::AdvancePageGSN<storage::BTreeNode>(storage::BTreeNode *, pageid_t);

}  // namespace leanstore::transaction