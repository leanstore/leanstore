#include "sync/page_guard/page_guard.h"
#include "leanstore/leanstore.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
PageGuard<PageClass>::PageGuard() : PageGuard<PageClass>(nullptr, 0, GuardMode::MOVED) {}

template <class PageClass>
PageGuard<PageClass>::PageGuard(buffer::BufferManager *bm, pageid_t pid, GuardMode mode)
    : buffer_(bm), page_id_(pid), mode_(mode) {}

template <class PageClass>
auto PageGuard<PageClass>::operator->() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return reinterpret_cast<PageClass *>(buffer_->ToPtr(page_id_));
}

template <class PageClass>
auto PageGuard<PageClass>::Ptr() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return reinterpret_cast<PageClass *>(buffer_->ToPtr(page_id_));
}

template <class PageClass>
auto PageGuard<PageClass>::Mode() -> GuardMode {
  return mode_;
}

template <class PageClass>
auto PageGuard<PageClass>::PageID() -> pageid_t {
  return page_id_;
}

template <class PageClass>
auto PageGuard<PageClass>::GSN() -> timestamp_t {
  return Ptr()->p_gsn;
}

template <class PageClass>
void PageGuard<PageClass>::DetectGSNDependency() {
  if (FLAGS_wal_enable) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning());
    auto &logger = txn.LogWorker();
    auto buffer  = txn.BufferPool();
    if (GSN() > logger.rfa_gsn_flushed &&
        buffer->BufferFramePage(page_id_).last_writer != LeanStore::worker_thread_id) {
      prev_owner_ = buffer->BufferFramePage(page_id_).last_writer;
      prev_gsn_   = GSN();
    }
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), GSN()));
  }
}

template <class PageClass>
void PageGuard<PageClass>::AdvanceGSN() {
  if (FLAGS_wal_enable) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning());
    // This transaction should already hold X-lock on this page
    auto &logger = txn.LogWorker();
    auto buffer  = txn.BufferPool();
    //  therefore, it's GSN should not be higher than the local copy of GSN
    Ensure(GSN() <= logger.GetCurrentGSN());
    Ptr()->p_gsn                                  = logger.GetCurrentGSN() + 1;
    buffer->BufferFramePage(page_id_).last_writer = LeanStore::worker_thread_id;
    logger.SetCurrentGSN(std::max<timestamp_t>(logger.GetCurrentGSN(), GSN()));
  }
}

template <class PageClass>
void PageGuard<PageClass>::UpdateDependencyVector() {
  /* prev_gsn_ > 0 means somebody modifies the page */
  if (FLAGS_wal_enable && prev_gsn_ > 0) {
    auto &txn = TM::active_txn;
    Ensure(txn.IsRunning() && prev_owner_ != LeanStore::worker_thread_id &&
           prev_owner_ != std::numeric_limits<wid_t>::max());
    txn.gsn_vector[prev_owner_] = prev_gsn_;
  }
}

template <class PageClass>
void PageGuard<PageClass>::SubmitActiveWalEntry() {
  if (FLAGS_wal_enable) {
    Ensure((TM::active_txn.IsRunning()) && (!TM::active_txn.ReadOnly()));
    TM::active_txn.LogWorker().SubmitActiveLogEntry();
  }
}

template class PageGuard<storage::Page>;
template class PageGuard<storage::MetadataPage>;
template class PageGuard<storage::BTreeNode>;

}  // namespace leanstore::sync