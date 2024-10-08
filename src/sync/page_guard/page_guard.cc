#include "sync/page_guard/page_guard.h"
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
void PageGuard<PageClass>::DetectGSNDependency() {
  TM::active_txn.DetectGSNDependency(buffer_->ToPtr(page_id_), this->page_id_);
}

template <class PageClass>
void PageGuard<PageClass>::AdvanceGSN() {
  TM::active_txn.AdvancePageGSN(buffer_->ToPtr(page_id_), this->page_id_);
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