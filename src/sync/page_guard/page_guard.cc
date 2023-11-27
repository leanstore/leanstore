#include "sync/page_guard/page_guard.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
PageGuard<PageClass>::PageGuard() : PageGuard<PageClass>(0, nullptr, nullptr, GuardMode::MOVED) {}

template <class PageClass>
PageGuard<PageClass>::PageGuard(pageid_t pid, PageClass *ptr, PageState *page_state, GuardMode mode)
    : page_id_(pid), page_ptr_(ptr), ps_(page_state), mode_(mode) {}

template <class PageClass>
auto PageGuard<PageClass>::operator->() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return page_ptr_;
}

template <class PageClass>
auto PageGuard<PageClass>::Ptr() -> PageClass * {
  if (mode_ == GuardMode::MOVED) { throw leanstore::ex::GenericException("Can't access MOVED PageGuard"); }
  return page_ptr_;
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
  TM::active_txn.DetectGSNDependency(this->page_ptr_, this->page_id_);
}

template <class PageClass>
void PageGuard<PageClass>::AdvanceGSN() {
  TM::active_txn.AdvancePageGSN(this->page_ptr_, this->page_id_);
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