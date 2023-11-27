#include "sync/page_guard/shared_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
SharedGuard<PageClass>::SharedGuard(pageid_t pid, PageClass *ptr, PageState *page_state)
    : PageGuard<PageClass>(pid, ptr, page_state, GuardMode::SHARED) {
  this->DetectGSNDependency();
}

template <class PageClass>
SharedGuard<PageClass>::SharedGuard(OptimisticGuard<PageClass> &&other) noexcept(false) {
  assert(other.mode_ == GuardMode::OPTIMISTIC);
  if (!other.ps_->TryLockShared(other.old_version_)) { throw RestartException(); }

  this->page_id_  = other.page_id_;
  this->ps_       = other.ps_;
  this->page_ptr_ = other.page_ptr_;
  this->mode_     = GuardMode::SHARED;
  other.mode_     = GuardMode::MOVED;
  other.page_ptr_ = nullptr;
}

template <class PageClass>
auto SharedGuard<PageClass>::operator=(SharedGuard<PageClass> &&other) noexcept(false) -> SharedGuard & {
  assert(other.mode_ == GuardMode::SHARED);
  if (this->mode_ == GuardMode::SHARED) { this->ps_->UnlockShared(); }

  this->page_id_  = other.page_id_;
  this->ps_       = other.ps_;
  this->page_ptr_ = other.page_ptr_;
  this->mode_     = GuardMode::SHARED;
  other.mode_     = GuardMode::MOVED;
  other.page_ptr_ = nullptr;

  return *this;
}

template <class PageClass>
SharedGuard<PageClass>::SharedGuard(SharedGuard<PageClass> &&other) noexcept {
  *this = std::move(other);
}

template <class PageClass>
SharedGuard<PageClass>::~SharedGuard() noexcept {
  this->Unlock();
}

template <class PageClass>
void SharedGuard<PageClass>::Unlock() {
  if (this->mode_ == GuardMode::SHARED) {
    this->ps_->UnlockShared();
    this->mode_ = GuardMode::MOVED;
  }
}

template class SharedGuard<storage::Page>;
template class SharedGuard<storage::MetadataPage>;
template class SharedGuard<storage::BTreeNode>;

}  // namespace leanstore::sync