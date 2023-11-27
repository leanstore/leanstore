#include "sync/page_guard/exclusive_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
ExclusiveGuard<PageClass>::ExclusiveGuard(pageid_t pid, PageClass *ptr, PageState *page_state)
    : PageGuard<PageClass>(pid, ptr, page_state, GuardMode::EXCLUSIVE) {
  ptr->dirty = true;
  this->DetectGSNDependency();
}

template <class PageClass>
ExclusiveGuard<PageClass>::ExclusiveGuard(OptimisticGuard<PageClass> &&other) noexcept(false) {
  assert(other.mode_ == GuardMode::OPTIMISTIC);
  for (auto cnt = 0;; cnt++) {
    auto &ps            = other.ps_;
    u64 state_w_version = ps->StateAndVersion().load();
    if (PageState::Version(state_w_version) != PageState::Version(other.old_version_)) { throw RestartException(); }
    u64 state = PageState::LockState(state_w_version);
    if ((state == PageState::UNLOCKED) || (state == PageState::MARKED)) {
      if (ps->TryLockExclusive(state_w_version)) {
        this->page_id_  = other.page_id_;
        this->ps_       = other.ps_;
        this->page_ptr_ = other.page_ptr_;
        this->mode_     = GuardMode::EXCLUSIVE;
        other.mode_     = GuardMode::MOVED;
        other.page_ptr_ = nullptr;
        // Mark the page ptr as dirty
        this->page_ptr_->dirty = true;
        break;
      }
    }
    AsmYield(cnt);
  }
  this->DetectGSNDependency();
}

template <class PageClass>
auto ExclusiveGuard<PageClass>::operator=(ExclusiveGuard<PageClass> &&other) noexcept(false) -> ExclusiveGuard & {
  assert(other.mode_ == GuardMode::EXCLUSIVE);
  if (this->mode_ == GuardMode::EXCLUSIVE) { this->ps_->UnlockExclusive(); }

  this->page_id_  = other.page_id_;
  this->ps_       = other.ps_;
  this->page_ptr_ = other.page_ptr_;
  this->mode_     = GuardMode::EXCLUSIVE;
  other.mode_     = GuardMode::MOVED;
  other.page_ptr_ = nullptr;

  return *this;
}

template <class PageClass>
ExclusiveGuard<PageClass>::~ExclusiveGuard() noexcept {
  this->Unlock();
}

template <class PageClass>
void ExclusiveGuard<PageClass>::Unlock() {
  if (this->mode_ == GuardMode::EXCLUSIVE) {
    this->ps_->UnlockExclusive();
    this->mode_ = GuardMode::MOVED;
  }
}

template <class PageClass>
auto ExclusiveGuard<PageClass>::UnlockAndGetPtr() -> PageClass * {
  if (this->mode_ == GuardMode::EXCLUSIVE) {
    this->ps_->UnlockExclusive();
    this->mode_ = GuardMode::MOVED;
    return this->page_ptr_;
  }
  return nullptr;
}

template class ExclusiveGuard<storage::Page>;
template class ExclusiveGuard<storage::MetadataPage>;
template class ExclusiveGuard<storage::BTreeNode>;

}  // namespace leanstore::sync