#include "sync/page_guard/optimistic_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
OptimisticGuard<PageClass>::OptimisticGuard(pageid_t pid, PageClass *ptr, PageState *page_state,
                                            std::function<void(pageid_t)> &&page_loader_func)
    : PageGuard<PageClass>(pid, ptr, page_state, GuardMode::OPTIMISTIC),
      page_loader_func_(std::move(page_loader_func)) {
  this->Init();
  this->DetectGSNDependency();
}

template <class PageClass>
OptimisticGuard<PageClass>::OptimisticGuard(OptimisticGuard<PageClass> &&other) noexcept {
  assert(this->mode_ == GuardMode::OPTIMISTIC);
  this->page_id_          = other.page_id_;
  this->ps_               = other.ps_;
  this->page_ptr_         = other.page_ptr_;
  this->mode_             = other.mode_;
  this->old_version_      = other.old_version_;
  this->page_loader_func_ = other.page_loader_func_;
  other.mode_             = GuardMode::MOVED;
  other.page_ptr_         = nullptr;
}

template <class PageClass>
auto OptimisticGuard<PageClass>::operator=(OptimisticGuard<PageClass> &&other) noexcept(false) -> OptimisticGuard & {
  if (this->mode_ == GuardMode::OPTIMISTIC) { this->ValidateOrRestart(); }
  this->page_id_          = other.page_id_;
  this->ps_               = other.ps_;
  this->page_ptr_         = other.page_ptr_;
  this->mode_             = other.mode_;
  this->old_version_      = other.old_version_;
  this->page_loader_func_ = other.page_loader_func_;
  other.mode_             = GuardMode::MOVED;
  other.page_ptr_         = nullptr;
  return *this;
}

template <class PageClass>
OptimisticGuard<PageClass>::~OptimisticGuard() noexcept(false) {
  ValidateOrRestart();
}

template <class PageClass>
void OptimisticGuard<PageClass>::Init() {
  assert(this->mode_ == GuardMode::OPTIMISTIC);
  for (auto cnt = 0;; cnt++) {
    u64 v = this->ps_->StateAndVersion().load();
    switch (PageState::LockState(v)) {
      case PageState::EXCLUSIVE: break;
      case PageState::MARKED: {
        u64 new_v;
        if (this->ps_->Unmark(v, new_v)) {
          this->old_version_ = new_v;
          return;
        }
        break;
      }
      case PageState::EVICTED:
        if (this->ps_->TryLockExclusive(v)) {
          this->page_loader_func_(this->page_id_);
          this->ps_->UnlockExclusive();
        }
        break;
      default: this->old_version_ = v; return;
    }
    AsmYield(cnt);
  }
}

template <class PageClass>
void OptimisticGuard<PageClass>::ValidateOrRestart(bool auto_moved) {
  // it's possible that this guard has been moved, return
  if (this->mode_ == GuardMode::MOVED) { return; }
  assert(this->mode_ == GuardMode::OPTIMISTIC);
  if (auto_moved) {
    // mark MOVED to prevent unnecessary 2nd validation
    this->mode_ = GuardMode::MOVED;
  }
  auto current_version = this->ps_->StateAndVersion().load();
  if (this->old_version_ == current_version) {
    // fast path - nothing changed
    return;
  }
  // Same version, start checking
  if (PageState::Version(this->old_version_) == PageState::Version(current_version)) {
    auto state = PageState::LockState(current_version);
    if (state <= PageState::MAX_SHARED) {
      // ignore shared locks
      return;
    }
    if (state == PageState::MARKED) {
      u64 unused;
      if (this->ps_->Unmark(current_version, unused)) {
        // MARKED clear successfully
        return;
      }
    }
  }
  // The Optimistic check failed.
  // If none throws Optimistic Exception, then we throw it here
  if (std::uncaught_exceptions() == 0) { throw RestartException(); }
}

template class OptimisticGuard<storage::Page>;
template class OptimisticGuard<storage::MetadataPage>;
template class OptimisticGuard<storage::BTreeNode>;

}  // namespace leanstore::sync