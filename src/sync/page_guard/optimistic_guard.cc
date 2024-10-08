#include "sync/page_guard/optimistic_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
OptimisticGuard<PageClass>::OptimisticGuard(buffer::BufferManager *buffer, pageid_t pid)
    : PageGuard<PageClass>(buffer, pid, GuardMode::OPTIMISTIC) {
  this->Init();
  this->DetectGSNDependency();
}

template <class PageClass>
OptimisticGuard<PageClass>::OptimisticGuard(OptimisticGuard<PageClass> &&other) noexcept
    : PageGuard<PageClass>(other.buffer_, other.page_id_, other.mode_), old_version_(other.old_version_) {
  assert(this->mode_ == GuardMode::OPTIMISTIC);
  other.mode_   = GuardMode::MOVED;
  other.buffer_ = nullptr;
}

template <class PageClass>
auto OptimisticGuard<PageClass>::operator=(OptimisticGuard<PageClass> &&other) noexcept(false) -> OptimisticGuard & {
  if (this->mode_ == GuardMode::OPTIMISTIC) { this->ValidateOrRestart(); }
  this->page_id_     = other.page_id_;
  this->buffer_      = other.buffer_;
  this->mode_        = other.mode_;
  this->old_version_ = other.old_version_;
  other.mode_        = GuardMode::MOVED;
  other.buffer_      = nullptr;
  return *this;
}

template <class PageClass>
template <class PageClass2>
OptimisticGuard<PageClass>::OptimisticGuard(buffer::BufferManager *buffer, pageid_t pid,
                                            OptimisticGuard<PageClass2> &parent) {
  // parent could be OptimisticGuard multiple times, so we shouldn't MOVED that guard
  parent.ValidateOrRestart(false);
  // We need to validate parent first to guarantee we retrieve the correct pid
  PageGuard<PageClass>::page_id_ = pid;
  PageGuard<PageClass>::buffer_  = buffer;
  PageGuard<PageClass>::mode_    = sync::GuardMode::OPTIMISTIC;
  Init();
}

template <class PageClass>
OptimisticGuard<PageClass>::~OptimisticGuard() noexcept(false) {
  ValidateOrRestart();
}

template <class PageClass>
void OptimisticGuard<PageClass>::Init() {
  assert(this->mode_ == GuardMode::OPTIMISTIC);
  auto &ps = this->buffer_->GetPageState(this->page_id_);

  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (PageState::LockState(v)) {
      case PageState::EXCLUSIVE: break;
      case PageState::MARKED: {
        u64 new_v;
        if (ps.Unmark(v, new_v)) {
          this->old_version_ = new_v;
          return;
        }
        break;
      }
      case PageState::EVICTED:
        if (ps.TryLockExclusive(v)) {
          this->buffer_->HandlePageFault(this->page_id_);
          ps.UnlockExclusive();
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

  auto &ps             = this->buffer_->GetPageState(this->page_id_);
  auto current_version = ps.StateAndVersion().load();
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
      if (ps.Unmark(current_version, unused)) {
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

// Optimistic Lock-Coupling facility
template OptimisticGuard<storage::BTreeNode>::OptimisticGuard(buffer::BufferManager *, pageid_t,
                                                              OptimisticGuard<storage::MetadataPage> &);
template OptimisticGuard<storage::BTreeNode>::OptimisticGuard(buffer::BufferManager *, pageid_t,
                                                              OptimisticGuard<storage::BTreeNode> &);

}  // namespace leanstore::sync