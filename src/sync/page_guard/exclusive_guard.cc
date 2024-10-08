#include "sync/page_guard/exclusive_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
ExclusiveGuard<PageClass>::ExclusiveGuard(buffer::BufferManager *buffer, pageid_t pid)
    : PageGuard<PageClass>(buffer, pid, GuardMode::EXCLUSIVE) {
  buffer->FixExclusive(pid);
  this->DetectGSNDependency();
}

/**
 * @brief Construct a new ExclusiveGuard<PageClass> object, specifically used for page allocation
 *
 * The Database should already hold an X-lock on this page (i.e. alloc_page)
 */
template <class PageClass>
ExclusiveGuard<PageClass>::ExclusiveGuard(buffer::BufferManager *buffer, storage::Page *alloc_page)
    : PageGuard<PageClass>(buffer, buffer->ToPID(alloc_page), GuardMode::EXCLUSIVE) {
  assert(buffer->GetPageState(buffer->ToPID(alloc_page)).LockState() == sync::PageState::EXCLUSIVE);
  this->DetectGSNDependency();
}

template <class PageClass>
ExclusiveGuard<PageClass>::ExclusiveGuard(OptimisticGuard<PageClass> &&other) noexcept(false) {
  assert(other.mode_ == GuardMode::OPTIMISTIC);
  for (auto cnt = 0;; cnt++) {
    auto &ps            = other.buffer_->GetPageState(other.page_id_);
    u64 state_w_version = ps.StateAndVersion().load();
    if (PageState::Version(state_w_version) != PageState::Version(other.old_version_)) { throw RestartException(); }

    u64 state = PageState::LockState(state_w_version);
    if ((state == PageState::UNLOCKED) || (state == PageState::MARKED)) {
      if (ps.TryLockExclusive(state_w_version)) {
        this->page_id_ = other.page_id_;
        this->buffer_  = other.buffer_;
        this->mode_    = GuardMode::EXCLUSIVE;
        other.mode_    = GuardMode::MOVED;
        other.buffer_  = nullptr;
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
  if (this->mode_ == GuardMode::EXCLUSIVE) { this->buffer_->GetPageState(this->page_id_).UnlockExclusive(); }

  this->page_id_ = other.page_id_;
  this->buffer_  = other.buffer_;
  this->mode_    = GuardMode::EXCLUSIVE;
  other.mode_    = GuardMode::MOVED;
  other.buffer_  = nullptr;

  return *this;
}

template <class PageClass>
ExclusiveGuard<PageClass>::~ExclusiveGuard() noexcept {
  this->Unlock();
}

template <class PageClass>
void ExclusiveGuard<PageClass>::Unlock() {
  if (this->mode_ == GuardMode::EXCLUSIVE) {
    this->buffer_->GetPageState(this->page_id_).UnlockExclusive();
    this->mode_ = GuardMode::MOVED;
  }
}

template <class PageClass>
auto ExclusiveGuard<PageClass>::UnlockAndGetPtr() -> PageClass * {
  if (this->mode_ == GuardMode::EXCLUSIVE) {
    this->buffer_->GetPageState(this->page_id_).UnlockExclusive();
    this->mode_ = GuardMode::MOVED;
    return reinterpret_cast<PageClass *>(this->buffer_->ToPtr(this->page_id_));
  }
  return nullptr;
}

template class ExclusiveGuard<storage::Page>;
template class ExclusiveGuard<storage::MetadataPage>;
template class ExclusiveGuard<storage::BTreeNode>;

}  // namespace leanstore::sync