#include "sync/page_guard/shared_guard.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "sync/page_state.h"

#include <stdexcept>

namespace leanstore::sync {

template <class PageClass>
SharedGuard<PageClass>::SharedGuard(buffer::BufferManager *buffer, pageid_t pid)
    : PageGuard<PageClass>(buffer, pid, GuardMode::SHARED) {
  buffer->FixShare(pid);
  this->DetectGSNDependency();
}

template <class PageClass>
SharedGuard<PageClass>::SharedGuard(OptimisticGuard<PageClass> &&other) noexcept(false) {
  assert(other.mode_ == GuardMode::OPTIMISTIC);
  if (!other.buffer_->GetPageState(other.page_id_).TryLockShared(other.old_version_)) { throw RestartException(); }

  this->page_id_ = other.page_id_;
  this->buffer_  = other.buffer_;
  this->mode_    = GuardMode::SHARED;
  other.mode_    = GuardMode::MOVED;
  other.buffer_  = nullptr;
}

template <class PageClass>
auto SharedGuard<PageClass>::operator=(SharedGuard<PageClass> &&other) noexcept(false) -> SharedGuard & {
  assert(other.mode_ == GuardMode::SHARED);
  if (this->mode_ == GuardMode::SHARED) { this->buffer_->GetPageState(this->page_id_).UnlockShared(); }

  this->page_id_ = other.page_id_;
  this->buffer_  = other.buffer_;
  this->mode_    = GuardMode::SHARED;
  other.mode_    = GuardMode::MOVED;
  other.buffer_  = nullptr;

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
    this->buffer_->GetPageState(this->page_id_).UnlockShared();
    this->mode_ = GuardMode::MOVED;
  }
}

template class SharedGuard<storage::Page>;
template class SharedGuard<storage::MetadataPage>;
template class SharedGuard<storage::BTreeNode>;

}  // namespace leanstore::sync