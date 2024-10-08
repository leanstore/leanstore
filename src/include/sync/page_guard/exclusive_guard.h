#pragma once

#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/page_guard.h"
#include "sync/page_state.h"

namespace leanstore::sync {

/* Caller to ExclusiveGuard should X-lock the PageState before construct the Guard */
template <class PageClass>
class ExclusiveGuard : public PageGuard<PageClass> {
 public:
  ExclusiveGuard() : PageGuard<PageClass>::PageGuard() {}

  ExclusiveGuard(buffer::BufferManager *buffer, pageid_t pid);
  ExclusiveGuard(buffer::BufferManager *buffer, storage::Page *alloc_page);
  explicit ExclusiveGuard(OptimisticGuard<PageClass> &&other) noexcept(false);
  auto operator=(ExclusiveGuard &&other) noexcept(false) -> ExclusiveGuard &;
  ~ExclusiveGuard() noexcept;

  void Unlock();
  auto UnlockAndGetPtr() -> PageClass *;
};

}  // namespace leanstore::sync