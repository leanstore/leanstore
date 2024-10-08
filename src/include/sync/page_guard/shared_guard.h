#pragma once

#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/page_guard.h"
#include "sync/page_state.h"

namespace leanstore::sync {

/* Caller of SharedGuard should S-lock the PageState before constructing the Guard */
template <class PageClass>
class SharedGuard : public PageGuard<PageClass> {
 public:
  SharedGuard() : PageGuard<PageClass>::PageGuard() {}

  SharedGuard(buffer::BufferManager *buffer, pageid_t pid);
  SharedGuard(SharedGuard &&other) noexcept;
  explicit SharedGuard(OptimisticGuard<PageClass> &&other) noexcept(false);
  auto operator=(SharedGuard &&other) noexcept(false) -> SharedGuard &;
  ~SharedGuard() noexcept;

  void Unlock();
};

}  // namespace leanstore::sync