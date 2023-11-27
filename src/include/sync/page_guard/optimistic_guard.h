#pragma once

#include "common/exceptions.h"
#include "sync/page_guard/page_guard.h"
#include "sync/page_state.h"

namespace leanstore::sync {

template <class PageClass>
class OptimisticGuard : public PageGuard<PageClass> {
 public:
  OptimisticGuard() : PageGuard<PageClass>::PageGuard() {}

  OptimisticGuard(pageid_t pid, PageClass *ptr, PageState *page_state,
                  std::function<void(pageid_t)> &&page_loader_func);
  OptimisticGuard(OptimisticGuard &&other) noexcept;
  ~OptimisticGuard() noexcept(false);
  auto operator=(OptimisticGuard &&other) noexcept(false) -> OptimisticGuard &;

  void Init();
  void ValidateOrRestart(bool auto_moved = true);

 protected:
  friend class SharedGuard<PageClass>;
  friend class ExclusiveGuard<PageClass>;

  u64 old_version_{0};                              // Used for OPTIMISTIC check
  std::function<void(pageid_t)> page_loader_func_;  // BufferManager::HandlePageFault
};

}  // namespace leanstore::sync