#pragma once

#include "common/exceptions.h"
#include "sync/page_guard/page_guard.h"
#include "sync/page_state.h"

namespace leanstore::sync {

template <class PageClass>
class OptimisticGuard : public PageGuard<PageClass> {
 public:
  OptimisticGuard() = default;
  OptimisticGuard(buffer::BufferManager *buffer, pageid_t pid);
  OptimisticGuard(OptimisticGuard &&other) noexcept;
  ~OptimisticGuard() noexcept(false);
  auto operator=(OptimisticGuard &&other) noexcept(false) -> OptimisticGuard &;

  /** Lock Coupling facility */
  template <class PageClass2>
  OptimisticGuard(buffer::BufferManager *buffer, pageid_t pid, OptimisticGuard<PageClass2> &parent);

  void Init();
  void ValidateOrRestart(bool auto_moved = true);

 protected:
  friend class SharedGuard<PageClass>;
  friend class ExclusiveGuard<PageClass>;

  u64 old_version_{0};  // Used for OPTIMISTIC check
};

}  // namespace leanstore::sync