#include "OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
void OptimisticGuard::slowPath()
{
  if (FLAGS_mutex) {
    for (u32 attempt = 0; attempt < 30; attempt++) {
      local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
      if ((local_version & LATCH_EXCLUSIVE_BIT) == 0) {
        return;
      }
    }
    auto mutex = reinterpret_cast<std::mutex*>(latch_ptr->ptr() + 1);
    mutex->lock();
    mutex_locked = true;
    local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
    assert(!(local_version & LATCH_EXCLUSIVE_BIT));
  } else {
    volatile u32 mask = 1;
    while ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {  // spin
                                                                            // bf_s_lock
      BACKOFF_STRATEGIES()
      local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
    }
  }
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
