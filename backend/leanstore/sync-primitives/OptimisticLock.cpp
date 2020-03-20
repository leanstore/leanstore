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
  volatile u32 mask = 1;
  while ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {  // spin
                                                                          // bf_s_lock
    BACKOFF_STRATEGIES()
    local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
  }
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
