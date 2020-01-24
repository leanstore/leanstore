#include "OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
void OptimisticGuard::spinTillLatching()
{
  u32 mask = 1;
  u32 const max = 64;                                           // MAX_BACKOFF
  while ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {  // spin
                                                                // bf_s_lock
    for (u32 i = mask; i; --i) {
      _mm_pause();
    }
    mask = mask < max ? mask << 1 : max;
    local_version = latch_ptr->ref().load();
  }
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}
