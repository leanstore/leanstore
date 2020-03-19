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
void OptimisticGuard::mutex()
{
  // assert(FLAGS_mutex);
  // for (u32 attempt = 0; attempt < 30; attempt++) {
  //   local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
  //   if ((local_version & LATCH_EXCLUSIVE_BIT) == 0) {
  //     return;
  //   }
  // }
  // raise(SIGTRAP);
  // auto mutex = reinterpret_cast<std::mutex*>(latch_ptr->ptr() + 1);
  // mutex->lock();
  // local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
  // jumpmu_registerDestructor();
  // assert(!(local_version & LATCH_EXCLUSIVE_BIT));
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
