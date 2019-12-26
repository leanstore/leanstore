#include "OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
void OptimisticGuard::spin()
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
ExclusiveGuard::ExclusiveGuard(OptimisticGuard& read_lock) : ref_guard(read_lock)
{
  assert(ref_guard.latch_ptr != nullptr);
  assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == 0);
  u64 new_version = ref_guard.local_version + LATCH_EXCLUSIVE_BIT;
  u64 lv = ref_guard.local_version;
  if (!std::atomic_compare_exchange_strong(ref_guard.latch_ptr->ptr(), &lv, new_version)) {
    throw RestartException();
  }
  ref_guard.local_version = new_version;
  assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
}
// -------------------------------------------------------------------------------------
ExclusiveGuard::~ExclusiveGuard()
{
    assert(ref_guard.latch_ptr != nullptr);
    assert(ref_guard.local_version == ref_guard.latch_ptr->ref().load());
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
    ref_guard.local_version = LATCH_EXCLUSIVE_BIT + ref_guard.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT);
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == 0);
}
// -------------------------------------------------------------------------------------
// TODO: part
SharedGuard::SharedGuard(OptimisticGuard& read_guard) : ref_guard(read_guard)
{

}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}
