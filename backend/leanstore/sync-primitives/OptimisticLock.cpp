#include "OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
void ReadGuard::spin()
{
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   while ((local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT ) { //spin bf_s_lock
      for ( u32 i = mask; i; --i ) {
         _mm_pause();
      }
      mask = mask < max ? mask << 1 : max;
      local_version = version_ptr->load();
   }
}
// -------------------------------------------------------------------------------------
ExclusiveGuard::ExclusiveGuard(ReadGuard &read_lock)
        : ref_guard(read_lock)
{
   assert(ref_guard.version_ptr != nullptr);
   assert((ref_guard.local_version & WRITE_LOCK_BIT) == 0);
   lock_version_t new_version = ref_guard.local_version + WRITE_LOCK_BIT;
   /*
    * A better alternative can be
    * u64 lv = ref_guard.local_version;
    * std::atomic_compare_exchange_strong(ref_guard.version_ptr, &lv, new_version)
    */
   u64 lv = ref_guard.local_version;
   if ( !std::atomic_compare_exchange_strong(ref_guard.version_ptr, &lv, new_version)) {
      throw RestartException();
   }
   ref_guard.local_version = new_version;
   assert((ref_guard.local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT);
}
// -------------------------------------------------------------------------------------
ExclusiveGuard::~ExclusiveGuard()
{

   assert(ref_guard.version_ptr != nullptr);
   assert(ref_guard.local_version == ref_guard.version_ptr->load());
   assert((ref_guard.local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT);
   ref_guard.local_version = WRITE_LOCK_BIT + ref_guard.version_ptr->fetch_add(WRITE_LOCK_BIT);
   assert((ref_guard.local_version & WRITE_LOCK_BIT) == 0);
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// TODO: part
SharedGuard::SharedGuard(ReadGuard &read_guard)
        : ref_guard(read_guard)
{
   lock_version_t new_version = ref_guard.local_version + shared_bit;
}
// -------------------------------------------------------------------------------------
}
}