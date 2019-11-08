#include "OptimisticLock.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
SharedGuard::SharedGuard(OptimisticVersion &lock)
        : version_ptr(&lock)
{
   local_version = version_ptr->load();
   int mask = 1;
   int const max = 64; //MAX_BACKOFF
   //TODO: move to separate compilation unit
   while ((local_version & 2) == 2 ) { //spin bf_s_lock
      for ( int i = mask; i; --i ) {
         _mm_pause();
      }
      mask = mask < max ? mask << 1 : max;
      local_version = version_ptr->load();
   }
   locked = true;
   assert((local_version & 2) != 2);
}
// -------------------------------------------------------------------------------------
void SharedGuard::recheck()
{
   if ( locked && local_version != *version_ptr ) {
      throw RestartException();
   }
}
// -------------------------------------------------------------------------------------
SharedGuard::SharedGuard(atomic<u64> *version_ptr, u64 local_version, bool locked)
        :
        version_ptr(version_ptr)
        , local_version(local_version)
        , locked(locked) {}
// -------------------------------------------------------------------------------------
ExclusiveGuard::ExclusiveGuard(leanstore::SharedGuard &shared_lock) : ref_lock(shared_lock){
   assert(ref_lock.version_ptr != nullptr);
   assert((ref_lock.local_version & 2 ) == 0);
   lock_version_t new_version = ref_lock.local_version + 2;
   /*
    * A better alternative can be
    * atomic<u64> lv = ref_lock.local_version;
    * std::atomic_compare_exchange_strong(ref_lock.version_ptr, &lv, new_version)
    */
   if ( !std::atomic_compare_exchange_strong(ref_lock.version_ptr, &ref_lock.local_version, new_version)) {
      throw RestartException();
   }
   ref_lock.local_version = new_version;
   assert((ref_lock.local_version & 2 ) == 2);
}
// -------------------------------------------------------------------------------------
ExclusiveGuard::~ExclusiveGuard() {

   assert(ref_lock.version_ptr != nullptr);
   assert(ref_lock.local_version == ref_lock.version_ptr->load());
   assert((ref_lock.local_version & 2 ) == 2);
   ref_lock.local_version = 2 + ref_lock.version_ptr->fetch_add(2);
   assert((ref_lock.local_version & 2 ) == 0);
}
// -------------------------------------------------------------------------------------
}
