#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unistd.h>
#include <emmintrin.h>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
struct RestartException {
public:
   RestartException() {}
   RestartException(int code)
   {
      cout << code << endl;
   }
};
// -------------------------------------------------------------------------------------
class SharedLock;
class ExclusiveLock;
template<typename T>
class PageGuard;
using lock_version_t = u64;
using OptimisticVersion = atomic<lock_version_t>;
// -------------------------------------------------------------------------------------
class SharedLock {
   friend class ExclusiveLock;
   template<typename T>
   friend class PageGuard;
private:
   atomic<u64> *version_ptr = nullptr;
   u64 local_version;
   bool locked = false;
public:
   // -------------------------------------------------------------------------------------
   SharedLock() = default;
   // -------------------------------------------------------------------------------------
   SharedLock(OptimisticVersion &lock)
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
   }
   // -------------------------------------------------------------------------------------
   void recheck()
   {
      if ( locked && local_version != *version_ptr ) {
         throw RestartException();
      }
   }
   // -------------------------------------------------------------------------------------
   SharedLock &operator=(const SharedLock &other) = default;
   operator bool()
   const
   {
      return locked;
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveLock {
private:
   SharedLock &ref_lock; // our basis
public:
   // -------------------------------------------------------------------------------------
   ExclusiveLock(SharedLock &shared_lock)
           : ref_lock(shared_lock)
   {
      assert(ref_lock.version_ptr != nullptr);
      lock_version_t new_version = ref_lock.local_version + 2;
      if ( !std::atomic_compare_exchange_strong(ref_lock.version_ptr, &ref_lock.local_version, new_version)) {
         throw RestartException();
      }
      ref_lock.local_version = new_version;
   }
   // -------------------------------------------------------------------------------------
   ~ExclusiveLock()
   {
      assert(ref_lock.version_ptr != nullptr);
      if ( ref_lock.version_ptr != nullptr ) {
         ref_lock.local_version = 2 + ref_lock.version_ptr->fetch_add(2);
      }
   }
};
// -------------------------------------------------------------------------------------
}