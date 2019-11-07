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
class SharedGuard;
class ExclusiveGuard;
template<typename T>
class ReadPageGuard;
using lock_version_t = u64;
using OptimisticVersion = atomic<lock_version_t>;
// -------------------------------------------------------------------------------------
class SharedGuard {
   friend class ExclusiveGuard;
   template<typename T>
   friend
   class ReadPageGuard;
   template<typename T>
   friend
   class WritePageGuard;
private:
   SharedGuard(atomic<u64> *version_ptr, u64 local_version, bool locked)
           : version_ptr(version_ptr)
             , local_version(local_version)
             , locked(locked) {}
public:
   atomic<u64> *version_ptr = nullptr;
   u64 local_version;
   bool locked = false;
   // -------------------------------------------------------------------------------------
   SharedGuard() = default;
   // -------------------------------------------------------------------------------------
   SharedGuard(OptimisticVersion &lock)
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
      assert((local_version & 2)!= 2);
   }
   // -------------------------------------------------------------------------------------
   void recheck()
   {
      if ( locked && local_version != *version_ptr ) {
         throw RestartException();
      }
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard {
private:
   SharedGuard &ref_lock; // our basis
public:
   // -------------------------------------------------------------------------------------
   ExclusiveGuard(SharedGuard &shared_lock)
           : ref_lock(shared_lock)
   {
      assert(ref_lock.version_ptr != nullptr);
      assert((ref_lock.local_version & 2 ) == 0);
      lock_version_t new_version = ref_lock.local_version + 2;
      if ( !std::atomic_compare_exchange_strong(ref_lock.version_ptr, &ref_lock.local_version, new_version)) {
         throw RestartException();
      }
      ref_lock.local_version = new_version;
      assert((ref_lock.local_version & 2 ) == 2);
   }
   // -------------------------------------------------------------------------------------
   ~ExclusiveGuard()
   {
      assert(ref_lock.version_ptr != nullptr);
      assert(ref_lock.local_version == ref_lock.version_ptr->load());
      assert((ref_lock.local_version & 2 ) == 2);
      ref_lock.local_version = 2 + ref_lock.version_ptr->fetch_add(2);
      assert((ref_lock.local_version & 2 ) == 0);
   }
};
// -------------------------------------------------------------------------------------
}