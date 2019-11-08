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
   SharedGuard(atomic<u64> *version_ptr, u64 local_version, bool locked);
public:
   atomic<u64> *version_ptr = nullptr;
   u64 local_version;
   bool locked = false;
   // -------------------------------------------------------------------------------------
   SharedGuard() = default;
   // -------------------------------------------------------------------------------------
   SharedGuard(OptimisticVersion &lock);
   // -------------------------------------------------------------------------------------
   void recheck();
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard {
private:
   SharedGuard &ref_lock; // our basis
public:
   // -------------------------------------------------------------------------------------
   ExclusiveGuard(SharedGuard &shared_lock);
   // -------------------------------------------------------------------------------------
   ~ExclusiveGuard();
};
// -------------------------------------------------------------------------------------
}