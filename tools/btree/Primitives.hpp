#pragma once
#include <atomic>
#include "Units.hpp"
using namespace std;
// -------------------------------------------------------------------------------------
struct OptimisticLockException {};
// -------------------------------------------------------------------------------------
class SharedLock;
class ExclusiveLock;
// -------------------------------------------------------------------------------------
class SharedLock {
private:
   atomic<u64> *lock = nullptr;
   u64 local_version;
   bool locked = true;
public:
   SharedLock() {
      locked = false;
   }
   SharedLock(atomic<u64> &lock)
           : lock(&lock)
   {
      local_version = lock;
      while ((local_version & 2) == 2) { //spin lock
         usleep(5);
         local_version = lock.load();
      }
   }
   // -------------------------------------------------------------------------------------
   ~SharedLock()
   {
      if ( locked && local_version != *lock ) {
         throw OptimisticLockException();
      }
   }
   // -------------------------------------------------------------------------------------
   void unlock() {
      if ( locked && local_version != *lock ) {
         throw OptimisticLockException();
      }
      locked = false;
   }
   // -------------------------------------------------------------------------------------
   void verify() {
      if ( locked && local_version != *lock ) {
         throw OptimisticLockException();
      }
   }
   // -------------------------------------------------------------------------------------
   SharedLock &operator=(const SharedLock &other) {
      if ( locked && local_version != *lock ) {
         throw OptimisticLockException();
      }
      lock = other.lock;
      local_version = *lock;
   }
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveLock {
private:
   atomic<u64> *lock;
   bool locked = true;
public:
   ExclusiveLock() {
      locked = false;
   }
   // -------------------------------------------------------------------------------------
   ExclusiveLock(u64 version, atomic<u64> &lock) : lock(&lock) {
      if (!lock.compare_exchange_strong(version, version +2)) {
         throw OptimisticLockException();
      }
   }
   // -------------------------------------------------------------------------------------
   void unlock() {
      if(locked) {
         lock->fetch_add(2);
      }
   }
   // -------------------------------------------------------------------------------------
   ~ExclusiveLock() {
      unlock();
   }
};
// -------------------------------------------------------------------------------------