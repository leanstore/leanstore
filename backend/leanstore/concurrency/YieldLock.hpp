#pragma once
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include "leanstore/sync-primitives/JumpMU.hpp"
#include <emmintrin.h>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
class SpinLock
{
   std::atomic_flag _lock = ATOMIC_FLAG_INIT;
  public:
   bool try_lock()
   {
      return !_lock.test_and_set(std::memory_order_acquire); 
   }
   void lock()
   {
      while (!try_lock()) { 
         _mm_pause();
      }
   }
   void unlock()
   {
      _lock.clear(std::memory_order_release);
   }
};
class YieldLock
{
   std::atomic_flag _lock = ATOMIC_FLAG_INIT;
   std::atomic<int> _waiting{0};
   std::atomic<int> _owner{0};
  public:
   // -------------------------------------------------------------------------------------
   bool try_lock();
   void lock();
   void unlock();
};
class SharedYieldLock
{
   static constexpr int EXCLUSIVE_BIT = 1 << 31;
   std::atomic<int> _lock{0};
  public:
   bool try_lock() {
      int expected = 0;
      int desired = EXCLUSIVE_BIT;
      return _lock.compare_exchange_strong(expected, desired, std::memory_order_acquire);
   }
   void lock() {
      // TODO do something more intelligen, maybe?
      int spin = 0;
      while (!try_lock()) {
         spin++;
         if (spin > 40) {
            jumpmu::jump(leanstore::UserJumpReason::Lock);
         }
         _mm_pause();
         _mm_pause();
         _mm_pause();
      }
   }
   void unlock() {
      assert(_lock == EXCLUSIVE_BIT);
      _lock.store(0, std::memory_order_release);
   }
   bool try_lock_shared() {
      int expected = 0;
      while (!_lock.compare_exchange_strong(expected, expected+1, std::memory_order_acquire)) {
         if (expected == EXCLUSIVE_BIT) return false; // if lock is excl lock, fail
      }
      return true;
   }
   void lock_shared() {
      // TODO do something more intelligen, maybe?
      while (!try_lock_shared()) { 
         _mm_pause();
         _mm_pause();
         _mm_pause();
      }
   }
   void unlock_shared() {
      assert(_lock != EXCLUSIVE_BIT);
      _lock.fetch_add(-1, std::memory_order_release);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
