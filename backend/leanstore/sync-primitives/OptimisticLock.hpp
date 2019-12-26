#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <unistd.h>
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
struct RestartException {
 public:
  RestartException() {}
};
// -------------------------------------------------------------------------------------
constexpr static u8 WRITE_LOCK_BIT = 1;
// -------------------------------------------------------------------------------------
class OptimisticGuard;
class ExclusiveGuard;
template <typename T>
class OptimisticPageGuard;
// -------------------------------------------------------------------------------------
using OptimisticLockType = atomic<u64>;
struct OptimisticLock {
  OptimisticLockType  version;
  template<typename... Args>
  OptimisticLock(Args&&... args) : version(std::forward<Args>(args)...) {}
  OptimisticLockType* operator->() { return &version; }
  OptimisticLockType* ptr() { return &version; }
  // -------------------------------------------------------------------------------------
  void assertExclusivelyLocked() {
    assert((version & WRITE_LOCK_BIT ) == WRITE_LOCK_BIT);
  }
  // -------------------------------------------------------------------------------------
  void assertNotExclusivelyLocked() {
    assert((version & WRITE_LOCK_BIT ) == 0);
  }
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
class OptimisticGuard
{
  friend class ExclusiveGuard;
  template <typename T>
  friend class OptimisticPageGuard;
  template <typename T>
  friend class WritePageGuard;

 private:
  OptimisticGuard(atomic<u64>* version_ptr, u64 local_version) : version_ptr(version_ptr), local_version(local_version) {}

 public:
  atomic<u64>* version_ptr = nullptr;
  u64 local_version;
  // -------------------------------------------------------------------------------------
  OptimisticGuard() = default;
  // -------------------------------------------------------------------------------------
  OptimisticGuard(OptimisticLock&
                  lock) : version_ptr(lock.ptr())
  {
    local_version = version_ptr->load();
    if ((local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT) {
      spin();
    }
    assert((local_version & WRITE_LOCK_BIT) != WRITE_LOCK_BIT);
  }
  // -------------------------------------------------------------------------------------
  inline void recheck()
  {
    if (local_version != *version_ptr) {
      throw RestartException();
    }
  }
  // -------------------------------------------------------------------------------------
  void spin();
  // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard
{
 private:
  OptimisticGuard& ref_guard;  // our basis
  bool manually_unlocked = false;

 public:
  // -------------------------------------------------------------------------------------
  ExclusiveGuard(OptimisticGuard& read_lock);
  // -------------------------------------------------------------------------------------
  ~ExclusiveGuard();
};
// -------------------------------------------------------------------------------------
class ExclusiveGuardTry
{
 private:
  atomic<u64>* version_ptr = nullptr;

 public:
  u64 local_version;
  ExclusiveGuardTry(OptimisticLock& lock) : version_ptr(lock.ptr())
  {
    local_version = version_ptr->load();
    if ((local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT) {
      throw RestartException();
    }
    u64 new_version = local_version + WRITE_LOCK_BIT;
    if (!std::atomic_compare_exchange_strong(version_ptr, &local_version, new_version)) {
      throw RestartException();
    }
    local_version = new_version;
    assert((version_ptr->load() & WRITE_LOCK_BIT) == WRITE_LOCK_BIT);
  }
  void unlock() { version_ptr->fetch_add(WRITE_LOCK_BIT); }
  ~ExclusiveGuardTry() {}
};
// -------------------------------------------------------------------------------------
#define spinAsLongAs(expr)               \
  u32 mask = 1;                          \
  u32 const max = 64;                    \
  while (expr) {                         \
    for (u32 i = mask; i; --i) {         \
      _mm_pause();                       \
    }                                    \
    mask = mask < max ? mask << 1 : max; \
  }                                      \
// -------------------------------------------------------------------------------------
// TODO: Shared guard for scans
/*
 * Plan:
 * SharedGuard control the LSB 6-bits
 * Exclusive bit is the LSB 7th bit
 * TODO: rewrite the read and exclusive guards
 */
// The constants
constexpr u64 exclusive_bit = 1 << 7;
constexpr u64 shared_bit = 1 << 0;
class SharedGuard
{
 private:
  OptimisticGuard& ref_guard;

 public:
  SharedGuard(OptimisticGuard& read_guard);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}
