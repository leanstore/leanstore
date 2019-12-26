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
  /*
    OptimisticLatch Design: 8 bits for state, 56 bits for version
    Shared: state = n + 1, where n = threads currently holding the shared latch
    , last one releasing sets the state back to 0
    Exclusively: state = 0, and version+=1
    Optimistic: change nothing
   */
constexpr static u64 LATCH_EXCLUSIVE_BIT = (1 << 8);
constexpr static u64 LATCH_STATE_MASK = ((1 << 8) - 1);  // 0xFF
constexpr static u64 LATCH_VERSION_MASK = ~LATCH_STATE_MASK;
constexpr static u64 LATCH_VERSION_STATE_MASK = ((1 << 9) - 1);
// -------------------------------------------------------------------------------------
class OptimisticGuard;
class SharedGuard;  // TODO
class ExclusiveGuard;
template <typename T>
class OptimisticPageGuard;
// -------------------------------------------------------------------------------------
using OptimisticLatchVersionType = atomic<u64>;
struct OptimisticLatch {
  OptimisticLatchVersionType  version;
  // -------------------------------------------------------------------------------------
  template<typename... Args>
  OptimisticLatch(Args&&... args) : version(std::forward<Args>(args)...) {}
  OptimisticLatchVersionType* operator->() { return &version; }
  // -------------------------------------------------------------------------------------
  OptimisticLatchVersionType* ptr() { return &version; }
  OptimisticLatchVersionType& ref() { return version; }
  // -------------------------------------------------------------------------------------
  void assertExclusivelyLatched() {
    assert((version & LATCH_EXCLUSIVE_BIT ) == LATCH_EXCLUSIVE_BIT);
  }
  void assertNotExclusivelyLatched() {
    assert((version & LATCH_EXCLUSIVE_BIT ) == 0);
  }
  // -------------------------------------------------------------------------------------
  void assertSharedLatched() {
    assert((version & LATCH_STATE_MASK) > 0);
  }
  void assertNotSharedLatched() {
    assert((version & LATCH_STATE_MASK) == 0);
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
  OptimisticGuard(OptimisticLatch* latch_ptr, u64 local_version) : latch_ptr(latch_ptr), local_version(local_version) {}

 public:
  OptimisticLatch* latch_ptr = nullptr;
  u64 local_version;
  // -------------------------------------------------------------------------------------
  OptimisticGuard() = default;
  // -------------------------------------------------------------------------------------
  OptimisticGuard(OptimisticLatch&
                  lock) : latch_ptr(&lock)
  {
    // Ignore the state field
    local_version = latch_ptr->version.load() & LATCH_VERSION_MASK;
    if ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      spin();
    }
    assert((local_version & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
  inline void recheck()
  {
    if (local_version != latch_ptr->version.load()) {
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
  OptimisticLatch* latch_ptr = nullptr;

 public:
  u64 local_version;
  ExclusiveGuardTry(OptimisticLatch& lock) : latch_ptr(&lock)
  {
    local_version = latch_ptr->ref().load();
    if ((local_version & LATCH_VERSION_STATE_MASK) > 0) {
      throw RestartException();
    }
    u64 new_version = local_version + LATCH_EXCLUSIVE_BIT;
    if (!std::atomic_compare_exchange_strong(latch_ptr->ptr(), &local_version, new_version)) {
      throw RestartException();
    }
    local_version = new_version;
    assert((latch_ptr->ref().load() & LATCH_VERSION_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
  }
  void unlock() { latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT); }
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
