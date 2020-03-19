#pragma once
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
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
constexpr static u64 LATCH_EXCLUSIVE_STATE_MASK = ((1 << 9) - 1);
// -------------------------------------------------------------------------------------
#define MAX_BACKOFF FLAGS_backoff  // FLAGS_x
#define BACKOFF_STRATEGIES()                                            \
  if (FLAGS_backoff) {                                                  \
    for (u64 i = utils::RandomGenerator::getRandU64(0, mask); i; --i) { \
      _mm_pause();                                                      \
    }                                                                   \
    mask = mask < MAX_BACKOFF ? mask << 1 : MAX_BACKOFF;                \
  }
// -------------------------------------------------------------------------------------
class OptimisticGuard;
class SharedGuard;
class ExclusiveGuard;
template <typename T>
class OptimisticPageGuard;
// -------------------------------------------------------------------------------------
using OptimisticLatchVersionType = atomic<u64>;
struct OptimisticLatch {
  OptimisticLatchVersionType raw;
  // -------------------------------------------------------------------------------------
  template <typename... Args>
  OptimisticLatch(Args&&... args) : raw(std::forward<Args>(args)...)
  {
  }
  OptimisticLatchVersionType* operator->() { return &raw; }
  // -------------------------------------------------------------------------------------
  OptimisticLatchVersionType* ptr() { return &raw; }
  OptimisticLatchVersionType& ref() { return raw; }
  // -------------------------------------------------------------------------------------
  void assertExclusivelyLatched() { assert(isExclusivelyLatched()); }
  void assertNotExclusivelyLatched() { assert(!isExclusivelyLatched()); }
  // -------------------------------------------------------------------------------------
  void assertSharedLatched() { assert(isSharedLatched()); }
  void assertNotSharedLatched() { assert(!isSharedLatched()); }
  // -------------------------------------------------------------------------------------
  bool isExclusivelyLatched() { return (raw & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT; }
  bool isSharedLatched() { return (raw & LATCH_STATE_MASK) > 0; }
  bool isAnyLatched() { return (raw & LATCH_EXCLUSIVE_STATE_MASK) > 0; }
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
class OptimisticGuard
{
  friend class ExclusiveGuard;
  friend class SharedGuard;
  template <typename T>
  friend class OptimisticPageGuard;
  template <typename T>
  friend class ExclusivePageGuard;

 private:
  OptimisticGuard(OptimisticLatch* latch_ptr, u64 local_version) : latch_ptr(latch_ptr), local_version(local_version) {}

 public:
  enum class IF_LOCKED { JUMP, SET_NULL, CAN_NOT_BE };
  // -------------------------------------------------------------------------------------
  OptimisticLatch* latch_ptr = nullptr;
  u64 local_version;                  // without the state
  bool mutex_locked_upfront = false;  // set to true only when OptimisticPageGuard has acquired the mutex
  // -------------------------------------------------------------------------------------
  OptimisticGuard() = delete;
  // copy constructor
  OptimisticGuard(OptimisticGuard& other)
      : latch_ptr(other.latch_ptr), local_version(other.local_version), mutex_locked_upfront(other.mutex_locked_upfront)
  {
  }
  // move constructor
  OptimisticGuard(OptimisticGuard&& other)
      : latch_ptr(other.latch_ptr), local_version(other.local_version), mutex_locked_upfront(other.mutex_locked_upfront)
  {
    other.latch_ptr = reinterpret_cast<OptimisticLatch*>(0x99);
    other.local_version = 0;
    other.mutex_locked_upfront = false;
  }
  OptimisticGuard& operator=(OptimisticGuard& other) = delete;
  OptimisticGuard& operator=(OptimisticGuard&& other)
  {
    latch_ptr = other.latch_ptr;
    local_version = other.local_version;
    mutex_locked_upfront = other.mutex_locked_upfront;
    return *this;
  }
  // -------------------------------------------------------------------------------------
  // Keep spinning constructor
  OptimisticGuard(OptimisticLatch& lock) : latch_ptr(&lock)
  {
    // Ignore the state field
    local_version = latch_ptr->raw.load() & LATCH_VERSION_MASK;
    if ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      slowPath();
    }
    assert((local_version & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
  OptimisticGuard(OptimisticLatch& lock, IF_LOCKED option) : latch_ptr(&lock)
  {
    // Ignore the state field
    for (u32 attempt = 0; attempt < 40; attempt++) {
      local_version = latch_ptr->raw.load() & LATCH_VERSION_MASK;
      if (((local_version & LATCH_EXCLUSIVE_BIT) == 0))
        return;
    }
    if ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {
      if (option == IF_LOCKED::JUMP) {
        jumpmu::jump();
      } else if (option == IF_LOCKED::SET_NULL) {
        local_version = 0;
        latch_ptr = nullptr;
      } else if (option == IF_LOCKED::CAN_NOT_BE) {
        ensure(false);
      }
    }
    assert((local_version & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
  inline void recheck()
  {
    if (local_version != (latch_ptr->ref().load() & LATCH_VERSION_MASK)) {
      assert(!mutex_locked_upfront);
      jumpmu::jump();
    }
  }
  // -------------------------------------------------------------------------------------
  void slowPath();
  void mutex();
  // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard
{
 private:
  OptimisticGuard& ref_guard;  // our basis
 public:
  static inline void latch(OptimisticGuard& ref_guard)
  {
    assert(ref_guard.latch_ptr != nullptr);
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_STATE_MASK) == 0);
    if (FLAGS_mutex) {
      if (ref_guard.mutex_locked_upfront) {
        assert(ref_guard.local_version == ref_guard.latch_ptr->ref().load());
        ref_guard.local_version = (LATCH_EXCLUSIVE_BIT + ref_guard.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT));
      } else {
        auto mutex = reinterpret_cast<std::mutex*>(ref_guard.latch_ptr->ptr() + 1);
        if (!mutex->try_lock()) {
          jumpmu::jump();
        }
        // mutex->lock();
        const u64 new_version = (ref_guard.local_version + LATCH_EXCLUSIVE_BIT);
        u64 expected = ref_guard.local_version;  // assuming state == 0
        if (!ref_guard.latch_ptr->ref().compare_exchange_strong(expected, new_version)) {
          mutex->unlock();
          jumpmu::jump();
        }
        ref_guard.local_version = new_version;
        assert((ref_guard.local_version & LATCH_EXCLUSIVE_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
      }
    } else {
      const u64 new_version = ref_guard.local_version + LATCH_EXCLUSIVE_BIT;
      u64 new_compound = new_version;
      u64 lv = ref_guard.local_version;  // assuming state == 0
      if (!ref_guard.latch_ptr->ref().compare_exchange_strong(lv, new_compound)) {
        // we restart when another thread has shared latched
        jumpmu::jump();
      }
      ref_guard.local_version = new_version;
    }
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
  }
  static inline void unlatch(OptimisticGuard& ref_guard)
  {
    assert(ref_guard.latch_ptr != nullptr);
    assert(ref_guard.local_version == ref_guard.latch_ptr->ref().load());
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
    {
      ref_guard.local_version = LATCH_EXCLUSIVE_BIT + ref_guard.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT);
      if (FLAGS_mutex && !ref_guard.mutex_locked_upfront) {
        auto mutex = reinterpret_cast<std::mutex*>(ref_guard.latch_ptr->ptr() + 1);
        mutex->unlock();
      }
    }
    assert((ref_guard.local_version & LATCH_STATE_MASK) == 0);
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == 0);
  }
  // -------------------------------------------------------------------------------------
  ExclusiveGuard(OptimisticGuard& o_lock) : ref_guard(o_lock)
  {
    ExclusiveGuard::latch(ref_guard);
    assert(jumpmu::de_stack_counter < 5);
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusiveGuard)
      // -------------------------------------------------------------------------------------
      ~ExclusiveGuard()
  {
    ExclusiveGuard::unlatch(ref_guard);
    jumpmu::clearLastDestructor();
  }
};
// -------------------------------------------------------------------------------------
class ExclusiveGuardTry
{
 private:
  OptimisticLatch* latch_ptr = nullptr;

 public:
  ExclusiveGuardTry(OptimisticLatch& lock) : latch_ptr(&lock)
  {
    u64 current_compound = latch_ptr->ref().load();
    if ((current_compound & LATCH_EXCLUSIVE_STATE_MASK) > 0) {
      jumpmu::jump();
    }
    const u64 new_version = current_compound + LATCH_EXCLUSIVE_BIT;
    u64 new_compound = new_version;
    if (!latch_ptr->ref().compare_exchange_strong(current_compound, new_compound)) {
      jumpmu::jump();
    }
    assert((latch_ptr->ref().load() & LATCH_EXCLUSIVE_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
  }
  void unlock() { latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT); }
};
// -------------------------------------------------------------------------------------
class SharedGuard
{
 private:
  OptimisticGuard& ref_guard;

 public:
  // -------------------------------------------------------------------------------------
  static inline void latch(OptimisticGuard& basis_guard)
  {
    /*
      it is fine if the state changed in-between therefore we have to keep trying as long
      as the version stayed the same
     */
  try_accquire_shared_guard:
    u64 current_state = basis_guard.latch_ptr->ref().load() & LATCH_STATE_MASK;
    u64 expected_old_compound = basis_guard.local_version | current_state;
    u64 new_state = current_state + ((current_state == 0) ? 2 : 1);
    u64 new_compound = basis_guard.local_version | new_state;
    if (!basis_guard.latch_ptr->ref().compare_exchange_strong(expected_old_compound, new_compound)) {
      if ((expected_old_compound & LATCH_VERSION_MASK) != basis_guard.local_version) {
        jumpmu::jump();
      } else {
        goto try_accquire_shared_guard;
      }
    }
  }
  static inline void unlatch(OptimisticGuard& basis_guard)
  {
  try_release_shared_guard:
    u64 current_compound = basis_guard.latch_ptr->ref().load();
    u64 new_compound = current_compound;
    if ((current_compound & LATCH_STATE_MASK) == 2) {
      new_compound -= 2;
    } else {
      new_compound -= 1;
    }
    if (!basis_guard.latch_ptr->ref().compare_exchange_strong(current_compound, new_compound)) {
      goto try_release_shared_guard;
    }
  }
  // -------------------------------------------------------------------------------------
  SharedGuard(OptimisticGuard& basis_guard) : ref_guard(basis_guard) { SharedGuard::latch(ref_guard); }
  ~SharedGuard() { SharedGuard::unlatch(ref_guard); }
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
}  // namespace buffermanager
}  // namespace leanstore
