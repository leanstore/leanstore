#pragma once
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#ifdef __x86_64__
#include <emmintrin.h>
#define MYPAUSE()  _mm_pause()
#endif
#ifdef __aarch64__
#include <arm_acle.h>
#define MYPAUSE() asm("YIELD");
#endif
#include <unistd.h>

#include <atomic>
#include <shared_mutex>
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
      MYPAUSE();                                                      \
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
struct alignas(64) OptimisticLatch {
  OptimisticLatchVersionType version;
  std::shared_mutex mutex;
  // -------------------------------------------------------------------------------------
  template <typename... Args>
  OptimisticLatch(Args&&... args) : version(std::forward<Args>(args)...)
  {
  }
  OptimisticLatchVersionType* operator->() { return &version; }
  // -------------------------------------------------------------------------------------
  OptimisticLatchVersionType* ptr() { return &version; }
  OptimisticLatchVersionType& ref() { return version; }
  // -------------------------------------------------------------------------------------
  void assertExclusivelyLatched() { assert(isExclusivelyLatched()); }
  void assertNotExclusivelyLatched() { assert(!isExclusivelyLatched()); }
  // -------------------------------------------------------------------------------------
  void assertSharedLatched() { assert(isSharedLatched()); }
  void assertNotSharedLatched() { assert(!isSharedLatched()); }
  // -------------------------------------------------------------------------------------
  bool isExclusivelyLatched() { return (version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT; }
  bool isSharedLatched() { return (version & LATCH_STATE_MASK) > 0; }
  bool isAnyLatched() { return (version & LATCH_EXCLUSIVE_STATE_MASK) > 0; }
};
static_assert(sizeof(OptimisticLatch) == 64, "");
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
    // -------------------------------------------------------------------------------------
    other.latch_ptr = reinterpret_cast<OptimisticLatch*>(0x99);
    other.local_version = 0;
    other.mutex_locked_upfront = false;
    // -------------------------------------------------------------------------------------
    return *this;
  }
  // -------------------------------------------------------------------------------------
  // Keep spinning constructor
  OptimisticGuard(OptimisticLatch& lock) : latch_ptr(&lock)
  {
    // Ignore the state field
    local_version = latch_ptr->version.load() & LATCH_VERSION_MASK;
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
      local_version = latch_ptr->version.load() & LATCH_VERSION_MASK;
      if (((local_version & LATCH_EXCLUSIVE_BIT) == 0)) {
        return;
      }
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
    {
      const u64 new_version = (ref_guard.local_version + LATCH_EXCLUSIVE_BIT);
      u64 expected = ref_guard.local_version;  // assuming state == 0
      if (ref_guard.mutex_locked_upfront) {
        if (!ref_guard.latch_ptr->ref().compare_exchange_strong(expected, new_version)) {
          ensure(false);
        }
      } else {
        if (!ref_guard.latch_ptr->mutex.try_lock()) {
          jumpmu::jump();
        }
        // ref_guard.latch_ptr->mutex.lock(); can be helpful for debugging
        if (!ref_guard.latch_ptr->ref().compare_exchange_strong(expected, new_version)) {
          ref_guard.latch_ptr->mutex.unlock();
          jumpmu::jump();
        }
      }
      ref_guard.local_version = new_version;
    }
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
  }
  static inline void unlatch(OptimisticGuard& ref_guard)
  {
    assert(ref_guard.latch_ptr != nullptr);
    assert(ref_guard.local_version == ref_guard.latch_ptr->ref().load());
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_STATE_MASK) == LATCH_EXCLUSIVE_BIT);
    {
      ref_guard.local_version = LATCH_EXCLUSIVE_BIT + ref_guard.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      if (!ref_guard.mutex_locked_upfront) {
        ref_guard.latch_ptr->assertNotExclusivelyLatched();
        ref_guard.latch_ptr->mutex.unlock();
      }
    }
    assert((ref_guard.local_version & LATCH_STATE_MASK) == 0);
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == 0);
  }
  // -------------------------------------------------------------------------------------
  ExclusiveGuard(OptimisticGuard& o_lock) : ref_guard(o_lock)
  {
    ExclusiveGuard::latch(ref_guard);
    assert(jumpmu::de_stack_counter < 10);
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
// Plan: lock the mutex in shared mode, then try to release the shit
class SharedGuard
{
 private:
  OptimisticGuard& ref_guard;

 public:
  // -------------------------------------------------------------------------------------
  static inline void latch(OptimisticGuard& ref_guard)
  {
    /*
      it is fine if the state changed in-between therefore we have to keep trying as long
      as the version stayed the same
     */
    ensure(false);
    ref_guard.latch_ptr->mutex.lock_shared();
  try_accquire_shared_guard : {
    u64 current_state = ref_guard.latch_ptr->ref().load() & LATCH_STATE_MASK;
    u64 expected_old_compound = ref_guard.local_version | current_state;
    u64 new_state = current_state + ((current_state == 0) ? 2 : 1);
    u64 new_compound = ref_guard.local_version | new_state;
    if (!ref_guard.latch_ptr->ref().compare_exchange_strong(expected_old_compound, new_compound)) {
      if ((expected_old_compound & LATCH_VERSION_MASK) != ref_guard.local_version) {
        jumpmu::jump();
      } else {
        goto try_accquire_shared_guard;
      }
    }
  }
  }
  static inline void unlatch(OptimisticGuard& ref_guard)
  {
    ensure(false);
  try_release_shared_guard : {
    u64 current_compound = ref_guard.latch_ptr->ref().load();
    u64 new_compound = current_compound;
    if ((current_compound & LATCH_STATE_MASK) == 2) {
      new_compound -= 2;
    } else {
      new_compound -= 1;
    }
    if (!ref_guard.latch_ptr->ref().compare_exchange_strong(current_compound, new_compound)) {
      goto try_release_shared_guard;
    }
    ref_guard.latch_ptr->mutex.unlock_shared();
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
      MYPAUSE();                       \
    }                                    \
    mask = mask < max ? mask << 1 : max; \
  }                                      \
  // -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
