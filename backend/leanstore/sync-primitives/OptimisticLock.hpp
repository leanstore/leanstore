#pragma once
#include "Latch.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
#define MAX_BACKOFF FLAGS_backoff  // FLAGS_x
#define BACKOFF_STRATEGIES()                                            \
  if (FLAGS_backoff) {                                                  \
    for (u64 i = utils::RandomGenerator::getRandU64(0, mask); i; --i) { \
      MYPAUSE();                                                        \
    }                                                                   \
    mask = mask < MAX_BACKOFF ? mask << 1 : MAX_BACKOFF;                \
  }
// -------------------------------------------------------------------------------------
class OptimisticGuard;
template <typename T>
class SharedGuard;
class ExclusiveGuard;
template <typename T>
class HybridPageGuard;
// -------------------------------------------------------------------------------------
void latch(HybridLatch& latch, GUARD_STATE& state, u64& dest_version, FALLBACK_METHOD& if_contended)
{
  switch (if_contended) {
    case FALLBACK_METHOD::SPIN: {
      break;
    }
    default:
      break;
  }
}
class OptimisticGuard
{
  friend class ExclusiveGuard;
  template <typename T>
  friend class HybridPageGuard;
  template <typename T>
  friend class ExclusivePageGuard;

 private:
  OptimisticGuard(HybridLatch* latch_ptr, u64 local_version) : latch_ptr(latch_ptr), local_version(local_version) {}

 public:
  HybridLatch* latch_ptr = nullptr;
  u64 local_version;
  bool mutex_locked_upfront = false;  // set to true only when OptimisticPageGuard has acquired the mutex
  // -------------------------------------------------------------------------------------
  OptimisticGuard(HybridLatch& lock, FALLBACK_METHOD option = FALLBACK_METHOD::SPIN) : latch_ptr(&lock)
  {
    for (u32 attempt = 0; attempt < 40; attempt++) {
      local_version = latch_ptr->version.load() & LATCH_VERSION_MASK;
      if (((local_version & LATCH_EXCLUSIVE_BIT) == 0)) {
        return;
      }
    }
    switch (option) {
      case FALLBACK_METHOD::SPIN: {
        volatile u32 mask = 1;
        while ((local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT) {  // spin
                                                                                // bf_s_lock
          BACKOFF_STRATEGIES()
          local_version = latch_ptr->ref().load() & LATCH_VERSION_MASK;
        }
      } break;
      case FALLBACK_METHOD::JUMP: {
        jumpmu::jump();
      }
      default:
        ensure(false);
    }
    assert((local_version & LATCH_EXCLUSIVE_BIT) != LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
  OptimisticGuard() = delete;
  OptimisticGuard(OptimisticGuard& other) = delete;  // copy constructor
  // move constructor
  OptimisticGuard(OptimisticGuard&& other)
      : latch_ptr(other.latch_ptr), local_version(other.local_version), mutex_locked_upfront(other.mutex_locked_upfront)
  {
    other.latch_ptr = reinterpret_cast<HybridLatch*>(0x99);
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
    other.latch_ptr = reinterpret_cast<HybridLatch*>(0x99);
    other.local_version = 0;
    other.mutex_locked_upfront = false;
    // -------------------------------------------------------------------------------------
    return *this;
  }
  // -------------------------------------------------------------------------------------
  inline void recheck()
  {
    if (local_version != (latch_ptr->ref().load() & LATCH_VERSION_MASK)) {
      assert(!mutex_locked_upfront);
      jumpmu::jump();
    }
  }
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
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == 0);
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
    assert((ref_guard.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
  }
  static inline void unlatch(OptimisticGuard& ref_guard)
  {
    assert(ref_guard.latch_ptr != nullptr);
    assert(ref_guard.local_version == ref_guard.latch_ptr->ref().load());
    {
      ref_guard.local_version = LATCH_EXCLUSIVE_BIT + ref_guard.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      if (!ref_guard.mutex_locked_upfront) {
        ref_guard.latch_ptr->assertNotExclusivelyLatched();
        ref_guard.latch_ptr->mutex.unlock();
      }
    }
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
}  // namespace buffermanager
}  // namespace leanstore
