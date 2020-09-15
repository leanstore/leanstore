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
class OptimisticGuard;
template <typename T>
class SharedGuard;
class ExclusiveGuard;
template <typename T>
class HybridPageGuard;
// -------------------------------------------------------------------------------------
class OptimisticGuard
{
  friend class ExclusiveGuard;
  template <typename T>
  friend class HybridPageGuard;
  template <typename T>
  friend class ExclusivePageGuard;

 public:
  Guard guard;
  // -------------------------------------------------------------------------------------
  OptimisticGuard(HybridLatch& lock, FALLBACK_METHOD if_contended = FALLBACK_METHOD::SPIN) : guard(lock)
  {
    assert(if_contended != FALLBACK_METHOD::EXCLUSIVE && if_contended != FALLBACK_METHOD::SHARED);
    guard.transition(GUARD_STATE::OPTIMISTIC, if_contended);
  }
  // -------------------------------------------------------------------------------------
  OptimisticGuard() = delete;
  OptimisticGuard(OptimisticGuard& other) = delete;  // copy constructor
  // move constructor
  OptimisticGuard(OptimisticGuard&& other) : guard(std::move(other.guard)) {}
  OptimisticGuard& operator=(OptimisticGuard& other) = delete;
  OptimisticGuard& operator=(OptimisticGuard&& other)
  {
    guard = std::move(other.guard);
    // -------------------------------------------------------------------------------------
    return *this;
  }
  // -------------------------------------------------------------------------------------
  inline void recheck() { guard.recheck(); }
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard
{
 private:
  union {
    OptimisticGuard* optimistic_guard;  // our basis
    Guard* guard;                       // our basis
  };
  const bool is_optimistic_guard = true;

 public:
  ExclusiveGuard(OptimisticGuard& o_lock) : optimistic_guard(&o_lock)
  {
    optimistic_guard->guard.transition(GUARD_STATE::EXCLUSIVE);
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  ExclusiveGuard(Guard& guard) : guard(&guard), is_optimistic_guard(false)
  {
    optimistic_guard->guard.transition(GUARD_STATE::EXCLUSIVE);
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusiveGuard)
      // -------------------------------------------------------------------------------------
      ~ExclusiveGuard()
  {
    if (is_optimistic_guard) {
      optimistic_guard->guard.transition(GUARD_STATE::OPTIMISTIC);
    } else {
      guard->transition(GUARD_STATE::OPTIMISTIC);
    }
    jumpmu::clearLastDestructor();
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
