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
  OptimisticGuard(HybridLatch& lock) : guard(lock)
  {
    // assert(if_contended != FALLBACK_METHOD::EXCLUSIVE && if_contended != FALLBACK_METHOD::SHARED);
    guard.transition<GUARD_STATE::OPTIMISTIC, FALLBACK_METHOD::SPIN>();
  }
  OptimisticGuard(HybridLatch& lock, bool) : guard(lock)  // TODO: temporary hack
  {
    guard.transition<GUARD_STATE::OPTIMISTIC, FALLBACK_METHOD::JUMP>();
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
  OptimisticGuard* optimistic_guard;  // our basis

 public:
  ExclusiveGuard(OptimisticGuard& o_lock) : optimistic_guard(&o_lock)
  {
    optimistic_guard->guard.transition<GUARD_STATE::EXCLUSIVE>();
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusiveGuard)
      // -------------------------------------------------------------------------------------
      ~ExclusiveGuard()
  {
    optimistic_guard->guard.transition<GUARD_STATE::OPTIMISTIC>();
    jumpmu::clearLastDestructor();
  }
};
// -------------------------------------------------------------------------------------
class ExclusiveUpgradeIfNeeded
{
 private:
  Guard& guard;
  const bool was_exclusive;

 public:
  ExclusiveUpgradeIfNeeded(Guard& guard) : guard(guard), was_exclusive(guard.state == GUARD_STATE::EXCLUSIVE)
  {
    guard.transition<GUARD_STATE::EXCLUSIVE>();
    jumpmu_registerDestructor();
  }
  jumpmu_defineCustomDestructor(ExclusiveUpgradeIfNeeded)
      // -------------------------------------------------------------------------------------
      ~ExclusiveUpgradeIfNeeded()
  {
    if (!was_exclusive) {
      guard.transition<GUARD_STATE::OPTIMISTIC>();
    }
    jumpmu::clearLastDestructor();
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
