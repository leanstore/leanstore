#pragma once
#include "Latch.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class OptimisticGuard;
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
      guard.toOptimisticSpin();
   }
   OptimisticGuard(HybridLatch& lock, bool) : guard(lock)  // TODO: temporary hack
   {
      guard.toOptimisticOrJump();
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
   OptimisticGuard& optimistic_guard;  // our basis

  public:
   ExclusiveGuard(OptimisticGuard& o_lock) : optimistic_guard(o_lock)
   {
      optimistic_guard.guard.toExclusive();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(ExclusiveGuard)
       // -------------------------------------------------------------------------------------
       ~ExclusiveGuard()
   {
      optimistic_guard.guard.unlock();
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
      guard.toExclusive();
      jumpmu_registerDestructor();
   }
   jumpmu_defineCustomDestructor(ExclusiveUpgradeIfNeeded)
       // -------------------------------------------------------------------------------------
       ~ExclusiveUpgradeIfNeeded()
   {
      if (!was_exclusive) {
         guard.unlock();
      }
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
class SharedGuard
{
  private:
   OptimisticGuard& optimistic_guard;  // our basis

  public:
   SharedGuard(OptimisticGuard& o_lock) : optimistic_guard(o_lock)
   {
      optimistic_guard.guard.toShared();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(SharedGuard)
       // -------------------------------------------------------------------------------------
       ~SharedGuard()
   {
      optimistic_guard.guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
