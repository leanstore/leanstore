#pragma once
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// The following guards are primarily designed for buffer management use cases
// This implies that the guards never block (sleep), they immediately jump instead.
class BMOptimisticGuard;
class BMExclusiveGuard;
template <typename T>
class HybridPageGuard;
// -------------------------------------------------------------------------------------
class BMOptimisticGuard
{
   friend class BMExclusiveGuard;
   template <typename T>
   friend class HybridPageGuard;
   template <typename T>
   friend class ExclusivePageGuard;

  public:
   Guard guard;
   // -------------------------------------------------------------------------------------
   BMOptimisticGuard(HybridLatch& lock) : guard(lock) { guard.toOptimisticOrJump(); }
   // -------------------------------------------------------------------------------------
   BMOptimisticGuard() = delete;
   BMOptimisticGuard(BMOptimisticGuard& other) = delete;  // copy constructor
   // move constructor
   BMOptimisticGuard(BMOptimisticGuard&& other) : guard(std::move(other.guard)) {}
   BMOptimisticGuard& operator=(BMOptimisticGuard& other) = delete;
   BMOptimisticGuard& operator=(BMOptimisticGuard&& other)
   {
      guard = std::move(other.guard);
      // -------------------------------------------------------------------------------------
      return *this;
   }
   // -------------------------------------------------------------------------------------
   inline void recheck() { guard.recheck(); }
};
// -------------------------------------------------------------------------------------
class BMExclusiveGuard
{
  private:
   BMOptimisticGuard& optimistic_guard;  // our basis

  public:
   BMExclusiveGuard(BMOptimisticGuard& o_lock) : optimistic_guard(o_lock)
   {
      // optimistic_guard.guard.toExclusive();
      optimistic_guard.guard.tryToExclusive();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(BMExclusiveGuard)
       // -------------------------------------------------------------------------------------
       ~BMExclusiveGuard()
   {
      optimistic_guard.guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
class BMExclusiveUpgradeIfNeeded
{
  private:
   Guard& guard;
   const bool was_exclusive;

  public:
   BMExclusiveUpgradeIfNeeded(Guard& guard) : guard(guard), was_exclusive(guard.state == GUARD_STATE::EXCLUSIVE)
   {
      guard.tryToExclusive();
      jumpmu_registerDestructor();
   }
   jumpmu_defineCustomDestructor(BMExclusiveUpgradeIfNeeded)
       // -------------------------------------------------------------------------------------
       ~BMExclusiveUpgradeIfNeeded()
   {
      if (!was_exclusive) {
         guard.unlock();
      }
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
class BMSharedGuard
{
  private:
   BMOptimisticGuard& optimistic_guard;  // our basis

  public:
   BMSharedGuard(BMOptimisticGuard& o_lock) : optimistic_guard(o_lock)
   {
      // optimistic_guard.guard.toShared();
      optimistic_guard.guard.tryToShared();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(BMSharedGuard)
       // -------------------------------------------------------------------------------------
       ~BMSharedGuard()
   {
      optimistic_guard.guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
