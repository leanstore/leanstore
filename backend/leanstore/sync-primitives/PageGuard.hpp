#pragma once
#include "Exceptions.hpp"
#include "Latch.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
// OptimisticPageGuard can hold the mutex. There are 3 locations where it can release it:
// 1- Destructor if not moved
// 2- Assign operator
// 3- kill()
template <typename T>
class ExclusivePageGuard;
template <typename T>
class SharedPageGuard;
template <typename T>
class HybridPageGuard
{
  public:
   BufferFrame* bf = nullptr;
   Guard guard;
   bool manually_checked = false;
   bool keep_alive = true;
   // -------------------------------------------------------------------------------------
   // Constructors
   HybridPageGuard() : bf(nullptr), guard(nullptr) { jumpmu_registerDestructor(); }  // use with caution
   HybridPageGuard(Guard& guard, BufferFrame* bf) : bf(bf), guard(std::move(guard)) { jumpmu_registerDestructor(); }
   // -------------------------------------------------------------------------------------
   HybridPageGuard(HybridPageGuard& other) = delete;   // Copy constructor
   HybridPageGuard(HybridPageGuard&& other) = delete;  // Move constructor
   // -------------------------------------------------------------------------------------
   // I: Allocate a new page
   HybridPageGuard(DTID dt_id, bool keep_alive = true)
       : bf(&BMC::global_bf->allocatePage()), guard(bf->header.latch, GUARD_STATE::EXCLUSIVE), keep_alive(keep_alive)
   {
      assert(BMC::global_bf != nullptr);
      bf->page.dt_id = dt_id;
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Root case
   HybridPageGuard(BufferFrame* bf) : bf(bf), guard(bf->header.latch)
   {
      guard.toOptimisticSpin();
      syncGSN();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Lock coupling
   template <typename T2>
   HybridPageGuard(HybridPageGuard<T2>& p_guard, Swip<T>& swip, const FALLBACK_METHOD if_contended = FALLBACK_METHOD::SPIN)
       : bf(&BMC::global_bf->tryFastResolveSwip(p_guard.guard, swip.template cast<BufferFrame>())), guard(bf->header.latch)
   {
      if (if_contended == FALLBACK_METHOD::SPIN) {
         guard.toOptimisticSpin();
      } else if (if_contended == FALLBACK_METHOD::EXCLUSIVE) {
         guard.toOptimisticOrExclusive();
      } else if (if_contended == FALLBACK_METHOD::SHARED) {
         guard.toOptimisticOrShared();
      }
      syncGSN();
      jumpmu_registerDestructor();
      // -------------------------------------------------------------------------------------
      DEBUG_BLOCK()
      {
         [[maybe_unused]] DTID p_dt_id = p_guard.bf->page.dt_id, dt_id = bf->page.dt_id;
         p_guard.recheck();
         recheck();
         assert(p_dt_id == dt_id);
      }
      // -------------------------------------------------------------------------------------
      p_guard.recheck();
   }
   // I: Downgrade exclusive
   HybridPageGuard(ExclusivePageGuard<T>&&) = delete;
   HybridPageGuard& operator=(ExclusivePageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // I: Downgrade shared
   HybridPageGuard(SharedPageGuard<T>&&) = delete;
   HybridPageGuard& operator=(SharedPageGuard<T>&&)
   {
      guard.unlock();
      return *this;
   }
   // -------------------------------------------------------------------------------------
   // Assignment operator
   constexpr HybridPageGuard& operator=(HybridPageGuard& other) = delete;
   template <typename T2>
   constexpr HybridPageGuard& operator=(HybridPageGuard<T2>&& other)
   {
      bf = other.bf;
      guard = std::move(other.guard);
      keep_alive = other.keep_alive;
      manually_checked = other.manually_checked;
      return *this;
   }
   // -------------------------------------------------------------------------------------
   inline void syncGSN()
   {
      if (FLAGS_wal) {
         auto current_gsn = cr::Worker::my().getCurrentGSN();
         if (current_gsn < bf->page.GSN) {
            cr::Worker::my().setCurrentGSN(bf->page.GSN);
         }
      }
   }
   // -------------------------------------------------------------------------------------
   inline bool hasFacedContention() { return guard.faced_contention; }
   inline void kill() { guard.unlock(); }
   inline void recheck() { guard.recheck(); }
   inline void recheck_done()
   {
      manually_checked = true;
      guard.recheck();
   }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(bf); }
   inline T* operator->() { return reinterpret_cast<T*>(bf->page.dt); }
   // -------------------------------------------------------------------------------------
   void reclaim()
   {
      BMC::global_bf->reclaimPage(*(bf));
      guard.state = GUARD_STATE::MOVED;
   }
   // -------------------------------------------------------------------------------------
   jumpmu_defineCustomDestructor(HybridPageGuard)
       // -------------------------------------------------------------------------------------
       ~HybridPageGuard()
   {
      if (guard.state == GUARD_STATE::EXCLUSIVE) {
         if (!keep_alive) {
            reclaim();
         }
      }
      guard.unlock();
      jumpmu::clearLastDestructor();
   }
};
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard
{
  private:
   HybridPageGuard<T>& ref_guard;

  public:
   // -------------------------------------------------------------------------------------
   // I: Upgrade
   ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : ref_guard(o_guard)
   {
      ref_guard.guard.toExclusive();
      if (!FLAGS_wal) {
         ref_guard.bf->page.GSN++;
      }
   }
   // -------------------------------------------------------------------------------------
   template <typename WT>
   cr::Worker::WALEntryHandler<WT> reserveWALEntry(u64 extra_size)
   {
      assert(FLAGS_wal);
      LID gsn = std::max<LID>(ref_guard.bf->page.GSN, cr::Worker::my().getCurrentGSN()) + 1;
      ref_guard.bf->page.GSN = gsn;
      cr::Worker::my().setCurrentGSN(gsn);
      auto handler = cr::Worker::my().reserveDTEntry<WT>(sizeof(WT) + extra_size);
      handler->pid = ref_guard.bf->header.pid;
      handler->gsn = gsn;
      return handler;
   }
   // -------------------------------------------------------------------------------------
   inline void submitWALEntry(u64 requested_size) { cr::Worker::my().submitDTEntry(requested_size); }
   // -------------------------------------------------------------------------------------
   template <typename... Args>
   void init(Args&&... args)
   {
      new (ref_guard.bf->page.dt) T(std::forward<Args>(args)...);
   }
   // -------------------------------------------------------------------------------------
   void keepAlive() { ref_guard.keep_alive = true; }
   // -------------------------------------------------------------------------------------
   ~ExclusivePageGuard()
   {
      if (!ref_guard.keep_alive && ref_guard.guard.state == GUARD_STATE::EXCLUSIVE) {
         ref_guard.reclaim();
      } else {
         ref_guard.guard.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bf); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline BufferFrame* bf() { return ref_guard.bf; }
   inline void reclaim() { ref_guard.reclaim(); }
};
// -------------------------------------------------------------------------------------
template <typename T>
class SharedPageGuard
{
  public:
   HybridPageGuard<T>& ref_guard;
   // I: Upgrade
   SharedPageGuard(HybridPageGuard<T>&& h_guard) : ref_guard(h_guard) { ref_guard.guard.toShared(); }
   // -------------------------------------------------------------------------------------
   ~SharedPageGuard() { ref_guard.guard.unlock(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(ref_guard.bf); }
   inline T* operator->() { return reinterpret_cast<T*>(ref_guard.bf->page.dt); }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
