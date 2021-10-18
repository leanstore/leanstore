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
  protected:
   void latchAccordingToFallbackMode(Guard& guard, const LATCH_FALLBACK_MODE if_contended)
   {
      if (if_contended == LATCH_FALLBACK_MODE::SPIN) {
         guard.toOptimisticSpin();
      } else if (if_contended == LATCH_FALLBACK_MODE::EXCLUSIVE) {
         guard.toOptimisticOrExclusive();
      } else if (if_contended == LATCH_FALLBACK_MODE::SHARED) {
         guard.toOptimisticOrShared();
      } else if (if_contended == LATCH_FALLBACK_MODE::JUMP) {
         guard.toOptimisticOrJump();
      } else {
         UNREACHABLE();
      }
   }

  public:
   BufferFrame* bf = nullptr;
   Guard guard;
   bool keep_alive = true;
   // -------------------------------------------------------------------------------------
   // Constructors
   HybridPageGuard() : bf(nullptr), guard(nullptr) { jumpmu_registerDestructor(); }  // use with caution
   HybridPageGuard(Guard&& guard, BufferFrame* bf) : bf(bf), guard(std::move(guard)) { jumpmu_registerDestructor(); }
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
   HybridPageGuard(Swip<BufferFrame> sentinal_swip, const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
       : bf(&sentinal_swip.asBufferFrame()), guard(bf->header.latch)
   {
      latchAccordingToFallbackMode(guard, if_contended);
      syncGSN();
      jumpmu_registerDestructor();
   }
   // -------------------------------------------------------------------------------------
   // I: Lock coupling
   template <typename T2>
   HybridPageGuard(HybridPageGuard<T2>& p_guard, Swip<T>& swip, const LATCH_FALLBACK_MODE if_contended = LATCH_FALLBACK_MODE::SPIN)
       : bf(&BMC::global_bf->tryFastResolveSwip(p_guard.guard, swip.template cast<BufferFrame>())), guard(bf->header.latch)
   {
      latchAccordingToFallbackMode(guard, if_contended);
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
      return *this;
   }
   // -------------------------------------------------------------------------------------
   inline void incrementGSN()
   {
      assert(bf != nullptr);
      // TODO: this is a temporary hack, we should write WAL entries for every page we write and enable this check ensure(!FLAGS_wal);
      bf->page.GSN++;
      cr::Worker::my().setCurrentGSN(std::max<LID>(cr::Worker::my().getCurrentGSN(), bf->page.GSN));
   }
   // WAL
   inline void syncGSN()
   {
      // TODO: don't sync on temporary table pages like VersionsSpace
      if (FLAGS_wal) {
         if (FLAGS_wal_rfa) {
            if (bf->page.GSN > cr::Worker::my().rfa_gsn_flushed && bf->header.last_writer_worker_id != cr::Worker::my().worker_id) {
               cr::Worker::my().needs_remote_flush = true;
            }
         }
         cr::Worker::my().setCurrentGSN(std::max<LID>(cr::Worker::my().getCurrentGSN(), bf->page.GSN));
      }
   }
   template <typename WT>
   cr::Worker::WALEntryHandler<WT> reserveWALEntry(u64 extra_size)
   {
      assert(FLAGS_wal);
      assert(guard.state == GUARD_STATE::EXCLUSIVE);
      const LID new_gsn = std::max<LID>(bf->page.GSN, cr::Worker::my().getCurrentGSN()) + 1;
      bf->header.last_writer_worker_id = cr::Worker::my().worker_id;  // RFA
      bf->page.GSN = new_gsn;
      cr::Worker::my().setCurrentGSN(new_gsn);
      // -------------------------------------------------------------------------------------
      const auto pid = bf->header.pid;
      const auto dt_id = bf->page.dt_id;
      auto handler = cr::Worker::my().reserveDTEntry<WT>(sizeof(WT) + extra_size, pid, new_gsn, dt_id);
      return handler;
   }
   inline void submitWALEntry(u64 total_size) { cr::Worker::my().submitDTEntry(total_size); }
   // -------------------------------------------------------------------------------------
   inline bool hasFacedContention() { return guard.faced_contention; }
   inline void unlock() { guard.unlock(); }
   inline void recheck() { guard.recheck(); }
   // -------------------------------------------------------------------------------------
   inline T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }
   inline T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }
   inline Swip<T> swip() { return Swip<T>(bf); }
   inline T* operator->() { return reinterpret_cast<T*>(bf->page.dt); }
   // -------------------------------------------------------------------------------------
   // Use with caution!
   void toShared() { guard.toShared(); }
   void toExclusive() { guard.toExclusive(); }
   void tryToShared() { guard.tryToShared(); }        // Can jump
   void tryToExclusive() { guard.tryToExclusive(); }  // Can jump
   // -------------------------------------------------------------------------------------
   // Parent should be exclusively locked, we need a Swip in the parent
   // This is the child page that we want to cool
   void cool(Swip<BufferFrame>& swip_in_parent)
   {
      BMC::global_bf->coolPage(*bf);
      swip_in_parent.cool();
      unlock();
      guard.state = GUARD_STATE::MOVED;
      // Should not use this page guard at this point
   }
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
   ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : ref_guard(o_guard) { ref_guard.guard.toExclusive(); }
   // -------------------------------------------------------------------------------------
   template <typename WT>
   cr::Worker::WALEntryHandler<WT> reserveWALEntry(u64 extra_size)
   {
      return ref_guard.template reserveWALEntry<WT>(extra_size);
   }
   // -------------------------------------------------------------------------------------
   inline void submitWALEntry(u64 total_size) { ref_guard.submitWALEntry(total_size); }
   // -------------------------------------------------------------------------------------
   template <typename... Args>
   void init(Args&&... args)
   {
      new (ref_guard.bf->page.dt) T(std::forward<Args>(args)...);
   }
   // -------------------------------------------------------------------------------------
   void keepAlive() { ref_guard.keep_alive = true; }
   void incrementGSN() { ref_guard.incrementGSN(); }
   // -------------------------------------------------------------------------------------
   ~ExclusivePageGuard()
   {
      if (!ref_guard.keep_alive && ref_guard.guard.state == GUARD_STATE::EXCLUSIVE) {
         ref_guard.reclaim();
      } else {
         ref_guard.unlock();
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
   SharedPageGuard(HybridPageGuard<T>&& h_guard) : ref_guard(h_guard) { ref_guard.toShared(); }
   // -------------------------------------------------------------------------------------
   ~SharedPageGuard() { ref_guard.unlock(); }
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
