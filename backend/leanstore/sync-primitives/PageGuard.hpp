#pragma once
#include "Exceptions.hpp"
#include "Latch.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
#define PAGE_GUARD_HEADER    \
  BufferFrame* bf = nullptr; \
  Guard guard;

#define PAGE_GUARD_UTILS                                        \
  T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }       \
  T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }        \
  Swip<T> swip() { return Swip<T>(bf); }                        \
  T* operator->() { return reinterpret_cast<T*>(bf->page.dt); } \
  bool hasBf() const { return bf != nullptr; }

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
  HybridPageGuard(HybridLatch& latch) : guard(latch) { jumpmu_registerDestructor(); }
  // -------------------------------------------------------------------------------------
 public:
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_HEADER
  bool manually_checked = false;
  // -------------------------------------------------------------------------------------
  HybridPageGuard() : bf(nullptr), guard(nullptr) { jumpmu_registerDestructor(); }  // use with caution
  HybridPageGuard(Guard& guard, BufferFrame* bf) : bf(bf), guard(std::move(guard)) { jumpmu_registerDestructor(); }
  // -------------------------------------------------------------------------------------
  // Copy constructor
  HybridPageGuard(HybridPageGuard& other) = delete;
  // Move constructor
  HybridPageGuard(HybridPageGuard&& other) : bf(other.bf), guard(std::move(other.guard)) { jumpmu_registerDestructor(); }
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  // I: Root case
  static HybridPageGuard makeRootGuard(HybridLatch& latch_guard) { return HybridPageGuard(latch_guard); }
  // -------------------------------------------------------------------------------------
  // I: Lock coupling
  HybridPageGuard(HybridPageGuard& p_guard, Swip<T>& swip, FALLBACK_METHOD if_contended = FALLBACK_METHOD::SPIN)
      : bf(&BMC::global_bf->resolveSwip(p_guard.guard, swip.template cast<BufferFrame>())), guard(bf->header.latch)
  {
    if (if_contended == FALLBACK_METHOD::SPIN) {
      guard.transition<GUARD_STATE::OPTIMISTIC, FALLBACK_METHOD::SPIN>();
    } else if (if_contended == FALLBACK_METHOD::EXCLUSIVE) {
      guard.transition<GUARD_STATE::OPTIMISTIC, FALLBACK_METHOD::EXCLUSIVE>();
    } else if (if_contended == FALLBACK_METHOD::SHARED) {
      guard.transition<GUARD_STATE::OPTIMISTIC, FALLBACK_METHOD::SHARED>();
    }
    jumpmu_registerDestructor();
    p_guard.recheck();
  }
  // I: Downgrade exclusive
  HybridPageGuard& operator=(ExclusivePageGuard<T>&& other)
  {
    bf = other.bf;
    guard = std::move(other.guard);
    // -------------------------------------------------------------------------------------
    if (hasBf()) {
      bf->page.LSN++;
    }
    guard.transition<GUARD_STATE::OPTIMISTIC>();
    return *this;
  }
  // I: Downgrade shared
  HybridPageGuard(SharedPageGuard<T>&&) = delete;
  HybridPageGuard& operator=(SharedPageGuard<T>&& other)
  {
    // TODO
  }
  // -------------------------------------------------------------------------------------
  // Assignment operator
  constexpr HybridPageGuard& operator=(HybridPageGuard& other) = delete;
  constexpr HybridPageGuard& operator=(HybridPageGuard&& other)
  {
    bf = other.bf;
    guard = std::move(other.guard);
    return *this;
  }
  // -------------------------------------------------------------------------------------
  bool hasFacedContention() { return guard.faced_contention; }
  void kill() { guard.transition<GUARD_STATE::OPTIMISTIC>(); }
  void recheck() { guard.recheck(); }
  void recheck_done()
  {
    manually_checked = true;
    guard.recheck();
  }
  // -------------------------------------------------------------------------------------
  // Guard not needed anymore
  PAGE_GUARD_UTILS
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(HybridPageGuard)
      // -------------------------------------------------------------------------------------
      ~HybridPageGuard()
  {
    guard.transition<GUARD_STATE::OPTIMISTIC>();
    jumpmu::clearLastDestructor();
  }
};
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard
{
 public:
  PAGE_GUARD_HEADER
 protected:
  bool keep_alive = true;  // for the case when more than one page is allocated
                           // (2nd might fail and waste the first)
  // Called by the buffer manager when allocating a new page
  ExclusivePageGuard(BufferFrame& bf, bool keep_alive)
      : bf(&bf), guard(bf.header.latch, GUARD_STATE::EXCLUSIVE, bf.header.latch->load()), keep_alive(keep_alive)
  {
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
 public:
  // -------------------------------------------------------------------------------------
  // I: Upgrade
  ExclusivePageGuard(HybridPageGuard<T>&& o_guard) : bf(o_guard.bf), guard(std::move(o_guard.guard))
  {
    guard.transition<GUARD_STATE::EXCLUSIVE>();
    // -------------------------------------------------------------------------------------
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  static ExclusivePageGuard allocateNewPage(DTID dt_id, bool keep_alive = true)
  {
    ensure(BMC::global_bf != nullptr);
    auto& bf = BMC::global_bf->allocatePage();
    bf.page.dt_id = dt_id;
    return ExclusivePageGuard(bf, keep_alive);
  }

  template <typename... Args>
  void init(Args&&... args)
  {
    new (bf->page.dt) T(std::forward<Args>(args)...);
  }
  // -------------------------------------------------------------------------------------
  void keepAlive() { keep_alive = true; }
  // -------------------------------------------------------------------------------------
  void reclaim()
  {
    BMC::global_bf->reclaimPage(*bf);
    guard.state = GUARD_STATE::MOVED;
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusivePageGuard)
      // -------------------------------------------------------------------------------------
      ~ExclusivePageGuard()
  {
    if (guard.state == GUARD_STATE::EXCLUSIVE) {
      if (!keep_alive) {
        reclaim();
      } else {
        if (hasBf()) {
          bf->page.LSN++;
        }
        guard.transition<GUARD_STATE::OPTIMISTIC>();
      }
    }
    // -------------------------------------------------------------------------------------
    jumpmu::clearLastDestructor();
  }
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_UTILS
};
// -------------------------------------------------------------------------------------
// Plan: take the mutex in shared mode
template <typename T>
class SharedPageGuard
{
 public:
  PAGE_GUARD_HEADER
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_UTILS
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
