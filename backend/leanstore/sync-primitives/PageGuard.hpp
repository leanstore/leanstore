#pragma once
#include "Exceptions.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
template <typename T>
class ExclusivePageGuard;
template <typename T>
class SharedPageGuard;
template <typename T>
class OptimisticPageGuard
{
 protected:
  OptimisticPageGuard() : moved(true) {}
  OptimisticPageGuard(OptimisticLatch& swip_version) { bf_s_lock = OptimisticGuard(swip_version); }
  OptimisticPageGuard(OptimisticGuard read_guard, BufferFrame* bf) : moved(false), bf(bf), bf_s_lock(read_guard) {}
  // -------------------------------------------------------------------------------------
  bool manually_checked = false;
  // -------------------------------------------------------------------------------------
 public:
  // -------------------------------------------------------------------------------------
  bool moved = false;
  BufferFrame* bf = nullptr;
  OptimisticGuard bf_s_lock;
  // -------------------------------------------------------------------------------------
  static OptimisticPageGuard manuallyAssembleGuard(OptimisticGuard read_guard, BufferFrame* bf) { return OptimisticPageGuard(read_guard, bf); }
  // -------------------------------------------------------------------------------------
  // I: Root case
  static OptimisticPageGuard makeRootGuard(OptimisticLatch& swip_version) { return OptimisticPageGuard(swip_version); }
  // -------------------------------------------------------------------------------------
  // I: Lock coupling
  OptimisticPageGuard(OptimisticPageGuard& p_guard, Swip<T>& swip)
  {
    assert(!(p_guard.bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
    if (p_guard.moved == true) {
      assert(false);
    }
    auto& bf_swip = swip.template cast<BufferFrame>();
    bf = &BMC::global_bf->resolveSwip(p_guard.bf_s_lock, bf_swip);
    bf_s_lock = OptimisticGuard(bf->header.lock);
    p_guard.recheck();  // TODO: ??
  }
  // I: Downgrade
  OptimisticPageGuard(ExclusivePageGuard<T>&& other)
  {
    assert(!other.moved);
    bf = other.bf;
    bf_s_lock = other.bf_s_lock;
    bf_s_lock.local_version = LATCH_EXCLUSIVE_BIT + bf_s_lock.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT);
    moved = false;
    // -------------------------------------------------------------------------------------
    other.moved = true;
    assert((bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == 0);
  }
  // I: Downgrade
  OptimisticPageGuard(SharedPageGuard<T> &&other)
  {
    bf = other.bf;
    bf_s_lock = other.bf_s_lock;
    SharedGuard::unlatch(bf_s_lock);
    moved = false;
    // -------------------------------------------------------------------------------------
    other.moved = true;
  }
  // -------------------------------------------------------------------------------------
  // Assignment operator
  constexpr OptimisticPageGuard& operator=(OptimisticPageGuard&& other)
  {
    bf = other.bf;
    bf_s_lock = other.bf_s_lock;
    moved = false;
    manually_checked = false;
    // -------------------------------------------------------------------------------------
    other.moved = true;
    return *this;
  }
  // -------------------------------------------------------------------------------------
  template <typename T2>
  OptimisticPageGuard<T2>& cast()
  {
    return *reinterpret_cast<OptimisticPageGuard<T2>*>(this);
  }
  // -------------------------------------------------------------------------------------
  // Copy constructor
  OptimisticPageGuard(OptimisticPageGuard& other)
  {
    bf = other.bf;
    bf_s_lock = other.bf_s_lock;
    moved = other.moved;
    manually_checked = other.manually_checked;
  }
  // -------------------------------------------------------------------------------------
  void recheck() { bf_s_lock.recheck(); }
  void recheck_done()
  {
    manually_checked = true;
    bf_s_lock.recheck();
  }
  // -------------------------------------------------------------------------------------
  // Guard not needed anymore
  void kill() { moved = true; }
  T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }
  T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }
  Swip<T> swip() { return Swip<T>(bf); }
  T* operator->() { return reinterpret_cast<T*>(bf->page.dt); }
  // Is the bufferframe loaded
  bool hasBf() const { return bf != nullptr; }
  // -------------------------------------------------------------------------------------
  ~OptimisticPageGuard() noexcept(false)
  {
#ifdef DEBUG
    if (!manually_checked && !moved && std::uncaught_exceptions() == 0) {
      raise(SIGTRAP);
    }
#endif
  }
};
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard : public OptimisticPageGuard<T>
{
  using OptimisticClass = OptimisticPageGuard<T>;

 protected:
  bool keep_alive = true;  // for the case when more than one page is allocated
                           // (2nd might fail and waste the first)
  // Called by the buffer manager when allocating a new page
  ExclusivePageGuard(BufferFrame& bf, bool keep_alive) : keep_alive(keep_alive)
  {
    OptimisticClass::bf = &bf;
    OptimisticClass::bf_s_lock = OptimisticGuard(&bf.header.lock, bf.header.lock->load());
    OptimisticClass::moved = false;
    assert((OptimisticClass::bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
 public:
  // I: Upgrade
  ExclusivePageGuard(OptimisticClass&& o_guard)
  {
    o_guard.recheck();
    OptimisticClass::bf = o_guard.bf;
    OptimisticClass::bf_s_lock = o_guard.bf_s_lock;
    // -------------------------------------------------------------------------------------
    ExclusiveGuard::latch(OptimisticClass::bf_s_lock);
    // -------------------------------------------------------------------------------------
    o_guard.moved = true;
    OptimisticClass::moved = false;
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
    new (OptimisticClass::bf->page.dt) T(std::forward<Args>(args)...);
  }
  // -------------------------------------------------------------------------------------
  void keepAlive() { keep_alive = true; }
  // -------------------------------------------------------------------------------------
  void reclaim()
  {
    BMC::global_bf->reclaimPage(*OptimisticClass::bf);
    OptimisticClass::moved = true;
  }
  // -------------------------------------------------------------------------------------
  ~ExclusivePageGuard()
  {
    if (!OptimisticClass::moved) {
      if (!keep_alive) {
        reclaim();
      } else {
        if (OptimisticClass::hasBf()) {
          OptimisticClass::bf->page.LSN++;  // TODO: LSN
        }
        ExclusiveGuard::unlatch(OptimisticClass::bf_s_lock);
        OptimisticClass::moved = true;
      }
      // -------------------------------------------------------------------------------------
    }
  }
};
// -------------------------------------------------------------------------------------
template <typename T>
class SharedPageGuard : public OptimisticPageGuard<T>
{
  using OptimisticClass = OptimisticPageGuard<T>;
public:
  SharedPageGuard(OptimisticPageGuard<T>&& o_guard) {
    OptimisticClass::bf = o_guard.bf;
    OptimisticClass::bf_s_lock = o_guard.bf_s_lock;
    // -------------------------------------------------------------------------------------
    SharedGuard::latch(OptimisticClass::bf_s_lock);
    // -------------------------------------------------------------------------------------
    o_guard.moved = true;
    OptimisticClass::moved = false;
  }
  ~SharedPageGuard() {
    if (!OptimisticClass::moved) {
      SharedGuard::unlatch(OptimisticClass::bf_s_lock);
    }
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
