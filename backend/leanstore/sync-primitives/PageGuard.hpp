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
class WritePageGuard;
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
  // I: Root case
  static OptimisticPageGuard makeRootGuard(OptimisticLatch& swip_version) { return OptimisticPageGuard(swip_version); }
  // -------------------------------------------------------------------------------------
  static OptimisticPageGuard manuallyAssembleGuard(OptimisticGuard read_guard, BufferFrame* bf) { return OptimisticPageGuard(read_guard, bf); }
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
  OptimisticPageGuard(WritePageGuard<T>&& other)
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
  // -------------------------------------------------------------------------------------
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
  // Guard not needed anymore
  void kill() { moved = true; }
  // -------------------------------------------------------------------------------------
  T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }
  T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }
  Swip<T> swip() { return Swip<T>(bf); }
  T* operator->() { return reinterpret_cast<T*>(bf->page.dt); }
  // Is the bufferframe loaded
  bool hasBf() const { return bf != nullptr; }
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
class WritePageGuard : public OptimisticPageGuard<T>
{
  using ReadClass = OptimisticPageGuard<T>;

 protected:
  bool keep_alive = true;  // for the case when more than one page is allocated
                           // (2nd might fail and waste the first)
  // Called by the buffer manager when allocating a new page
  WritePageGuard(BufferFrame& bf, bool keep_alive) : keep_alive(keep_alive)
  {
    ReadClass::bf = &bf;
    ReadClass::bf_s_lock = OptimisticGuard(&bf.header.lock, bf.header.lock->load());
    ReadClass::moved = false;
    assert((ReadClass::bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
  }
  // -------------------------------------------------------------------------------------
 public:
  // I: Upgrade
  WritePageGuard(ReadClass&& read_guard)
  {
    read_guard.recheck();
    ReadClass::bf = read_guard.bf;
    ReadClass::bf_s_lock = read_guard.bf_s_lock;
    OptimisticLatchVersionType new_version = ReadClass::bf_s_lock.local_version + LATCH_EXCLUSIVE_BIT;
    if (!std::atomic_compare_exchange_strong(ReadClass::bf_s_lock.latch_ptr->ptr(), &ReadClass::bf_s_lock.local_version, new_version)) {
      throw RestartException();
    }
    ReadClass::bf_s_lock.local_version = new_version;
    read_guard.moved = true;
    ReadClass::moved = false;
  }
  // -------------------------------------------------------------------------------------
  static WritePageGuard allocateNewPage(DTID dt_id, bool keep_alive = true)
  {
    ensure(BMC::global_bf != nullptr);
    auto& bf = BMC::global_bf->allocatePage();
    bf.page.dt_id = dt_id;
    return WritePageGuard(bf, keep_alive);
  }

  template <typename... Args>
  void init(Args&&... args)
  {
    new (ReadClass::bf->page.dt) T(std::forward<Args>(args)...);
  }
  // -------------------------------------------------------------------------------------
  void keepAlive() { keep_alive = true; }
  // -------------------------------------------------------------------------------------
  void reclaim()
  {
    BMC::global_bf->reclaimPage(*ReadClass::bf);
    ReadClass::moved = true;
  }
  // -------------------------------------------------------------------------------------
  ~WritePageGuard()
  {
    if (!ReadClass::moved) {
      if (!keep_alive) {
        reclaim();
      } else {
        assert((ReadClass::bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
        if (ReadClass::hasBf()) {
          ReadClass::bf->page.LSN++;  // TODO: LSN
        }
        ReadClass::bf_s_lock.local_version = LATCH_EXCLUSIVE_BIT + ReadClass::bf_s_lock.latch_ptr->ref().fetch_add(LATCH_EXCLUSIVE_BIT);
        ReadClass::moved = true;
      }
      // -------------------------------------------------------------------------------------
    }
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
