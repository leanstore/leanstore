#pragma once
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
template<typename T>
class WritePageGuard;
template<typename T>
class ReadPageGuard {
protected:
   ReadPageGuard() : moved(true) {}
   ReadPageGuard(OptimisticVersion &swip_version) {
      bf_s_lock = SharedGuard(swip_version);
   }
   bool manually_checked = false;
public:
   bool moved = false;
   BufferFrame *bf = nullptr;
   SharedGuard bf_s_lock;

   // I: Root case
   static ReadPageGuard makeRootGuard(OptimisticVersion &swip_version)
   {
      return ReadPageGuard(swip_version);
   }
   // I: Lock coupling
   ReadPageGuard(ReadPageGuard &p_guard, Swip<T> &swip)
   {
      assert(p_guard.moved == false);
      auto &bf_swip = swip.template cast<BufferFrame>();
      bf = &BMC::global_bf->resolveSwip(p_guard.bf_s_lock, bf_swip);
      bf_s_lock = SharedGuard(bf->header.lock);
      p_guard.recheck();
   }
   // I: Downgrade
   ReadPageGuard &operator=(WritePageGuard<T> &&other)
   {
      assert(other.moved == false);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.local_version = 2 + bf_s_lock.version_ptr->fetch_add(2);
      // -------------------------------------------------------------------------------------
      other.moved = true;
      return *this;
   }
   // Casting helpers
   template<typename O> // TODO: cast is better, but is it really better ?
   ReadPageGuard(ReadPageGuard<O> &&other)
   {
      UNREACHABLE();
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck(); //TODO: do we really need to recheck while casting ?
      moved = false;
      other.moved = true;
   }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   ReadPageGuard<T2> &cast() {
      return *reinterpret_cast<ReadPageGuard<T2>*>(this);
   }
   // -------------------------------------------------------------------------------------
   ReadPageGuard &operator=(ReadPageGuard &&other)
   {
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
      moved = false;
      other.moved = true;
      return *this;
   }

   void recheck()
   {
      manually_checked = true;
      bf_s_lock.recheck();
   }
   T &ref() {
      return *reinterpret_cast<T *>(bf->page.dt);
   }

   T *operator->()
   {
      return reinterpret_cast<T *>(bf->page.dt);
   }
   // Is the bufferframe loaded
   operator bool() const
   {
      return bf != nullptr;
   }
   ~ReadPageGuard() noexcept(false) {
      if(!manually_checked && !moved && std::uncaught_exceptions() == 0) {
         recheck();
      }
   }
};
// -------------------------------------------------------------------------------------
template<typename T>
class WritePageGuard : public ReadPageGuard<T> {
   using ReadGuard = ReadPageGuard<T>;
protected:
   // Called by the buffer manager when allocating a new page
   WritePageGuard(BufferFrame *bf) {
      ReadGuard::bf = bf;
      ReadGuard::bf_s_lock = SharedGuard(&bf->header.lock, bf->header.lock.load(), true);
      ReadGuard::moved = false;
   }
public:
   // I: Upgrade
   WritePageGuard(ReadGuard &&read_guard) {
      ReadGuard::bf = read_guard.bf;
      ReadGuard::bf_s_lock = read_guard.bf_s_lock;
      lock_version_t new_version = ReadGuard::bf_s_lock.local_version + 2;
      if ( !std::atomic_compare_exchange_strong(ReadGuard::bf_s_lock.version_ptr, &ReadGuard::bf_s_lock.local_version, new_version)) {
         throw RestartException();
      }
      ReadGuard::bf_s_lock.local_version = new_version;
      read_guard.moved = true;
      ReadGuard::moved = false;
   }

   static WritePageGuard allocateNewPage(DTType dt_type)
   {
      auto bf = &BMC::global_bf->allocatePage();
      bf->page.dt_type = dt_type;
      return WritePageGuard(bf);
   }

   template<typename... Args>
   void init(Args &&... args) {
      new (ReadGuard::bf->page.dt) T(std::forward<Args>(args)...);
   }
   ~WritePageGuard() {
      if(!ReadGuard::moved) {
         ReadGuard::bf_s_lock.local_version = 2 + ReadGuard::bf_s_lock.version_ptr->fetch_add(2);
         ReadGuard::moved = true;
      }
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
