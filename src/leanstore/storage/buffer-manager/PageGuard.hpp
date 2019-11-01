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
   ReadPageGuard() {}
   bool moved = false;
public:
   BufferFrame *bf = nullptr;
   SharedLock bf_s_lock;

   // I: Root case
   static ReadPageGuard makeRootGuard(OptimisticVersion &swip_version, Swip &swip)
   {
      ReadPageGuard root_page;
      root_page.bf_s_lock = SharedLock(swip_version);
      return root_page;
   }
   // I: Lock coupling
   ReadPageGuard(ReadPageGuard &p_guard, Swip &swip)
   {
      bf = &BMC::global_bf->resolveSwip(p_guard.bf_s_lock, swip);
      bf_s_lock = SharedLock(bf->header.lock);
      p_guard.recheck();
   }

   // I: Downgrade
   ReadPageGuard &operator=(WritePageGuard<T> &&other)
   {
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.local_version = 2 + bf_s_lock.version_ptr->fetch_add(2);
      // -------------------------------------------------------------------------------------
      other.moved = true;
      return *this;
   }
   // Casting helpers
   template<typename O>
   ReadPageGuard(ReadPageGuard<O> &&other)
   {
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
   }
   ReadPageGuard &operator=(ReadPageGuard &&other)
   {
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
      return *this;
   }

   void recheck()
   {
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
};
// -------------------------------------------------------------------------------------
template<typename T>
class WritePageGuard : public ReadPageGuard<T> {
   using parent = ReadPageGuard<T>;
protected:
   WritePageGuard() {};
   // Called when creating a new page
   WritePageGuard(BufferFrame *bf) {
      parent::bf = bf;
      parent::bf_s_lock = SharedLock(&bf->header.lock, bf->header.lock.load(), true);
      parent::moved = false;
   };
public:
   WritePageGuard(ReadPageGuard<T> &&read_guard) {
      parent::bf = read_guard.bf;
      parent::bf_s_lock = read_guard.bf_s_lock;
      lock_version_t new_version = parent::bf_s_lock.local_version + 2;
      if ( !std::atomic_compare_exchange_strong(parent::bf_s_lock.version_ptr, &parent::bf_s_lock.local_version, new_version)) {
         throw RestartException();
      }
      parent::bf_s_lock.local_version = new_version;
   }
   template<typename... Args>
   static WritePageGuard allocateNewPage(Args &&... args)
   {
      auto bf = &BMC::global_bf->allocatePage();
      new(bf->page.dt) T(std::forward<Args>(args)...);
      return WritePageGuard(bf);
   }

   ~WritePageGuard() {
      if(!parent::moved) {
         parent::bf_s_lock.version_ptr->fetch_add(2);
      }
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
