#pragma once
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
template<typename T>
class PageGuard {
   // -------------------------------------------------------------------------------------
   struct WriteLock {
      // -------------------------------------------------------------------------------------
      PageGuard &guard;
      // -------------------------------------------------------------------------------------
      WriteLock(PageGuard &guard)
              : guard(guard)
      {
         assert (guard.write_lock_counter++ == 0);
         lock_version_t new_version = guard.bf_s_lock.local_version + 2;
         if ( !std::atomic_compare_exchange_strong(guard.bf_s_lock.version_ptr, &guard.bf_s_lock.local_version, new_version)) {
            throw RestartException();
         }
         guard.bf_s_lock.local_version = new_version;
      }
      ~WriteLock()
      {
         assert(guard.bf_s_lock.version_ptr != nullptr);
         assert(--guard.write_lock_counter == 0);
         guard.bf_s_lock.local_version = 2 + guard.bf_s_lock.version_ptr->fetch_add(2);
      }
   };
   // -------------------------------------------------------------------------------------
   PageGuard() {}
public:
   BufferFrame *bf = nullptr;
   SharedLock bf_s_lock;
   u8 write_lock_counter = 0;

   template<typename O>
   PageGuard(PageGuard<O> &&other)
   {
      assert(other.write_lock_counter == 0);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
   }

   PageGuard &operator=(PageGuard &&other)
   {
      assert(other.write_lock_counter == 0);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
      return *this;
   }


   //used only by buffer manager
   PageGuard(BufferFrame &bf)
           : bf(&bf)
   {
      bf_s_lock = SharedLock(bf.header.lock);
   }
   static PageGuard makeRootGuard(OptimisticVersion &swip_version, Swip &swip)
   {
      PageGuard root_page;
      root_page.bf_s_lock = SharedLock(swip_version);
      return root_page;
   }
   PageGuard(PageGuard &p_guard, Swip &swip)
   {
      bf = &BMC::global_bf->resolveSwip(p_guard.bf_s_lock, swip);
      bf_s_lock = SharedLock(bf->header.lock);
      p_guard.recheck();
   }
   void recheck()
   {
      bf_s_lock.recheck();
   }
   WriteLock writeLock()
   {
      return WriteLock(*this);
   }
   T *operator->()
   {
      return reinterpret_cast<T *>(bf->page.dt);
   }
   operator bool() const
   {
      return bf != nullptr;
   }
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

template<typename T>
class WritePageGuard;

template<typename T>
class ReadPageGuard {
   ReadPageGuard() {}
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
      return *this;
   }
   // Casting helpers
   template<typename O>
   ReadPageGuard(ReadPageGuard<O> &&other)
   {
      assert(other.write_lock_counter == 0);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
   }
   ReadPageGuard &operator=(ReadPageGuard &&other)
   {
      assert(other.write_lock_counter == 0);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.recheck();
      return *this;
   }

   void recheck()
   {
      bf_s_lock.recheck();
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
class WritePageGuard {
   BufferFrame *bf = nullptr;
   SharedLock bf_s_lock;
public:
   WritePageGuard(ReadPageGuard<T> &&read_guard) {
      assert(read_guard);
      bf = read_guard.bf;
      bf_s_lock = read_guard.bf_s_lock;
      lock_version_t new_version = bf_s_lock.local_version + 2;
      if ( !std::atomic_compare_exchange_strong(bf_s_lock.version_ptr, &bf_s_lock.local_version, new_version)) {
         throw RestartException();
      }
      bf_s_lock.local_version = new_version;
   }

   ~WritePageGuard() {
      assert((bf_s_lock.version_ptr->load() & 2) != 2);
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
