#pragma once
#include "Exceptions.hpp"
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
// Objects of this class must be thread local !
template<typename T>
class WritePageGuard;
template<typename T>
class ReadPageGuard {
protected:
   ReadPageGuard()
           : moved(true) {}
   ReadPageGuard(OptimisticLock &swip_version)
   {
      bf_s_lock = ReadGuard(swip_version);
   }
   ReadPageGuard(ReadGuard read_guard, BufferFrame *bf)
           : moved(false)
             , bf(bf)
             , bf_s_lock(read_guard)
   {
   }
   bool manually_checked = false;
public:
   bool moved = false;
   BufferFrame *bf = nullptr;
   ReadGuard bf_s_lock;
   // I: Root case
   static ReadPageGuard makeRootGuard(OptimisticLock &swip_version)
   {
      return ReadPageGuard(swip_version);
   }
   // -------------------------------------------------------------------------------------
   static ReadPageGuard manuallyAssembleGuard(ReadGuard read_guard, BufferFrame *bf)
   {
      return ReadPageGuard(read_guard, bf);
   }
   // I: Lock coupling
   ReadPageGuard(ReadPageGuard &p_guard, Swip <T> &swip)
   {
      if ( p_guard.moved == true ) {
         assert(false);
      }
      if ( swip.isSwizzled()) {
         bf = &swip.asBufferFrame();
         p_guard.recheck();
      } else {
         auto &bf_swip = swip.template cast<BufferFrame>();
         bf = &BMC::global_bf->resolveSwip(p_guard.bf_s_lock, bf_swip);
      }
      p_guard.recheck(); // to prevent dereferencing dangling pointer
      bf_s_lock = ReadGuard(bf->header.lock);
      p_guard.recheck(); // TODO: ??
   }
   // I: Downgrade
   ReadPageGuard(WritePageGuard<T> &&other)
   {
      assert(!other.moved);
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      bf_s_lock.local_version = WRITE_LOCK_BIT + bf_s_lock.version_ptr->fetch_add(WRITE_LOCK_BIT);
      moved = false;
      // -------------------------------------------------------------------------------------
      other.moved = true;
   }
   // -------------------------------------------------------------------------------------
   template<typename T2>
   ReadPageGuard<T2> &cast()
   {
      return *reinterpret_cast<ReadPageGuard<T2> *>(this);
   }
   // -------------------------------------------------------------------------------------
   ReadPageGuard(ReadPageGuard &other)
   {
      bf = other.bf;
      bf_s_lock = other.bf_s_lock;
      moved = other.moved;
      manually_checked = other.manually_checked;
   }
   // -------------------------------------------------------------------------------------
   void recheck()
   {
      bf_s_lock.recheck();
   }
   void recheck_done()
   {
      manually_checked = true;
      bf_s_lock.recheck();
   }
   // Guard not needed anymore
   void kill()
   {
      moved = true;
   }
   // -------------------------------------------------------------------------------------
   T &ref()
   {
      return *reinterpret_cast<T *>(bf->page.dt);
   }
   T *ptr()
   {
      return reinterpret_cast<T *>(bf->page.dt);
   }
   Swip <T> swip()
   {
      return Swip<T>(bf);
   }
   T *operator->()
   {
      return reinterpret_cast<T *>(bf->page.dt);
   }
   // Is the bufferframe loaded
   bool hasBf() const
   {
      return bf != nullptr;
   }
   ~ReadPageGuard() noexcept(false)
   {
      if ( !manually_checked && !moved && std::uncaught_exceptions() == 0 ) {
         recheck();
      }
   }
};
// -------------------------------------------------------------------------------------
template<typename T>
class WritePageGuard : public ReadPageGuard<T> {
   using ParentClass = ReadPageGuard<T>;
protected:
   bool keep_alive = true;
   // Called by the buffer manager when allocating a new page
   WritePageGuard(BufferFrame &bf, bool keep_alive)
           : keep_alive(keep_alive)
   {
      ParentClass::bf = &bf;
      ParentClass::bf_s_lock = ReadGuard(&bf.header.lock, bf.header.lock.load());
      ParentClass::moved = false;
   }
public:
   // I: Upgrade
   WritePageGuard(ParentClass &&read_guard)
   {
      read_guard.recheck();
      ParentClass::bf = read_guard.bf;
      ParentClass::bf_s_lock = read_guard.bf_s_lock;
      lock_version_t new_version = ParentClass::bf_s_lock.local_version + WRITE_LOCK_BIT;
      if ( !std::atomic_compare_exchange_strong(ParentClass::bf_s_lock.version_ptr, &ParentClass::bf_s_lock.local_version, new_version)) {
         throw RestartException();
      }
      ParentClass::bf_s_lock.local_version = new_version;
      read_guard.moved = true;
      ParentClass::moved = false;
   }

   static WritePageGuard allocateNewPage(DTID dt_id, bool keep_alive = true)
   {
      ensure(BMC::global_bf != nullptr);
      auto &bf = BMC::global_bf->allocatePage();
      bf.page.dt_id = dt_id;
      return WritePageGuard(bf, keep_alive);
   }

   template<typename... Args>
   void init(Args &&... args)
   {
      new(ParentClass::bf->page.dt) T(std::forward<Args>(args)...);
   }
   // -------------------------------------------------------------------------------------
   void keepAlive()
   {
      keep_alive = true;
   }
   // -------------------------------------------------------------------------------------
   void reclaim()
   {
      BMC::global_bf->reclaimPage(*ParentClass::bf);
      ParentClass::moved = true;
   }
   // -------------------------------------------------------------------------------------
   ~WritePageGuard()
   {
      if ( !ParentClass::moved ) {
         assert((ParentClass::bf_s_lock.local_version & WRITE_LOCK_BIT) == WRITE_LOCK_BIT);
         if ( ParentClass::hasBf()) {
            ParentClass::bf->page.LSN++; // TODO: LSN
         }
         ParentClass::bf_s_lock.local_version = WRITE_LOCK_BIT + ParentClass::bf_s_lock.version_ptr->fetch_add(WRITE_LOCK_BIT);
         ParentClass::moved = true;
         // -------------------------------------------------------------------------------------
         if ( !keep_alive ) {
            reclaim();
         }
      }
   }
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
