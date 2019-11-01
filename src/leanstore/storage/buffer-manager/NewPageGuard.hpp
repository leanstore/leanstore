#pragma once
#include "BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
template<typename T>
class NewPageGuard {
public:
   T *object;
   BufferFrame *bf;
   template<typename... Args>
   NewPageGuard(Args &&... args)
   {
      bf = &BMC::global_bf->allocatePage();
      object = new(bf->page.dt) T(std::forward<Args>(args)...);
   }
   ~NewPageGuard()
   {
      assert(bf->header.lock & 2);
      bf->header.lock.fetch_add(2);
   }
   T *operator->()
   {
      return reinterpret_cast<T *>(bf->page.dt);
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------