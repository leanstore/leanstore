#pragma once
#include "Units.hpp"
#include "BufferFrame.hpp"
#include "DTTypes.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <unordered_map>
#include <tuple>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame> &swip;
   SharedGuard guard;
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct CallbackFunctions {
      std::function<void(void *, BufferFrame &, SharedGuard &, std::function<bool(Swip<BufferFrame> &)>)> iterate_childern;
      std::function<ParentSwipHandler(void *, BufferFrame &, SharedGuard &)> find_parent;
   };
   std::unordered_map<DTType, CallbackFunctions> callbacks_ht;
   std::unordered_map<u64, std::tuple<DTType, void *>> dt_meta_ht;
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame &, SharedGuard &, std::function<bool(Swip<BufferFrame> &)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame &, SharedGuard &);
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
