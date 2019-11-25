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
namespace buffermanager {
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame> &swip;
   ReadGuard guard;
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct DTMeta {
      std::function<void(void *, BufferFrame &, std::function<bool(Swip<BufferFrame> &)>)> iterate_childern;
      // the caller must have called the current buffer frame exclusively before calling
      std::function<ParentSwipHandler(void *, BufferFrame &)> find_parent;
      u64 instances_counter = 0;
   };
   // Not syncrhonized
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void *>> dt_instances_ht;
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame &, std::function<bool(Swip<BufferFrame> &)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame &);
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
