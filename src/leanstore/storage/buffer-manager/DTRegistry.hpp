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
   ReadGuard guard;
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct DTMeta {
      std::function<void(void *, BufferFrame &, ReadGuard &, std::function<bool(Swip<BufferFrame> &)>)> iterate_childern;
      std::function<ParentSwipHandler(void *, BufferFrame &, ReadGuard &)> find_parent;
      u64 instances_counter = 0;
   };
   // Not syncrhonized
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void *>> dt_instances_ht;
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame &, ReadGuard &, std::function<bool(Swip<BufferFrame> &)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame &, ReadGuard &);
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
