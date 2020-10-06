#pragma once
#include "BufferFrame.hpp"
#include "DTTypes.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <tuple>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
  Swip<BufferFrame>& swip;
  Guard parent_guard;
  BufferFrame* parent_bf;
  s32 pos = -2;  // meaning it is the root bf in the dt
  // -------------------------------------------------------------------------------------
  template <typename T>
  HybridPageGuard<T> getParentReadPageGuard()
  {
    return HybridPageGuard<T>(parent_guard, parent_bf);
  }
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
  struct DTMeta {
    std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
    std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
    std::function<bool(void*, BufferFrame&, OptimisticGuard&, ParentSwipHandler&)> check_space_utilization;
    std::function<void(void* btree_object, BufferFrame &bf, u8* dest)> checkpoint;
    u64 instances_counter = 0;
  };
  // TODO: Not syncrhonized
  std::unordered_map<DTType, DTMeta> dt_types_ht;
  std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
  // -------------------------------------------------------------------------------------
  void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
  ParentSwipHandler findParent(DTID dtid, BufferFrame&);
  bool checkSpaceUtilization(DTID dtid, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
  // Pre: bf is shared/exclusive latched
  void checkpoint(DTID dt_id, BufferFrame& bf, u8*);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
