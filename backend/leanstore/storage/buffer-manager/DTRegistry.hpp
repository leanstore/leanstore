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
  OptimisticGuard parent_guard;
  BufferFrame* parent_bf;
  s32 pos = -2; // meaning it is the root bf in the dt
  // -------------------------------------------------------------------------------------
  template <typename T>
  OptimisticPageGuard<T> getParentReadPageGuard()
  {
    return OptimisticPageGuard<T>::manuallyAssembleGuard(std::move(parent_guard), parent_bf);
  }
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
  struct DTMeta {
    std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
    std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
    std::function<bool(void*, BufferFrame&, OptimisticGuard&, ParentSwipHandler&)> check_space_utilization;
    u64 instances_counter = 0;
  };
  // Not syncrhonized
  std::unordered_map<DTType, DTMeta> dt_types_ht;
  std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
  // -------------------------------------------------------------------------------------
  void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
  ParentSwipHandler findParent(DTID dtid, BufferFrame&);
  bool checkSpaceUtilization(DTID dtid, BufferFrame &, OptimisticGuard&, ParentSwipHandler&);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
