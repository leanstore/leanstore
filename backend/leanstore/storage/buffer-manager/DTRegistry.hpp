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
  OptimisticGuard guard;
  BufferFrame* parent;
  // -------------------------------------------------------------------------------------
  template <typename T>
  OptimisticPageGuard<T> getParentReadPageGuard()
  {
    return OptimisticPageGuard<T>::manuallyAssembleGuard(guard, parent);
  }
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
  struct DTMeta {
    std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
    // the caller must have locked the current buffer frame exclusively before
    // calling
    std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
    u64 instances_counter = 0;
  };
  // Not syncrhonized
  std::unordered_map<DTType, DTMeta> dt_types_ht;
  std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
  // -------------------------------------------------------------------------------------
  void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
  ParentSwipHandler findParent(DTID dtid, BufferFrame&);
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
