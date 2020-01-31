#include "DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
void DTRegistry::iterateChildrenSwips(DTID dtid, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
  auto dt_meta = dt_instances_ht[dtid];
  dt_types_ht[std::get<0>(dt_meta)].iterate_children(std::get<1>(dt_meta), bf, callback);
}
// -------------------------------------------------------------------------------------
ParentSwipHandler DTRegistry::findParent(DTID dtid, BufferFrame& bf)
{
  auto dt_meta = dt_instances_ht[dtid];
  return dt_types_ht[std::get<0>(dt_meta)].find_parent(std::get<1>(dt_meta), bf);
}
// -------------------------------------------------------------------------------------
bool DTRegistry::checkSpaceUtilization(DTID dtid, BufferFrame& bf, OptimisticGuard& guard, ParentSwipHandler& parent_handler)
{
  auto dt_meta = dt_instances_ht[dtid];
  return dt_types_ht[std::get<0>(dt_meta)].check_space_utilization(std::get<1>(dt_meta), bf, guard, parent_handler);
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
