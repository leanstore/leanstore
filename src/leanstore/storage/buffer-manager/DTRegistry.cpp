#include "DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
void DTRegistry::iterateChildrenSwips(DTID dtid, leanstore::BufferFrame &bf, leanstore::SharedGuard &guard, std::function<bool(Swip<BufferFrame> &)> callback)
{
   auto dt_meta = dt_meta_ht[dtid];
   callbacks_ht[std::get<0>(dt_meta)].iterate_childern(std::get<1>(dt_meta), bf, guard, callback);
}
// -------------------------------------------------------------------------------------
ParentSwipHandler DTRegistry::findParent(DTID dtid, leanstore::BufferFrame &bf, leanstore::SharedGuard &guard)
{
   auto dt_meta = dt_meta_ht[dtid];
   return callbacks_ht[std::get<0>(dt_meta)].find_parent(std::get<1>(dt_meta), bf, guard);
}
// -------------------------------------------------------------------------------------
}

