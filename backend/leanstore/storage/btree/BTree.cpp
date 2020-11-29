
#include "BTree.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
// bool BTree::lookupOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
// {
//    assert(!(FLAGS_vw && FLAGS_vi));
//    if (FLAGS_vi) {
//      return
//    } else if(FLAGS_vw) {

//    }
// }
// void BTree::scanAsc(u8* start_key, u16 key_length, function<bool(u8* key, u8* value, u16 value_length)>, function<void()>) {}
// void BTree::scanDesc(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)>, function<void()>) {}
// void BTree::insert(u8* key, u16 key_length, u64 valueLength, u8* value) {}
// void BTree::updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0}) {}
// bool BTree::remove(u8* key, u16 key_length) {}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
