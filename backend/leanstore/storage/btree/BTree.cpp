
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
OP_RESULT BTree::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   if (FLAGS_vw) {
      return lookupVW(key, key_length, payload_callback);
   } else if (FLAGS_vi) {
      return lookupVI(key, key_length, payload_callback);
   } else {
      const bool ret = lookupOneLL(key, key_length, payload_callback);
      if (ret) {
         return OP_RESULT::OK;
      } else {
         return OP_RESULT::NOT_FOUND;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::insert(u8* key, u16 key_length, u64 value_length, u8* value)
{
   if (FLAGS_vw) {
      return insertVW(key, key_length, value_length, value);
   } else if (FLAGS_vi) {
      return insertVI(key, key_length, value_length, value);
   } else {
      insertLL(key, key_length, value_length, value);
      return OP_RESULT::OK;
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator wal_generator)
{
   if (FLAGS_vw) {
      return updateVW(key, key_length, callback, wal_generator);
   } else if (FLAGS_vi) {
      return updateVI(key, key_length, callback, wal_generator);
   } else {
      updateSameSizeLL(key, key_length, callback, wal_generator);
      return OP_RESULT::OK;
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::remove(u8* key, u16 key_length)
{
   if (FLAGS_vw) {
      return removeVW(key, key_length);
   } else if (FLAGS_vi) {
      return removeVI(key, key_length);
   } else {
      removeLL(key, key_length);
      return OP_RESULT::OK;
   }
}
// -------------------------------------------------------------------------------------
void BTree::scanAsc(u8* start_key,
                    u16 key_length,
                    function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                    function<void()> undo)
{
   if (FLAGS_vw) {
      scanAscVW(start_key, key_length, callback, undo);
   } else if (FLAGS_vi) {
      ensure(false);
   } else {
      scanAscLL(start_key, key_length, callback, undo);
   }
}
// -------------------------------------------------------------------------------------
void BTree::scanDesc(u8* start_key,
                     u16 key_length,
                     function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                     function<void()> undo)
{
   if (FLAGS_vw) {
      scanDescVW(start_key, key_length, callback, undo);
   } else if (FLAGS_vi) {
      ensure(false);
   } else {
      scanDescLL(start_key, key_length, callback, undo);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
