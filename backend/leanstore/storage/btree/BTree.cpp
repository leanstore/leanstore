
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
                                    .undo = undo,
                                    .todo = todo};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   OP_RESULT res;
   if (FLAGS_vw) {
      res = lookupVW(key, key_length, payload_callback);
   } else if (FLAGS_vi) {
      res = lookupVI(key, key_length, payload_callback);
   } else {
      const bool ret = lookupOneLL(key, key_length, payload_callback);
      if (ret) {
         res = OP_RESULT::OK;
      } else {
         res = OP_RESULT::NOT_FOUND;
      }
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::insert(u8* key, u16 key_length, u64 value_length, u8* value)
{
   OP_RESULT res;
   if (FLAGS_vw) {
      res = insertVW(key, key_length, value_length, value);
   } else if (FLAGS_vi) {
      res = insertVI(key, key_length, value_length, value);
   } else {
      insertLL(key, key_length, value_length, value);
      res = OP_RESULT::OK;
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator wal_generator)
{
   OP_RESULT res;
   if (FLAGS_vw) {
      res = updateVW(key, key_length, callback, wal_generator);
   } else if (FLAGS_vi) {
      res = updateVI(key, key_length, callback, wal_generator);
   } else {
      updateSameSizeLL(key, key_length, callback, wal_generator);
      res = OP_RESULT::OK;
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::remove(u8* key, u16 key_length)
{
   OP_RESULT res;
   if (FLAGS_vw) {
      res = removeVW(key, key_length);
   } else if (FLAGS_vi) {
      res = removeVI(key, key_length);
   } else {
      removeLL(key, key_length);
      res = OP_RESULT::OK;
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::scanAsc(u8* start_key,
                         u16 key_length,
                         function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                         function<void()> undo)
{
   OP_RESULT res = OP_RESULT::OK;
   if (FLAGS_vw) {
      res = scanAscVW(start_key, key_length, callback, undo);
   } else if (FLAGS_vi) {
      ensure(false);
   } else {
      scanAscLL(start_key, key_length, callback, undo);
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::scanDesc(u8* start_key,
                          u16 key_length,
                          function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                          function<void()> undo)
{
   OP_RESULT res = OP_RESULT::OK;
   if (FLAGS_vw) {
      res = scanDescVW(start_key, key_length, callback, undo);
   } else if (FLAGS_vi) {
      ensure(false);
   } else {
      scanDescLL(start_key, key_length, callback, undo);
   }
   if (res == OP_RESULT::ABORT_TX) {
      cr::Worker::my().abortTX();
   }
   return res;
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
