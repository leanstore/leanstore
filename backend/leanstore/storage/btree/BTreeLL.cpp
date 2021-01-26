#include "BTreeLL.hpp"

#include "core/BTreeGenericIterator.hpp"
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
OP_RESULT BTreeLL::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   volatile u32 mask = 1;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         DEBUG_BLOCK()
         {
            s16 sanity_check_result = leaf->sanityCheck(key, key_length);
            leaf.recheck_done();
            if (sanity_check_result != 0) {
               cout << leaf->count << endl;
            }
            ensure(sanity_check_result == 0);
         }
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck_done();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck_done();
            raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch()
      {
         BACKOFF_STRATEGIES()
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(Slice(start_key, key_length));
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.next() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(Slice(start_key, key_length));
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      while (true) {
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            jumpmu_return OP_RESULT::OK;
         } else {
            if (iterator.prev() != OP_RESULT::OK) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.insertKV(Slice(key, key_length), Slice(value, value_length));
      ensure(ret == OP_RESULT::OK);
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(key_length + value_length);
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = key_length;
         wal_entry->value_length = value_length;
         std::memcpy(wal_entry->payload, key, key_length);
         std::memcpy(wal_entry->payload + key_length, value, value_length);
         wal_entry.submit();
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSize(u8* key,
                                  u16 key_length,
                                  function<void(u8* payload, u16 payload_size)> callback,
                                  WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(Slice(key, key_length));
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      if (FLAGS_wal) {
         // if it is a secondary index, then we can not use updateSameSize
         assert(wal_update_generator.entry_size > 0);
         // -------------------------------------------------------------------------------------
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key_length + wal_update_generator.entry_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = key_length;
         std::memcpy(wal_entry->payload, key, key_length);
         wal_update_generator.before(iterator.valuePtr(), wal_entry->payload + key_length);
         // The actual update by the client
         callback(iterator.valuePtr(), iterator.valueLength());
         wal_update_generator.after(iterator.valuePtr(), wal_entry->payload + key_length);
         wal_entry.submit();
      } else {
         callback(iterator.valuePtr(), iterator.valueLength());
      }
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* key_ptr, u16 key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(key_ptr, key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      Slice value = iterator.value();
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(key_length);
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = key_length;
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + key_length, value.data(), value.length());
         wal_entry.submit();
      }
      ret = iterator.removeCurrent();
      ensure(ret == OP_RESULT::OK);
      iterator.mergeIfNeeded();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countEntries()
{
   return BTreeGeneric::countEntries();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::countPages()
{
   return BTreeGeneric::countPages();
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::getHeight()
{
   return BTreeGeneric::getHeight();
}
// -------------------------------------------------------------------------------------
void BTreeLL::undo(void*, const u8*, const u64)
{
   // TODO: undo for storage
}
// -------------------------------------------------------------------------------------
void BTreeLL::todo(void*, const u8*, const u64) {}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeLL::getMeta()
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
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParent(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
