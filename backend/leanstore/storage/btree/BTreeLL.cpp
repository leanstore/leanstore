#include "BTreeLL.hpp"

#include "core/BTreeGenericIterator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "leanstore/storage/btree/core/BTreeInterface.hpp"
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
            s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
            leaf.recheck();
            if (sanity_check_result != 0) {
               cout << leaf->count << endl;
            }
            ensure(sanity_check_result == 0);
         }
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
            leaf.recheck();
            jumpmu_return OP_RESULT::OK;
         } else {
            leaf.recheck();
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
   return OP_RESULT::UNREACHABLE;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(key);
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
   return OP_RESULT::UNREACHABLE;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   Slice key(start_key, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekForPrev(key);
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
   return OP_RESULT::UNREACHABLE;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   Slice value(o_value, o_value_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.insertKV(key, value);
      ensure(ret == OP_RESULT::OK);
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(key.length() + value.length());
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = key.length();
         wal_entry->value_length = value.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + key.length(), value.data(), value.length());
         wal_entry.submit();
      } else {
        iterator.leaf.incrementGSN();
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
   return OP_RESULT::UNREACHABLE;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSize(u8* o_key,
                                  u16 o_key_length,
                                  function<void(u8* payload, u16 payload_size)> callback,
                                  WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto current_value = iterator.mutableValue();
      if (FLAGS_wal) {
         // if it is a secondary index, then we can not use updateSameSize
         assert(wal_update_generator.entry_size > 0);
         // -------------------------------------------------------------------------------------
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key.length() + wal_update_generator.entry_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = key.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         wal_update_generator.before(current_value.data(), wal_entry->payload + key.length());
         // The actual update by the client
         callback(current_value.data(), current_value.length());
         wal_update_generator.after(current_value.data(), wal_entry->payload + key.length());
         wal_entry.submit();
      } else {
         callback(current_value.data(), current_value.length());
         iterator.leaf.incrementGSN();
      }
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
   return OP_RESULT::UNREACHABLE;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* o_key, u16 o_key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      Slice value = iterator.value();
      if (FLAGS_wal) {
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length);
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = o_key_length;
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + o_key_length, value.data(), value.length());
         wal_entry.submit();
      } else {
         iterator.leaf.incrementGSN();
      }
      ret = iterator.removeCurrent();
      ensure(ret == OP_RESULT::OK);
      //iterator.mergeIfNeeded();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
   return OP_RESULT::UNREACHABLE;
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
                                    .find_parent_no_jump = findParentNoJump,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo,
                                    .todo = todo};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
