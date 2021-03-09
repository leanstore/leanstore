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
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK) {
         iterator.assembleKey();
         auto key = iterator.key();
         auto value = iterator.value();
         if (!callback(key.data(), key.length(), value.data(), value.length())) {
            break;
         }
         ret = iterator.next();
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { return OP_RESULT::OTHER; }
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
         iterator.assembleKey();
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
   jumpmuCatch() { return OP_RESULT::OTHER; }
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
      OP_RESULT ret = iterator.insertKV(key, value);
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
   jumpmuCatch() { return OP_RESULT::OTHER; }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* payload, u16 payload_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
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
         assert(update_descriptor.count > 0);  // if it is a secondary index, then we can not use updateSameSize
         // -------------------------------------------------------------------------------------
         const u16 delta_length = update_descriptor.size() + calculateDeltaSize(update_descriptor);
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key.length() + delta_length);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = key.length();
         wal_entry->delta_length = delta_length;
         u8* wal_ptr = wal_entry->payload;
         std::memcpy(wal_ptr, key.data(), key.length());
         wal_ptr += key.length();
         std::memcpy(wal_ptr, &update_descriptor, update_descriptor.size());
         wal_ptr += update_descriptor.size();
         copyDiffTo(update_descriptor, wal_ptr, current_value.data());
         // The actual update by the client
         callback(current_value.data(), current_value.length());
         XORDiffTo(update_descriptor, wal_ptr, current_value.data());
         wal_entry.submit();
      } else {
         callback(current_value.data(), current_value.length());
         iterator.leaf.incrementGSN();
      }
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { return OP_RESULT::OTHER; }
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
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length + value.length());
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = o_key_length;
         wal_entry->value_length = value.length();
         std::memcpy(wal_entry->payload, key.data(), key.length());
         std::memcpy(wal_entry->payload + o_key_length, value.data(), value.length());
         wal_entry.submit();
      } else {
         iterator.leaf.incrementGSN();
      }
      ret = iterator.removeCurrent();
      ensure(ret == OP_RESULT::OK);
      iterator.mergeIfNeeded();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
   jumpmu_return OP_RESULT::OTHER;
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
                                    .todo = todo,
                                    .serialize = serialize,
                                    .deserialize = deserialize};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParent(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
}
// -------------------------------------------------------------------------------------
void BTreeLL::checkpoint(void* btree_object, BufferFrame& bf, u8* dest)
{
   return BTreeGeneric::checkpoint(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), bf, dest);
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BTreeLL::serialize(void* btree_object)
{
   return BTreeGeneric::serialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)));
}
// -------------------------------------------------------------------------------------
void BTreeLL::deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized)
{
   BTreeGeneric::deserialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), serialized);
}
// -------------------------------------------------------------------------------------
u64 BTreeLL::calculateDeltaSize(const UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   u64 total_size = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      total_size += update_descriptor.slots[a_i].size;
   }
   return total_size;
}
// -------------------------------------------------------------------------------------
void BTreeLL::copyDiffTo(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 dst_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      std::memcpy(dst + dst_offset, src + slot.offset, slot.size);
      dst_offset += slot.size;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::copyDiffFrom(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 src_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      std::memcpy(dst + slot.offset, src + src_offset, slot.size);
      src_offset += slot.size;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::XORDiffTo(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 dst_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      for (u64 b_i = 0; b_i < slot.size; b_i++) {
         *(dst + dst_offset + b_i) ^= *(src + slot.offset + b_i);
      }
      dst_offset += slot.size;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::XORDiffFrom(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 src_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      for (u64 b_i = 0; b_i < slot.size; b_i++) {
         *(dst + slot.offset + b_i) ^= *(src + src_offset + b_i);
      }
      src_offset += slot.size;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
