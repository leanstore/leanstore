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
      jumpmuCatch() { WorkerCounters::myCounters().dt_restarts_read[dt_id]++; }
   }
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
bool BTreeLL::isRangeSurelyEmpty(Slice start_key, Slice end_key)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, start_key.data(), start_key.length());
         // -------------------------------------------------------------------------------------
         Slice upper_fence(leaf->getUpperFenceKey(), leaf->upper_fence.length);
         Slice lower_fence(leaf->getLowerFenceKey(), leaf->lower_fence.length);
         assert(start_key >= lower_fence);
         if ((leaf->upper_fence.offset == 0 || end_key <= upper_fence) && leaf->count == 0) {
            s32 pos = leaf->lowerBound<false>(start_key.data(), start_key.length());
            if (pos == leaf->count) {
               leaf.recheck();
               jumpmu_return true;
            } else {
               leaf.recheck();
               jumpmu_return false;
            }
         } else {
            leaf.recheck();
            jumpmu_return false;
         }
      }
      jumpmuCatch() {}
   }
   UNREACHABLE();
   return false;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_scan_asc[dt_id]++; }
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
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_scan_desc[dt_id]++; }
   const Slice key(start_key, key_length);
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
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   if (config.enable_wal) {
      cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   const Slice key(o_key, o_key_length);
   const Slice value(o_value, o_value_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      OP_RESULT ret = iterator.insertKV(key, value);
      ensure(ret == OP_RESULT::OK);
      if (config.enable_wal) {
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
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::seekForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         bool is_equal = false;
         s16 cur = leaf->lowerBound<false>(key, key_length, &is_equal);
         if (is_equal == false) {
            if (cur == 0) {
               TODOException();
            } else {
               cur -= 1;
            }
         }
         payload_callback(leaf->getPayload(cur), leaf->getPayloadLength(cur));
         leaf.recheck();
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { WorkerCounters::myCounters().dt_restarts_read[dt_id]++; }
   }
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::append(std::function<void(u8*)> o_key,
                          u16 o_key_length,
                          std::function<void(u8*)> o_value,
                          u16 o_value_length,
                          std::unique_ptr<u8[]>& session_ptr)
{
   struct alignas(64) Session {
      BufferFrame* bf;
   };
   // -------------------------------------------------------------------------------------
   if (session_ptr.get()) {
      auto session = reinterpret_cast<Session*>(session_ptr.get());
      jumpmuTry()
      {
         Guard opt_guard(session->bf->header.latch);
         opt_guard.toOptimisticOrJump();
         {
            BTreeNode& node = *reinterpret_cast<BTreeNode*>(session->bf->page.dt);
            if (session->bf->page.dt_id != dt_id || !node.is_leaf || node.upper_fence.length != 0 || node.upper_fence.offset != 0) {
               jumpmu::jump();
            }
         }
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this), session->bf, opt_guard.version);
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.enoughSpaceInCurrentNode(o_key_length, o_value_length);
         if (ret == OP_RESULT::OK) {  // iterator.keyInCurrentBoundaries(key)
            u8 key_buffer[o_key_length];
            o_key(key_buffer);
            const s32 pos = iterator.leaf->count;
            iterator.leaf->insertDoNotCopyPayload(key_buffer, o_key_length, o_value_length, pos);
            iterator.cur = pos;
            o_value(iterator.mutableValue().data());
            iterator.markAsDirty();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_append_opt[dt_id]++; }
            jumpmu_return OP_RESULT::OK;
         }
      }
      jumpmuCatch() {}
   }
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         u8 key_buffer[o_key_length];
         for (u64 i = 0; i < o_key_length; i++) {
            key_buffer[i] = 255;
         }
         const Slice key(key_buffer, o_key_length);
         OP_RESULT ret = iterator.seekToInsert(key);
         explainWhen(ret == OP_RESULT::DUPLICATE);
         ret = iterator.enoughSpaceInCurrentNode(key, o_value_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         o_key(key_buffer);
         iterator.insertInCurrentNode(key, o_value_length);
         o_value(iterator.mutableValue().data());
         iterator.markAsDirty();
         // -------------------------------------------------------------------------------------
         Session* session = nullptr;
         if (session_ptr) {
            session = reinterpret_cast<Session*>(session_ptr.get());
         } else {
            session_ptr = std::make_unique<u8[]>(sizeof(Session));
            session = new (session_ptr.get()) Session();
         }
         session->bf = iterator.leaf.bf;
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_append[dt_id]++; }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* payload, u16 payload_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   if (config.enable_wal) {
      cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      auto current_value = iterator.mutableValue();
      if (config.enable_wal) {
         assert(update_descriptor.count > 0);  // if it is a secondary index, then we can not use updateSameSize
         // -------------------------------------------------------------------------------------
         const u16 delta_length = update_descriptor.size() + update_descriptor.diffLength();
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key.length() + delta_length);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = key.length();
         wal_entry->delta_length = delta_length;
         u8* wal_ptr = wal_entry->payload;
         std::memcpy(wal_ptr, key.data(), key.length());
         wal_ptr += key.length();
         std::memcpy(wal_ptr, &update_descriptor, update_descriptor.size());
         wal_ptr += update_descriptor.size();
         generateDiff(update_descriptor, wal_ptr, current_value.data());
         // The actual update by the client
         callback(current_value.data(), current_value.length());
         generateXORDiff(update_descriptor, wal_ptr, current_value.data());
         wal_entry.submit();
      } else {
         callback(current_value.data(), current_value.length());
         iterator.leaf.incrementGSN();
      }
      iterator.contentionSplit();
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::remove(u8* o_key, u16 o_key_length)
{
   if (config.enable_wal) {
      cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   const Slice key(o_key, o_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return ret;
      }
      Slice value = iterator.value();
      if (config.enable_wal) {
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
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeLL::rangeRemove(u8* start_key, u16 start_key_length, u8* end_key, u16 end_key_length)
{
   const Slice s_key(start_key, start_key_length);
   const Slice e_key(end_key, end_key_length);
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
         if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            iterator.cleanUpCallback([&, to_find = leaf.bf] {
               jumpmuTry() { this->tryMerge(*to_find); }
               jumpmuCatch() {}
            });
         }
      });
      // -------------------------------------------------------------------------------------
      if (FLAGS_tmp6) {
         auto ret = iterator.seek(s_key);
         if (ret != OP_RESULT::OK) {
            jumpmu_return ret;
         }
         while (true) {
            iterator.assembleKey();
            auto c_key = iterator.key();
            if (c_key >= s_key && c_key <= e_key) {
               COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_range_removed[dt_id]++; }
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               if (iterator.cur == iterator.leaf->count) {
                  ret = iterator.next();
               }
            } else {
               break;
            }
         }
         jumpmu_return OP_RESULT::OK;
         // TODO: WAL, atm used by none persistence trees
      } else {
         bool did_purge_full_page = false;
         iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
            if (leaf->count == 0) {
               return;
            }
            u8 first_key[leaf->getFullKeyLen(0)];
            leaf->copyFullKey(0, first_key);
            Slice p_s_key(first_key, leaf->getFullKeyLen(0));
            // -------------------------------------------------------------------------------------
            u8 last_key[leaf->getFullKeyLen(leaf->count - 1)];
            leaf->copyFullKey(leaf->count - 1, last_key);
            Slice p_e_key(last_key, leaf->getFullKeyLen(leaf->count - 1));
            if (p_s_key >= s_key && p_e_key <= e_key) {
               // Purge the whole page
               COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_range_removed[dt_id] += leaf->count; }
               leaf->reset();
               did_purge_full_page = true;
            }
         });
         // -------------------------------------------------------------------------------------
         while (true) {
            iterator.seek(s_key);
            if (did_purge_full_page) {
               did_purge_full_page = false;
               continue;
            } else {
               break;
            }
         }
      }
   }
   jumpmuCatch() {}
   return OP_RESULT::OK;
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
   TODOException();
}
// -------------------------------------------------------------------------------------
void BTreeLL::todo(void*, const u8*, const u64, const u64, const bool)
{
   UNREACHABLE();
}
// -------------------------------------------------------------------------------------
void BTreeLL::unlock(void*, const u8*)
{
   UNREACHABLE();
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeLL::getMeta()
{
   DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
                                    .find_parent = findParent,
                                    .check_space_utilization = checkSpaceUtilization,
                                    .checkpoint = checkpoint,
                                    .undo = undo,
                                    .todo = todo,
                                    .unlock = unlock,
                                    .serialize = serialize,
                                    .deserialize = deserialize};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
SpaceCheckResult BTreeLL::checkSpaceUtilization(void* btree_object, BufferFrame& bf)
{
   auto& btree = *reinterpret_cast<BTreeLL*>(btree_object);
   return BTreeGeneric::checkSpaceUtilization(static_cast<BTreeGeneric*>(&btree), bf);
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTreeLL::findParent(void* btree_object, BufferFrame& to_find)
{
   return BTreeGeneric::findParentJump(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
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
void BTreeLL::generateDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 dst_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      std::memcpy(dst + dst_offset, src + slot.offset, slot.length);
      dst_offset += slot.length;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::applyDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 src_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      std::memcpy(dst + slot.offset, src + src_offset, slot.length);
      src_offset += slot.length;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::generateXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 dst_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      for (u64 b_i = 0; b_i < slot.length; b_i++) {
         *(dst + dst_offset + b_i) ^= *(src + slot.offset + b_i);
      }
      dst_offset += slot.length;
   }
}
// -------------------------------------------------------------------------------------
void BTreeLL::applyXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
{
   u64 src_offset = 0;
   for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
      const auto& slot = update_descriptor.slots[a_i];
      for (u64 b_i = 0; b_i < slot.length; b_i++) {
         *(dst + slot.offset + b_i) ^= *(src + src_offset + b_i);
      }
      src_offset += slot.length;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
