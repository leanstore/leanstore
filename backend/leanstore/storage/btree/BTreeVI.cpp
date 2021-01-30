#include "BTreeVI.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
// -------------------------------------------------------------------------------------
// Assumptions made in this implementation:
// 1) We don't insert an already removed key
// 2) Secondary Versions contain delta
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::lookup(u8* o_key, u16 o_key_length, function<void(const u8*, u16)> payload_callback)
{
   u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   Slice key(key_buffer, key_length);
   StringU value;
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      Slice key = iterator.key();
      Slice payload = iterator.value();
      SN sn = swap(*reinterpret_cast<const SN*>(key.data() + key.length() - sizeof(SN)));
      while (true) {
         if (sn == 0) {
            const auto& version_meta = *reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
            if (isVisibleForMe(version_meta.worker_id, version_meta.tts)) {
               payload_callback(payload.data(), payload.length() - sizeof(PrimaryVersion));
               jumpmu_return OP_RESULT::OK;
            } else {
               value = StringU(payload.data(), payload.length() - sizeof(PrimaryVersion));
            }
         } else {
            const auto& version_meta = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
            const u16 delta_length = payload.length() - sizeof(SecondaryVersion);
            applyDelta(value.data(), payload.data(), delta_length);
            if (isVisibleForMe(version_meta.worker_id, version_meta.tts)) {
               payload_callback(value.data(), value.length());
               jumpmu_return OP_RESULT::OK;
            }
         }
         ret = iterator.next();
         if (ret == OP_RESULT::OK) {
            key = iterator.key();
            payload = iterator.value();
            sn = swap(*reinterpret_cast<const SN*>(key.data() + key.length() - sizeof(SN)));
            if (sn == 0) {
               jumpmu_return OP_RESULT::NOT_FOUND;
            }
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::updateSameSize(u8* o_key,
                                  u16 o_key_length,
                                  function<void(u8* value, u16 value_size)> callback,
                                  WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, key_length);
   SN& sn = *reinterpret_cast<SN*>(key_buffer + key_length - sizeof(SN));
   sn = 0;
   Slice key(key_buffer, key_length);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         auto ret = iterator.seekExact(key);
         if (ret != OP_RESULT::OK) {
            jumpmu_return ret;
         }
         auto primary_payload = iterator.mutableValue();
         auto& version_meta = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (!isVisibleForMe(version_meta.worker_id, version_meta.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         const u16 secondary_payload_length = wal_update_generator.entry_size + sizeof(SecondaryVersion);
         if (iterator.canInsertInCurrentNode(key, secondary_payload_length) == OP_RESULT::NOT_FOUND) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         sn = version_meta.next_version--;
         u8 secondary_payload[secondary_payload_length];
         new (secondary_payload) SecondaryVersion(myWorkerID(), myTTS(), false, true);
         wal_update_generator.before(primary_payload.data(), secondary_payload);
         iterator.insertInCurrentNode(key, Slice(secondary_payload, secondary_payload_length));
         callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
         // TODO: WAL
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::insert(u8* o_key, u16 o_key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, key_length);
   SN& sn = *reinterpret_cast<SN*>(key_buffer + key_length - sizeof(SN));
   sn = 0;
   Slice key(key_buffer, key_length);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         iterator.seekToInsert(key);
         if (iterator.isKeyEqualTo(key)) {
            ensure(false);  // not implemented
         }
         auto ret = iterator.canInsertInCurrentNode(key, value_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         iterator.insertInCurrentNode(key, value_length);
         auto payload = iterator.mutableValue();
         std::memcpy(payload.data(), value, value_length);
         new (payload.data() + payload.length() - sizeof(PrimaryVersion)) PrimaryVersion(myWorkerID(), myTTS());
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::remove(u8* k_wo_version, u16 kl_wo_version)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key[kl_wo_version + 8];
   u16 key_length = kl_wo_version + 8;
   std::memcpy(key, k_wo_version, kl_wo_version);
   *reinterpret_cast<u64*>(key + kl_wo_version) = 0;
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         auto leaf_ex = ExclusivePageGuard(std::move(leaf));
         const s32 primary_pos = leaf->lowerBound<true>(key, key_length);
         if (primary_pos != -1) {  // TODO:
            ensure(false);
         } else {
            const u16 value_length = +leaf_ex->getPayloadLength(primary_pos) - sizeof(PrimaryVersion);
            auto& primary_meta = *reinterpret_cast<PrimaryVersion*>(leaf_ex->getPayload(primary_pos) + value_length);
            ensure(isVisibleForMe(primary_meta.worker_id, primary_meta.tts));  // TODO:
            u8* primary_payload = leaf_ex->getPayload(primary_pos);
            const u16 secondary_payload_length = sizeof(SecondaryVersion) + value_length;
            *reinterpret_cast<u64*>(key + kl_wo_version) = primary_meta.next_version--;
            if (leaf_ex->prepareInsert(key, key_length, secondary_payload_length)) {
               s16 secondary_pos = leaf_ex->insertDoNotCopyPayload(key, key_length, secondary_payload_length);
               u8* secondary_payload = leaf_ex->getPayload(secondary_pos);
               std::memcpy(secondary_payload, leaf_ex->getPayload(primary_pos), value_length);
               new (secondary_payload + value_length) SecondaryVersion(myWorkerID(), myTTS(), false, false);
               // -------------------------------------------------------------------------------------
               std::memcpy(primary_payload, primary_payload + value_length, sizeof(PrimaryVersion));
               leaf_ex->setPayloadLength(primary_pos, sizeof(PrimaryVersion));
               jumpmu_return OP_RESULT::OK;
            } else {
               // -------------------------------------------------------------------------------------
               // Release lock
               leaf = std::move(leaf_ex);
               leaf.unlock();
               // -------------------------------------------------------------------------------------
               trySplit(*leaf.bf);
               // -------------------------------------------------------------------------------------
               jumpmu_continue;
            }
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts)
{
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   const WALEntry& entry = *reinterpret_cast<const WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case WAL_LOG_TYPE::WALInsert: {  // Assuming on insert after remove
         auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
         u16 key_length = insert_entry.key_length + 8;
         u8 key[key_length];
         std::memcpy(key, insert_entry.payload, insert_entry.key_length);
         *reinterpret_cast<u64*>(key + insert_entry.key_length) = 0;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
               auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
               const bool ret = c_x_guard->remove(key, key_length);
               ensure(ret);
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      case WAL_LOG_TYPE::WALUpdate: {
         const auto& update_entry = *reinterpret_cast<const WALUpdate*>(&entry);
         u16 key_length = update_entry.key_length + 8;
         u8 key[key_length];
         std::memcpy(key, update_entry.payload, update_entry.key_length);
         auto& sn = *reinterpret_cast<u64*>(key + update_entry.key_length);
         sn = update_entry.before_image_seq;
         // -------------------------------------------------------------------------------------
         u8 materialized_delta[PAGE_SIZE];
         u16 delta_length;
         SecondaryVersion secondary_version(0, 0, 0, 0);
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
               auto secondary_x_guard = ExclusivePageGuard(std::move(c_guard));
               const s16 secondary_pos = secondary_x_guard->lowerBound<true>(key, key_length);
               ensure(secondary_pos >= 0);
               u8* secondary_payload = secondary_x_guard->getPayload(secondary_pos);
               const u16 secondary_payload_length = secondary_x_guard->getPayloadLength(secondary_pos);
               secondary_version =
                   *reinterpret_cast<const SecondaryVersion*>(secondary_payload + secondary_payload_length - sizeof(SecondaryVersion));
               const bool is_delta = secondary_version.is_delta;
               ensure(is_delta);
               delta_length = secondary_payload_length - sizeof(SecondaryVersion);
               std::memcpy(materialized_delta, secondary_payload, delta_length);
               // -------------------------------------------------------------------------------------
               secondary_x_guard->removeSlot(secondary_pos);
               c_guard = std::move(secondary_x_guard);
            }
            jumpmuCatch() {}
         }
         while (true) {
            jumpmuTry()
            {
               // Go to primary version
               HybridPageGuard<BTreeNode> c_guard;
               sn = 0;
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
               auto primary_x_guard = ExclusivePageGuard(std::move(c_guard));
               const s16 primary_pos = primary_x_guard->lowerBound<true>(key, key_length);
               ensure(primary_pos >= 0);
               u8* primary_payload = primary_x_guard->getPayload(primary_pos);
               const u16 primary_payload_length = primary_x_guard->getPayloadLength(primary_pos);
               auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload + primary_payload_length - sizeof(PrimaryVersion));
               primary_version.worker_id = secondary_version.worker_id;
               primary_version.tts = secondary_version.tts;
               primary_version.next_version++;
               applyDelta(primary_payload, materialized_delta, delta_length);
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      case WAL_LOG_TYPE::WALRemove: {  // TODO:
         const auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
         u16 key_length = remove_entry.key_length + 8;
         u8 key[key_length];
         std::memcpy(key, remove_entry.payload, remove_entry.key_length);
         auto& sn = *reinterpret_cast<u64*>(key + remove_entry.key_length);
         sn = remove_entry.before_image_seq;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               // -------------------------------------------------------------------------------------
               // Get secondary version
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
               auto secondary_x_guard = ExclusivePageGuard(std::move(c_guard));
               const s32 secondary_pos = secondary_x_guard->lowerBound<true>(key, key_length);
               u8* secondary_payload = secondary_x_guard->getPayload(secondary_pos);
               ensure(secondary_pos >= 0);
               const u16 value_length = secondary_x_guard->getPayloadLength(secondary_pos) - sizeof(SecondaryVersion);
               auto secondary_version = *reinterpret_cast<SecondaryVersion*>(secondary_payload + value_length);
               u8 materialized_value[value_length];
               std::memcpy(materialized_value, secondary_payload, value_length);
               c_guard = std::move(secondary_x_guard);
               // -------------------------------------------------------------------------------------
               // Go to primary version
               sn = 0;
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::EXCLUSIVE>(c_guard, key, key_length);
               auto primary_x_guard = ExclusivePageGuard(std::move(c_guard));
               const s32 primary_pos = primary_x_guard->lowerBound<true>(key, key_length);
               ensure(primary_pos >= 0);
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      default: {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::iterateDesc(u8* start_key, u16 key_length, function<bool(HybridPageGuard<BTreeNode>&, s16)> callback)
{
   // TODO:
   u8* volatile next_key = start_key;
   volatile u16 next_key_length = key_length;
   volatile bool is_heap_freed = true;  // because at first we reuse the start_key
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         while (true) {
            findLeafCanJump(leaf, next_key, next_key_length);
            SharedPageGuard s_leaf(std::move(leaf));
            // -------------------------------------------------------------------------------------
            if (s_leaf->count == 0) {
               jumpmu_return;
            }
            s16 cur;
            if (next_key == start_key) {
               cur = s_leaf->lowerBound<false>(start_key, key_length);
               if (s_leaf->lowerBound<true>(start_key, key_length) == -1) {
                  cur--;
               }
            } else {
               cur = s_leaf->count - 1;
            }
            // -------------------------------------------------------------------------------------
            while (cur >= 0) {
               if (!callback(leaf, cur)) {
                  if (!is_heap_freed) {
                     delete[] next_key;
                     is_heap_freed = true;
                  }
                  jumpmu_return;
               }
               cur--;
            }
            // -------------------------------------------------------------------------------------
            if (!is_heap_freed) {
               delete[] next_key;
               is_heap_freed = true;
            }
            if (s_leaf->isLowerFenceInfinity()) {
               jumpmu_return;
            }
            // -------------------------------------------------------------------------------------
            next_key_length = s_leaf->lower_fence.length;
            next_key = new u8[next_key_length];
            is_heap_freed = false;
            memcpy(next_key, s_leaf->getLowerFenceKey(), s_leaf->lower_fence.length);
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::todo(void*, const u8*, const u64)
{
   ensure(false);
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTreeVI::getMeta()
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
OP_RESULT BTreeVI::scanDesc(u8* o_key, u16 o_key_length, [[maybe_ununsed]] function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   ensure(false);
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::scanAsc(u8* o_key,
                           u16 o_key_length,
                           function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback,
                           function<void()>)
{
   u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   *reinterpret_cast<SN*>(key_buffer + o_key_length) = 0;
   Slice key(key_buffer, key_length);
   StringU value;
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seek(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      // -------------------------------------------------------------------------------------
      bool skip = false;
      Slice key = iterator.key();
      Slice payload = iterator.value();
      SN sn = readSN(key);
      ensure(sn == 0);
      while (true) {
         if (sn == 0) {
            skip = false;
            const auto& primrary_version = *reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
            if (isVisibleForMe(primrary_version.worker_id, primrary_version.tts)) {
               callback(key.data(), key.length(), payload.data(), payload.length() - sizeof(PrimaryVersion));
               skip = true;
            } else {
               value = StringU(payload.data(), payload.length() - sizeof(PrimaryVersion));
            }
         } else {
            const auto& secondary_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
            const u16 delta_length = payload.length() - sizeof(SecondaryVersion);
            applyDelta(value.data(), payload.data(), delta_length);
            if (isVisibleForMe(secondary_version.worker_id, secondary_version.tts)) {
               callback(key.data(), key.length(), payload.data(), payload.length() - sizeof(PrimaryVersion));
               skip = true;
            }
         }
      next : {
         ret = iterator.next();
         if (ret == OP_RESULT::OK) {
            key = iterator.key();
            payload = iterator.value();
            sn = readSN(key);
            if (skip && sn != 0) {
               goto next;
            }
         } else {
            ensure(skip);
            break;
         }
      }
      }
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
void BTreeVI::reconstructTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(MutableSlice value)> callback) {

}
// -------------------------------------------------------------------------------------
void BTreeVI::applyDelta(u8* dst, const u8* delta_beginning, u16 delta_size)
{
   // TODO:
   const u8* delta_ptr = delta_beginning;
   while (delta_ptr - delta_beginning < delta_size) {
      const u16 offset = *reinterpret_cast<const u16*>(delta_ptr);
      delta_ptr += 2;
      const u16 size = *reinterpret_cast<const u16*>(delta_ptr);
      delta_ptr += 2;
      std::memcpy(dst + offset, delta_ptr, size);
      delta_ptr += size;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
