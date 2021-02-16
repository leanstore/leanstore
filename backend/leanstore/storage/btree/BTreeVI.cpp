#include "BTreeVI.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::OP_RESULT;
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
   if (1) {
      u16 key_length = o_key_length + sizeof(SN);
      u8 key_buffer[key_length];
      std::memcpy(key_buffer, o_key, o_key_length);
      MutableSlice key(key_buffer, key_length);
      setSN(key, 0);
      while (true) {
         jumpmuTry()
         {
            HybridPageGuard<BTreeNode> leaf;
            findLeafCanJump(leaf, key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            s16 pos = leaf->lowerBound<true>(key_buffer, key_length);
            if (pos != -1) {
               payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos) - sizeof(PrimaryVersion));
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
   } else {
      // 5K
      // -------------------------------------------------------------------------------------
      u16 key_length = o_key_length + sizeof(SN);
      u8 key_buffer[key_length];
      std::memcpy(key_buffer, o_key, o_key_length);
      MutableSlice key(key_buffer, key_length);
      setSN(key, 0);
      jumpmuTry()
      {
         BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
         auto ret = iterator.seekExact(Slice(key.data(), key.length()));
         if (ret != OP_RESULT::OK) {
            raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         ret = std::get<0>(reconstructTuple(iterator, key, [&](Slice value) { payload_callback(value.data(), value.length()); }));
         if (ret != OP_RESULT::OK) {  // For debugging
            raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         jumpmu_return ret;
      }
      jumpmuCatch() { ensure(false); }
   }
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
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   setSN(m_key, 0);
   Slice key(key_buffer, key_length);
   SN secondary_sn;
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   // 20K instructions more
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         ret = iterator.seekExact(key);
         if (ret != OP_RESULT::OK) {
            raise(SIGTRAP);
            jumpmu_return ret;
         }
         auto primary_payload = iterator.mutableValue();
         if (0) {
            callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
            jumpmu_return OP_RESULT::OK;
         }
         auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (!isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         // -------------------------------------------------------------------------------------
         secondary_sn = primary_version.next_sn;
         const u16 secondary_payload_length = wal_update_generator.entry_size + sizeof(SecondaryVersion);
         // -------------------------------------------------------------------------------------
         // Temporary
         if (1 && (primary_version.first_sn - 1) == primary_version.next_sn) {
            ensure(secondary_sn == primary_version.next_sn);
            u8 secondary_payload[secondary_payload_length];
            new (secondary_payload + wal_update_generator.entry_size) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, true);
            wal_update_generator.before(primary_payload.data(), secondary_payload);
            callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
            SN recent_sn = primary_version.first_sn;
            // -------------------------------------------------------------------------------------
            // Reuse recent version
            setSN(m_key, recent_sn);
            ret = iterator.seekExactWithHint(key, true);
            // ret = iterator.seekExact(key);
            ensure(ret == OP_RESULT::OK);
            if (iterator.mutableValue().length() >= secondary_payload_length) {
               ensure(iterator.mutableValue().length() >= secondary_payload_length);
               std::memcpy(iterator.mutableValue().data(), secondary_payload, secondary_payload_length);
               // -------------------------------------------------------------------------------------
               setSN(m_key, 0);
               ret = iterator.seekExactWithHint(key, false);
               // ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               auto primary_payload = iterator.mutableValue();
               auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
               primary_version.worker_id = myWorkerID();
               primary_version.tts = myTTS();
               primary_version.unlock();
               jumpmu_return OP_RESULT::OK;
            } else {
               iterator.removeCurrent();
               setSN(m_key, 0);
               ret = iterator.seekExactWithHint(key, false);
               ensure(ret == OP_RESULT::OK);
            }
         }
         // -------------------------------------------------------------------------------------
         primary_version.writeLock();
         const u64 buffer_zone_size = primary_payload.length() * 0;
         setSN(m_key, secondary_sn);
         if (iterator.keyInCurrentBoundaries(key) && iterator.enoughSpaceInCurrentNode(key, secondary_payload_length) == OP_RESULT::OK) {
            iterator.seekToInsertWithHint(key, true);
            iterator.insertInCurrentNode(key, secondary_payload_length);
            auto secondary_payload = iterator.mutableValue();
            new (secondary_payload.data() + wal_update_generator.entry_size) SecondaryVersion(myWorkerID(), myTTS(), false, true);
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            primary_payload = iterator.mutableValue();
            wal_update_generator.before(primary_payload.data(), secondary_payload.data());
            callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            primary_version.next_sn--;
            primary_version.unlock();
         } else {
            u8 secondary_payload[secondary_payload_length + buffer_zone_size];
            new (secondary_payload + wal_update_generator.entry_size + buffer_zone_size)
                SecondaryVersion(primary_version.worker_id, primary_version.tts, false, true);
            wal_update_generator.before(primary_payload.data(), secondary_payload);
            callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length + buffer_zone_size));
            ensure(ret == OP_RESULT::OK);
            // -------------------------------------------------------------------------------------
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            auto primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            primary_version.worker_id = myWorkerID();
            primary_version.tts = myTTS();
            primary_version.next_sn--;
            primary_version.unlock();
         }
         // TODO: WAL
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
      ensure(false);
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::insert(u8* o_key, u16 o_key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   *reinterpret_cast<SN*>(key_buffer + o_key_length) = 0;
   Slice key(key_buffer, key_length);
   const u16 payload_length = value_length + sizeof(PrimaryVersion);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            ensure(false);  // not implemented
         }
         ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         iterator.insertInCurrentNode(key, payload_length);
         auto payload = iterator.mutableValue();
         std::memcpy(payload.data(), value, value_length);
         new (payload.data() + value_length) PrimaryVersion(myWorkerID(), myTTS());
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::remove(u8* o_key, u16 o_key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key_buffer[o_key_length + 8];
   const u16 key_length = o_key_length + 8;
   std::memcpy(key_buffer, o_key, o_key_length);
   *reinterpret_cast<SN*>(key_buffer + o_key_length) = 0;
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         OP_RESULT ret = iterator.seekExact(key);
         if (ret != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         if (1) {
            iterator.removeCurrent();
            jumpmu_return OP_RESULT::OK;
         }
         // -------------------------------------------------------------------------------------
         SN secondary_sn;
         {
            auto primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            if (!isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
            primary_version.writeLock();
            secondary_sn = primary_version.next_sn;
            const u16 value_length = primary_payload.length() - sizeof(PrimaryVersion);
            const u16 secondary_payload_length = value_length + sizeof(SecondaryVersion);
            u8 secondary_payload[secondary_payload_length];
            std::memcpy(secondary_payload, primary_payload.data(), value_length);
            new (secondary_payload + value_length) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, false);
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
            ensure(ret == OP_RESULT::OK);
         }
         // -------------------------------------------------------------------------------------
         {
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            MutableSlice primary_payload = iterator.mutableValue();
            auto old_primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            iterator.shorten(sizeof(PrimaryVersion));
            primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data());
            primary_version = old_primary_version;
            primary_version.worker_id = myWorkerID();
            primary_version.tts = myTTS();
            primary_version.is_removed = true;
            primary_version.next_sn--;
            primary_version.unlock();
         }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   // TODO:
   ensure(false);
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
               primary_version.next_sn++;
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
               // auto secondary_version = *reinterpret_cast<SecondaryVersion*>(secondary_payload + value_length);
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
                                    .todo = todo,
                                    .serialize = serialize,
                                    .deserialize = deserialize};
   return btree_meta;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::scanDesc(u8* o_key, u16 o_key_length, function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   scan<false>(o_key, o_key_length, callback);
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::scanAsc(u8* o_key,
                           u16 o_key_length,
                           function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback,
                           function<void()>)
{
   scan<true>(o_key, o_key_length, callback);
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
std::tuple<OP_RESULT, u16> BTreeVI::reconstructTupleSlowPath(BTreeSharedIterator& iterator,
                                                             MutableSlice key,
                                                             std::function<void(Slice value)> callback)
{
   raise(SIGTRAP);
   u16 chain_length = 1;
   Slice payload = iterator.value();
   ensure(getSN(key) == 0);
   const auto primary_version = *reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
   if (primary_version.isFinal()) {
      return {OP_RESULT::NOT_FOUND, chain_length};
   }
   const u16 value_length = payload.length() - sizeof(PrimaryVersion);
   u8 value[value_length];
   std::memcpy(value, payload.data(), value_length);
   SN sn = primary_version.next_sn + 1;
   setSN(key, sn);
   while (iterator.seekExact(Slice(key.data(), key.length())) == OP_RESULT::OK) {
      chain_length++;
      payload = iterator.value();
      const auto& secondary_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
      ensure(secondary_version.is_delta);
      // Apply delta
      applyDelta(value, payload.data(), value_length);
      if (isVisibleForMe(secondary_version.worker_id, secondary_version.tts)) {
         if (secondary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, chain_length};
         }
         callback(Slice(value, value_length));
         return {OP_RESULT::OK, chain_length};
      }
      if (sn == std::numeric_limits<SN>::max()) {
         return {OP_RESULT::NOT_FOUND, chain_length};
      }
      setSN(key, ++sn);
   }
   return {OP_RESULT::NOT_FOUND, chain_length};
}
// -------------------------------------------------------------------------------------
void BTreeVI::applyDelta(u8* dst, const u8* delta_beginning, u16 delta_size)
{
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
