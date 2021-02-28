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
   // TODO: use optimistic latches for leaf 5K (optimistic scans)
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
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* value, u16 value_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
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
         PrimaryVersion* primary_version =
             reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version->isWriteLocked() || !isVisibleForMe(primary_version->worker_id, primary_version->tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         // -------------------------------------------------------------------------------------
         primary_version->writeLock();
         const u16 delta_and_descriptor_size = update_descriptor.size() + BTreeLL::calculateDeltaSize(update_descriptor);
         const u16 secondary_payload_length = delta_and_descriptor_size + sizeof(SecondaryVersion);
         // -------------------------------------------------------------------------------------
         u8 secondary_payload[secondary_payload_length];
         SecondaryVersion& secondary_version =
             *new (secondary_payload + delta_and_descriptor_size) SecondaryVersion(primary_version->worker_id, primary_version->tts, false, true);
         // -------------------------------------------------------------------------------------
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(secondary_payload, &update_descriptor, update_descriptor.size());
         BTreeLL::deltaBeforeImage(update_descriptor, secondary_payload + update_descriptor.size(), primary_payload.data());
         std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
         callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));
         BTreeLL::deltaXOR(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), primary_payload.data());
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         // Precise garbage collection TODO: recycling if it is worth it
         if (0) {
            auto getNextVisibleVersion = [&](u8 cur_worker_id, const SN start_sn) {
               SN trav_sn = start_sn;
               u64 versions_in_between = 0;
               while (true) {
                  setSN(m_key, trav_sn);
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  auto payload = iterator.value();
                  // -------------------------------------------------------------------------------------
                  u8 gc_worker_id;
                  u64 gc_tts;
                  SN next_sn;
                  bool is_final;
                  if (trav_sn == 0) {
                     auto& gc_version = *reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
                     gc_worker_id = gc_version.worker_id;
                     gc_tts = gc_version.tts;
                     next_sn = gc_version.next_sn;
                     is_final = gc_version.isFinal();
                  } else {
                     auto& gc_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
                     gc_worker_id = gc_version.worker_id;
                     gc_tts = gc_version.tts;
                     next_sn = gc_version.next_sn;
                     is_final = gc_version.isFinal();
                  }
                  // -------------------------------------------------------------------------------------
                  if (cr::Worker::my().isVisibleForIt(cur_worker_id, gc_worker_id, gc_tts) || is_final) {
                     return std::tuple<SN, u64>{trav_sn, versions_in_between};
                  } else {
                     trav_sn = next_sn;
                  }
                  versions_in_between++;
               }
            };
            SN current_sn = 0, visible_sn = primary_version->next_sn, prev_sn = current_sn;
            bool end_reached = false;
            for (u64 w_i = 0; !end_reached && w_i < cr::Worker::my().workers_count; w_i++) {
               const u8 sorted_w_id = cr::Worker::my().sorted_active_workers[w_i];
               auto next = getNextVisibleVersion(sorted_w_id, current_sn);
               SN next_sn = std::get<0>(next);
               u64 versions_in_between = std::get<1>(next);
               if (versions_in_between--) {
                  // Prune
                  setSN(m_key, prev_sn);
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  if (prev_sn == 0) {
                     next_sn = (reinterpret_cast<const PrimaryVersion*>(iterator.value().data()))->next_sn;
                  } else {
                     next_sn = (reinterpret_cast<const SecondaryVersion*>(iterator.value().data()))->next_sn;
                  }
                  // -------------------------------------------------------------------------------------
                  setSN(m_key, next_sn);
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  // -------------------------------------------------------------------------------------
                  // Delete
                  const SN collected_next_sn = reinterpret_cast<const SecondaryVersion*>(iterator.value().data())->next_sn;
                  iterator.removeCurrent();
                  // -------------------------------------------------------------------------------------
                  setSN(m_key, prev_sn);
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  if (prev_sn == 0) {
                     reinterpret_cast<PrimaryVersion*>(iterator.mutableValue().data())->next_sn = collected_next_sn;
                  } else {
                     reinterpret_cast<SecondaryVersion*>(iterator.mutableValue().data())->next_sn = collected_next_sn;
                  }
               } else {
                  current_sn = visible_sn;
               }
            }
         }
         // -------------------------------------------------------------------------------------
         {
            // Create new version
            secondary_version.worker_id = primary_version->worker_id;
            secondary_version.tts = primary_version->tts;
            secondary_version.next_sn = primary_version->next_sn;
            secondary_version.prev_sn = 0;
            do {
               secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
               // -------------------------------------------------------------------------------------
               setSN(m_key, secondary_sn);
               ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
            } while (ret != OP_RESULT::OK);
            // -------------------------------------------------------------------------------------
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            primary_payload = iterator.mutableValue();
            primary_version = reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
            primary_version->worker_id = myWorkerID();
            primary_version->tts = myTTS();
            primary_version->next_sn = secondary_sn;
            if (primary_version->versions_counter++ == 0) {
               primary_version->prev_sn = secondary_sn;
            }
            // -------------------------------------------------------------------------------------
            if (!primary_version->is_gc_scheduled) {
               cr::Worker::my().addTODO(myWorkerID(), myTTS(), dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
                  auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
                  todo_entry.key_length = o_key_length;
                  todo_entry.sn = secondary_sn;
                  std::memcpy(todo_entry.key, o_key, o_key_length);
               });
               primary_version->is_gc_scheduled = true;
            }
            primary_version->unlock();
            jumpmu_return OP_RESULT::OK;
         }
      }
      jumpmuCatch() { ensure(false); }
   }
   ensure(false);
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
   u8 key_buffer[o_key_length + sizeof(SN)];
   const u16 key_length = o_key_length + sizeof(SN);
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
         if (0) {
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
            const u16 value_length = primary_payload.length() - sizeof(PrimaryVersion);
            const u16 secondary_payload_length = value_length + sizeof(SecondaryVersion);
            u8 secondary_payload[secondary_payload_length];
            std::memcpy(secondary_payload, primary_payload.data(), value_length);
            // -------------------------------------------------------------------------------------
            auto& secondary_version =
                *new (secondary_payload + value_length) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, false);
            secondary_version.worker_id = primary_version.worker_id;
            secondary_version.tts = primary_version.tts;
            secondary_version.next_sn = primary_version.next_sn;
            do {
               secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
               // -------------------------------------------------------------------------------------
               setSN(m_key, secondary_sn);
               ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
            } while (ret != OP_RESULT::OK);
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
            primary_version.next_sn = secondary_sn;
            primary_version.is_gc_scheduled = true;
            primary_version.versions_counter++;
            primary_version.unlock();
         }
      }
      jumpmuCatch() { ensure(false); }
      // -------------------------------------------------------------------------------------
      cr::Worker::my().addTODO(myWorkerID(), myTTS(), dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
         auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
         todo_entry.key_length = o_key_length;
         std::memcpy(todo_entry.key, o_key, o_key_length);
      });
      return OP_RESULT::OK;
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   // TODO:
   ensure(false);
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   static_cast<void>(btree);
   const WALEntry& entry = *reinterpret_cast<const WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case WAL_LOG_TYPE::WALInsert: {  // Assuming on insert after remove
         break;
      }
      case WAL_LOG_TYPE::WALUpdate: {
         break;
      }
      case WAL_LOG_TYPE::WALRemove: {
         break;
      }
      default: {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::todo(void* btree_object, const u8* entry_ptr, const u64 tts)
{
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   const TODOEntry& todo_entry = *reinterpret_cast<const TODOEntry*>(entry_ptr);
   // -------------------------------------------------------------------------------------
   const u16 key_length = todo_entry.key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, todo_entry.key, todo_entry.key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   btree.setSN(m_key, 0);
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
         ret = iterator.seekExact(key);
         ensure(ret == OP_RESULT::OK);
         // -------------------------------------------------------------------------------------
         auto primary_payload = iterator.mutableValue();
         PrimaryVersion* primary_version =
             reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version->is_removed && primary_version->tts == tts) {
            const bool safe_to_remove = cr::Worker::my().isVisibleForAll(primary_version->worker_id, primary_version->tts);
            if (safe_to_remove) {
               primary_version->writeLock();
               SN next_sn = primary_version->next_sn;
               bool next_sn_higher = true;
               while (next_sn != 0) {
                  btree.setSN(m_key, next_sn);
                  ret = iterator.seekExactWithHint(key, next_sn_higher);
                  ensure(ret == OP_RESULT::OK);
                  // -------------------------------------------------------------------------------------
                  auto secondary_payload = iterator.value();
                  const auto& secondary_version =
                      *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
                  next_sn_higher = secondary_version.next_sn > next_sn;
                  next_sn = secondary_version.next_sn;
                  iterator.removeCurrent();
               }
               {
                  btree.setSN(m_key, 0);
                  ret = iterator.seekExactWithHint(key, false);
                  ensure(ret == OP_RESULT::OK);
                  iterator.removeCurrent();
               }
               jumpmu_return;
            } else {
               ensure(false);  // Should not happen in our current assumptions
            }
         } else {
            // TODO: High-water mark GC not only for main version
            const bool safe_to_remove = cr::Worker::my().isVisibleForAll(primary_version->worker_id, primary_version->tts);
            if (safe_to_remove) {  // Delete all older version
               primary_version->writeLock();
               SN next_sn = primary_version->next_sn;
               bool next_sn_higher = true;
               while (next_sn != 0) {
                  btree.setSN(m_key, next_sn);
                  ret = iterator.seekExactWithHint(key, next_sn_higher);
                  ensure(ret == OP_RESULT::OK);
                  // -------------------------------------------------------------------------------------
                  auto secondary_payload = iterator.value();
                  const auto& secondary_version =
                      *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
                  next_sn_higher = secondary_version.next_sn > next_sn;
                  next_sn = secondary_version.next_sn;
                  iterator.removeCurrent();
               }
               {
                  btree.setSN(m_key, 0);
                  ret = iterator.seekExactWithHint(key, false);
                  ensure(ret == OP_RESULT::OK);
                  // -------------------------------------------------------------------------------------
                  primary_payload = iterator.mutableValue();
                  primary_version = reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
                  primary_version->next_sn = 0;
                  primary_version->prev_sn = 0;
                  primary_version->versions_counter = 0;
                  primary_version->is_gc_scheduled = false;
                  primary_version->unlock();
               }
               jumpmu_return;
            } else {
               // TODO: Reschedule
               // ensure(false);
            }
         }
      }
      jumpmuCatch() {}
   }
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
   u16 chain_length = 1;
   OP_RESULT ret;
   Slice payload = iterator.value();
   assert(getSN(key) == 0);
   const PrimaryVersion* primary_version = reinterpret_cast<const PrimaryVersion*>(payload.data() + payload.length() - sizeof(PrimaryVersion));
   if (primary_version->isFinal()) {
      return {OP_RESULT::NOT_FOUND, chain_length};
   }
   u16 materialized_value_length = payload.length() - sizeof(PrimaryVersion);
   std::unique_ptr<u8[]> materialized_value = std::make_unique<u8[]>(materialized_value_length);
   std::memcpy(materialized_value.get(), payload.data(), materialized_value_length);
   SN secondary_sn = primary_version->next_sn;
   while (secondary_sn != 0) {
      setSN(key, secondary_sn);
      ret = iterator.seekExact(Slice(key.data(), key.length()));
      ensure(ret == OP_RESULT::OK);
      chain_length++;
      payload = iterator.value();
      const auto& secondary_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
      if (secondary_version.is_delta) {
         // Apply delta
         const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload.data());
         BTreeLL::deltaBeforeImage(update_descriptor, materialized_value.get(), payload.data() + update_descriptor.size());
      } else {
         materialized_value_length = payload.length() - sizeof(SecondaryVersion);
         materialized_value = std::make_unique<u8[]>(materialized_value_length);
         std::memcpy(materialized_value.get(), payload.data(), materialized_value_length);
      }
      ensure(!secondary_version.is_removed);
      if (isVisibleForMe(secondary_version.worker_id, secondary_version.tts)) {
         if (secondary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, chain_length};
         }
         callback(Slice(materialized_value.get(), materialized_value_length));
         return {OP_RESULT::OK, chain_length};
      }
      if (secondary_version.isFinal()) {
         raise(SIGTRAP);
         return {OP_RESULT::NOT_FOUND, chain_length};
      } else {
         secondary_sn = secondary_version.next_sn;
      }
   }
   raise(SIGTRAP);
   return {OP_RESULT::NOT_FOUND, chain_length};
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
