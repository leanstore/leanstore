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
// Keep in mind that garbage collection may leave pages completely empty
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::lookup(u8* o_key, u16 o_key_length, function<void(const u8*, u16)> payload_callback)
{
   u16 key_length = o_key_length + sizeof(ChainSN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   setSN(m_key, 0);
   Slice key(key_buffer, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
      auto ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      [[maybe_unused]] const auto primary_version = *reinterpret_cast<const ChainedTuple*>(iterator.value().data());
      auto reconstruct = reconstructTuple(iterator, m_key, [&](Slice value) { payload_callback(value.data(), value.length()); });
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().cc_read_chains[dt_id]++;
         WorkerCounters::myCounters().cc_read_versions_visited[dt_id] += std::get<1>(reconstruct);
      }
      ret = std::get<0>(reconstruct);
      if (ret != OP_RESULT::OK) {  // For debugging
         cout << endl;
         cout << u64(std::get<1>(reconstruct)) << endl;
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      jumpmu_return ret;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
const UpdateSameSizeInPlaceDescriptor& BTreeVI::FatTuple::updatedAttributesDescriptor() const
{
   ensure(same_attributes);
   return *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload + value_length);
}
// -------------------------------------------------------------------------------------
BTreeVI::FatTuple::Delta* BTreeVI::FatTuple::saDelta(u16 delta_i)
{
   ensure(same_attributes);
   ensure(used_space > value_length);
   return reinterpret_cast<Delta*>(payload + value_length + updatedAttributesDescriptor().size() + (delta_and_diff_length * delta_i));
}
// -------------------------------------------------------------------------------------
const BTreeVI::FatTuple::Delta* BTreeVI::FatTuple::csaDelta(u16 delta_i) const
{
   ensure(same_attributes);
   ensure(used_space > value_length);
   return reinterpret_cast<const Delta*>(payload + value_length + updatedAttributesDescriptor().size() + (delta_and_diff_length * delta_i));
}
// -------------------------------------------------------------------------------------
void BTreeVI::FatTuple::undoLastUpdate()
{
   // TODO:
   ensure(deltas_count >= 1);
   auto& delta = *reinterpret_cast<Delta*>(payload + value_length + updatedAttributesDescriptor().size());
   worker_id = delta.worker_id;
   tts = delta.tts;
   latest_commited_after_so = prev_commited_after_so;
   deltas_count -= 1;
   used_space -= delta_and_diff_length;
   BTreeLL::applyDiff(updatedAttributesDescriptor(), value(), delta.payload);
   std::memmove(saDelta(0), saDelta(1), deltas_count * delta_and_diff_length);
   if (deltas_count)
      debug = -1;
   else
      debug = 5;
}
// -------------------------------------------------------------------------------------
// Pre: tuple is write locked
bool BTreeVI::FatTuple::update(BTreeExclusiveIterator& iterator,
                               u8* o_key,
                               u16 o_key_length,
                               function<void(u8* value, u16 value_size)> cb,
                               UpdateSameSizeInPlaceDescriptor& update_descriptor,
                               BTreeVI& btree)
{
   ensure(same_attributes);
   if (FLAGS_vi_fupdate_fat_tuple) {
      cb(value(), value_length);
      return true;
   }
   const bool still_same_attributes = deltas_count == 0 || (update_descriptor == updatedAttributesDescriptor());
   ensure(still_same_attributes);  // TODO: return false to enforce and conversion to chained mode
   // -------------------------------------------------------------------------------------
   // Attention: we have to disable garbage collection if the latest delta was from us and not committed yet!
   // Otherwise we would crash during undo although the end result is the same if the transaction would commit (overwrite)
   const bool pgc = deltas_count >= FLAGS_vi_pgc_batch_size && !(worker_id == cr::Worker::my().workerID() && tts == cr::Worker::my().TTS());
   if (deltas_count > 0) {
      // Garbage collection first
      Delta* delta = saDelta(0);
      u16 delta_i = 0;
      // -------------------------------------------------------------------------------------
      if (deltas_count > 1 && cr::Worker::my().isVisibleForAll(delta->commited_before_so)) {
         const u16 removed_deltas = deltas_count - 1;
         used_space = reinterpret_cast<u8*>(saDelta(1)) - payload;  // Delete everything after the first delta
         deltas_count = 1;
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += removed_deltas; }
      } else if (pgc) {
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc[btree.dt_id]++; }
         cr::Worker::my().sortWorkers();
         u64 other_worker_index = 0;  // in the sorted array
         u64 other_worker_id = cr::Worker::my().all_sorted_so_starts[other_worker_index] & cr::Worker::WORKERS_MASK;
         auto next_worker = [&]() {
            if (++other_worker_index < cr::Worker::my().workers_count) {
               other_worker_id = cr::Worker::my().all_sorted_so_starts[other_worker_index] & cr::Worker::WORKERS_MASK;
               return true;
            } else {
               return false;
            }
         };
         auto is_current_worker_so_larger = [&](const u64 so) { return (cr::Worker::my().all_so_starts[other_worker_id]) > so; };
         // Skip all workers that see the latest version
         const u64 latest_commited_before_so = cr::Worker::my().getCB(other_worker_id, latest_commited_after_so);
         while (other_worker_index < cr::Worker::my().workers_count && is_current_worker_so_larger(latest_commited_before_so)) {
            next_worker();
         }
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc_skipped[btree.dt_id] += other_worker_index; }
         // -------------------------------------------------------------------------------------
         // Precise garbage collection
         // Note: we can't use so ordering to decide whether to remove a version
         // SO ordering helps in one case, if it tells visible then it is and nothing else
         while (other_worker_index < cr::Worker::my().workers_count && delta_i < deltas_count) {
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc_workers_visited[btree.dt_id]++; }
            if (cr::Worker::my().isVisibleForIt(other_worker_id, delta->worker_id, delta->tts)) {
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_kept[btree.dt_id]++; }
               while (next_worker()) {
                  if (is_current_worker_so_larger(delta->commited_before_so)) {
                     WorkerCounters::myCounters().cc_update_chains_pgc_skipped[btree.dt_id]++;
                  } else {
                     break;
                  }
               }
               delta_i++;
               delta = saDelta(delta_i);
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_skipped[btree.dt_id]++; }
               if (other_worker_index >= cr::Worker::my().workers_count) {
                  // Remove the rest including the current one
                  const u16 removed_deltas = deltas_count - delta_i;
                  deltas_count -= removed_deltas;
                  used_space = reinterpret_cast<u8*>(delta) - payload;
                  COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += removed_deltas; }
                  break;
               }
            } else {
               // Remove this delta
               const u16 deltas_to_move = (deltas_count - delta_i - 1) * delta_and_diff_length;
               std::memmove(delta, saDelta(delta_i + 1), deltas_to_move);
               used_space -= delta_and_diff_length;
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id]++; }
               deltas_count--;
            }
         }
      }
   } else {
      // Copy the update descriptor (set the attributes)
      std::memcpy(payload + value_length, &update_descriptor, update_descriptor.size());
      used_space += update_descriptor.size();
      delta_and_diff_length = sizeof(Delta) + update_descriptor.diffLength();
   }
   ensure(total_space >= used_space + delta_and_diff_length);
   // -------------------------------------------------------------------------------------
   {
      // Insert the new delta
      u8* deltas_beginn = payload + value_length + update_descriptor.size();
      std::memmove(deltas_beginn + delta_and_diff_length, deltas_beginn, delta_and_diff_length * deltas_count);
      auto& new_delta = *new (deltas_beginn) Delta();
      new_delta.worker_id = worker_id;
      new_delta.tts = tts;
      // Attention: we should not timestamp a delta that we created as committed!
      if (worker_id == cr::Worker::my().workerID() && tts == cr::Worker::my().TTS()) {
         new_delta.commited_before_so = std::numeric_limits<u64>::max();
      } else {
         new_delta.commited_before_so = cr::Worker::my().so_start;
      }
      BTreeLL::generateDiff(update_descriptor, new_delta.payload, value());
      used_space += delta_and_diff_length;
      prev_commited_after_so = latest_commited_after_so;
      latest_commited_after_so = cr::Worker::my().so_start;
      deltas_count++;
   }
   ensure(total_space >= used_space);
   // -------------------------------------------------------------------------------------
   {
      // WAL
      const u16 delta_and_descriptor_size = update_descriptor.size() + update_descriptor.diffLength();
      auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
      wal_entry->type = WAL_LOG_TYPE::WALUpdate;
      wal_entry->key_length = o_key_length;
      wal_entry->delta_length = delta_and_descriptor_size;
      wal_entry->before_worker_id = worker_id;
      wal_entry->before_tts = tts;
      worker_id = cr::Worker::my().workerID();
      tts = cr::Worker::my().TTS();
      wal_entry->after_worker_id = worker_id;
      wal_entry->after_tts = tts;
      std::memcpy(wal_entry->payload, o_key, o_key_length);
      std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
      // Update the value in-place
      BTreeLL::generateDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), value());
      cb(value(), value_length);
      debug = 1;
      BTreeLL::generateXORDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), value());
      wal_entry.submit();
   }
   return true;
}
// -------------------------------------------------------------------------------------
// Still missing: !same_attributes, remove
std::tuple<OP_RESULT, u16> BTreeVI::FatTuple::reconstructTuple(std::function<void(Slice value)> cb) const
{
   ensure(same_attributes);
   if (cr::Worker::my().isVisibleForMe(worker_id, tts)) {
      // Latest version is visible
      cb(Slice(cvalue(), value_length));
      return {OP_RESULT::OK, 1};
   } else if (deltas_count > 0) {
      u8 materialized_value[value_length];
      std::memcpy(materialized_value, cvalue(), value_length);
      // we have to apply the diffs
      u16 delta_i = 0;
      const Delta* delta = csaDelta(delta_i);
      while (delta_i < deltas_count) {
         if (cr::Worker::my().isVisibleForMe(delta->worker_id, delta->tts)) {
            BTreeLL::applyDiff(updatedAttributesDescriptor(), materialized_value, delta->payload);  // Apply diff
            cb(Slice(materialized_value, value_length));
            return {OP_RESULT::OK, delta_i + 2};
         }
         // -------------------------------------------------------------------------------------
         delta_i++;
         delta = csaDelta(delta_i);
      }
      // -------------------------------------------------------------------------------------
      raise(SIGTRAP);
      return {OP_RESULT::NOT_FOUND, delta_i + 2};
   } else {
      raise(SIGTRAP);
      return {OP_RESULT::NOT_FOUND, 1};
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::convertChainedToFatTuple(BTreeExclusiveIterator& iterator, MutableSlice& m_key)
{
   // TODO: Implement for variable diffs
   // Works only for same_attributes
   Slice key(m_key.data(), m_key.length());
   u8 fat_tuple_payload[PAGE_SIZE];
   u16 diff_length = 0;
   ChainSN next_sn;
   auto& fat_tuple = *new (fat_tuple_payload) FatTuple();
   fat_tuple.used_space = 0;
   {
      // Process the chain head
      MutableSlice head = iterator.mutableValue();
      auto& chain_head = *reinterpret_cast<ChainedTuple*>(head.data());
      ensure(chain_head.isWriteLocked());
      // -------------------------------------------------------------------------------------
      fat_tuple.value_length = head.length() - sizeof(ChainedTuple);
      std::memcpy(fat_tuple.payload + fat_tuple.used_space, chain_head.payload, fat_tuple.value_length);
      fat_tuple.used_space += fat_tuple.value_length;
      fat_tuple.worker_id = chain_head.worker_id;
      fat_tuple.tts = chain_head.tts;
      fat_tuple.latest_commited_after_so = chain_head.commited_after_so;
      // -------------------------------------------------------------------------------------
      next_sn = chain_head.next_sn;
   }
   u16 update_descriptor_size = 0;
   {
      // Iterate over the rest
      while (next_sn != 0) {
         const bool next_higher = next_sn >= getSN(m_key);
         setSN(m_key, next_sn);
         OP_RESULT ret = iterator.seekExactWithHint(key, next_higher);
         ensure(ret == OP_RESULT::OK);
         // -------------------------------------------------------------------------------------
         Slice delta_slice = iterator.value();
         const auto& chain_delta = *reinterpret_cast<const ChainedTupleDelta*>(delta_slice.data());
         // -------------------------------------------------------------------------------------
         if (update_descriptor_size == 0) {
            auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(chain_delta.payload);
            update_descriptor_size = update_descriptor.size();
            std::memcpy(fat_tuple.payload + fat_tuple.used_space, &update_descriptor, update_descriptor_size);
            fat_tuple.used_space += update_descriptor_size;
            diff_length = update_descriptor.diffLength();
            fat_tuple.delta_and_diff_length = sizeof(FatTuple::Delta) + diff_length;
         }
         // -------------------------------------------------------------------------------------
         // Add a FatTuple::Delta
         auto& new_delta = *new (fat_tuple.payload + fat_tuple.used_space) FatTuple::Delta();
         fat_tuple.used_space += sizeof(FatTuple::Delta);
         new_delta.worker_id = chain_delta.worker_id;
         new_delta.tts = chain_delta.tts;
         new_delta.commited_before_so = chain_delta.commited_before_so;
         std::memcpy(fat_tuple.payload + fat_tuple.used_space, chain_delta.payload + update_descriptor_size, diff_length);
         fat_tuple.used_space += diff_length;
         fat_tuple.deltas_count++;
         // -------------------------------------------------------------------------------------
         next_sn = chain_delta.next_sn;
         // Readers will restart and probably hang on the head's mutex
         ret = iterator.removeCurrent();
         ensure(ret == OP_RESULT::OK);
      }
   }
   ensure(fat_tuple.deltas_count == 0 || update_descriptor_size > 0);
   {
      // TODO: buggy
      // Finalize the new FatTuple
      // We could have more versions than the number of workers because of the way how gc works atm
      // const u16 space_needed_per_worker_version = sizeof(FatTuple::Delta) + diff_length;
      // fat_tuple.total_space = (fat_tuple.used_space / std::max<u64>(1, fat_tuple.deltas_count)) * cr::Worker::my().workers_count;
      // -------------------------------------------------------------------------------------
      setSN(m_key, 0);
      OP_RESULT ret = iterator.seekExactWithHint(key, false);
      ensure(ret == OP_RESULT::OK);
      ensure(reinterpret_cast<Tuple*>(iterator.mutableValue().data())->isWriteLocked());
      // -------------------------------------------------------------------------------------
      fat_tuple.total_space = 3 * 1024;
      ensure(fat_tuple.total_space >= fat_tuple.used_space);  // TODO:
      const u16 fat_tuple_length = sizeof(FatTuple) + fat_tuple.total_space;
      const u16 required_extra_space_in_node = (iterator.value().length() >= fat_tuple_length) ? 0 : (fat_tuple_length - iterator.value().length());
      ret = iterator.enoughSpaceInCurrentNode(key, required_extra_space_in_node);
      while (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
         iterator.splitForKey(key);
         ret = iterator.seekExact(key);
         ensure(ret == OP_RESULT::OK);
         ret = iterator.enoughSpaceInCurrentNode(key, required_extra_space_in_node);
      }
      setSN(m_key, 0);
      ret = iterator.seekExact(key);
      ensure(ret == OP_RESULT::OK);
      iterator.removeCurrent();
      iterator.insertInCurrentNode(key, fat_tuple_length);
      std::memcpy(iterator.mutableValue().data(), fat_tuple_payload, fat_tuple_length);
   }
   ensure(fat_tuple.deltas_count == 0 || fat_tuple.updatedAttributesDescriptor().count < 10);
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* value, u16 value_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(ChainSN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   Slice key(key_buffer, key_length);
   MutableSlice m_key(key_buffer, key_length);
   setSN(m_key, 0);
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   // 20K instructions more
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return ret;
      }
      // -------------------------------------------------------------------------------------
   restart : {
      MutableSlice primary_payload = iterator.mutableValue();
      auto& tuple = *reinterpret_cast<Tuple*>(primary_payload.data());
      if (tuple.isWriteLocked() || !isVisibleForMe(tuple.worker_id, tuple.tts)) {
         jumpmu_return OP_RESULT::ABORT_TX;
      }
      tuple.writeLock();
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains[dt_id]++; }
      // -------------------------------------------------------------------------------------
      if (tuple.tuple_format == TupleFormat::FAT_TUPLE) {
         const bool res = reinterpret_cast<FatTuple*>(&tuple)->update(iterator, o_key, o_key_length, callback, update_descriptor, *this);
         ensure(res);  // TODO: what if it fails, then we have to do something else
         tuple.unlock();
         // -------------------------------------------------------------------------------------
         iterator.contentionSplit();
         jumpmu_return OP_RESULT::OK;
      } else {
         auto& chain_head = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
         if (FLAGS_vi_fupdate_chained) {  //  (dt_id != 0 && dt_id != 1 && dt_id != 10)
            // WAL
            u16 delta_and_descriptor_size = update_descriptor.size() + update_descriptor.diffLength();
            auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
            wal_entry->type = WAL_LOG_TYPE::WALUpdate;
            wal_entry->key_length = o_key_length;
            wal_entry->delta_length = delta_and_descriptor_size;
            wal_entry->before_worker_id = chain_head.worker_id;
            wal_entry->before_tts = chain_head.tts;
            wal_entry->after_worker_id = cr::Worker::my().workerID();
            wal_entry->after_tts = cr::Worker::my().TTS();
            std::memcpy(wal_entry->payload, o_key, o_key_length);
            std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
            BTreeLL::generateDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), chain_head.payload);
            callback(chain_head.payload, primary_payload.length() - sizeof(ChainedTuple));
            BTreeLL::generateXORDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), chain_head.payload);
            wal_entry.submit();
            tuple.unlock();
            // -------------------------------------------------------------------------------------
            iterator.contentionSplit();
            jumpmu_return OP_RESULT::OK;
         } else if (FLAGS_vi_fat_tuple && dt_id != 2 && chain_head.versions_counter > 2) {
            ensure(chain_head.isWriteLocked());
            convertChainedToFatTuple(iterator, m_key);
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_fat_tuple_convert[dt_id]++; }
            goto restart;
            UNREACHABLE();
         }
      }
   }
      // -------------------------------------------------------------------------------------
      // Update in chained mode
      u16 delta_and_descriptor_size = update_descriptor.size() + update_descriptor.diffLength();
      u16 secondary_payload_length = delta_and_descriptor_size + sizeof(ChainedTupleDelta);
      u8 secondary_payload[PAGE_SIZE];
      auto& secondary_version = *reinterpret_cast<ChainedTupleDelta*>(secondary_payload);
      std::memcpy(secondary_version.payload, &update_descriptor, update_descriptor.size());
      // -------------------------------------------------------------------------------------
      ChainSN secondary_sn;
      // -------------------------------------------------------------------------------------
      DanglingPointer dangling_pointer;
      {
         MutableSlice head_payload = iterator.mutableValue();
         ChainedTuple& head_version = *reinterpret_cast<ChainedTuple*>(head_payload.data());
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_dangling_pointer) {
            dangling_pointer.bf = iterator.leaf.bf;
            dangling_pointer.version = iterator.leaf.guard.latch->version;
            dangling_pointer.head_slot = iterator.cur;
         }
         // -------------------------------------------------------------------------------------
         BTreeLL::generateDiff(update_descriptor, secondary_version.payload + update_descriptor.size(), head_version.payload);
         // -------------------------------------------------------------------------------------
         new (secondary_payload) ChainedTupleDelta(head_version.worker_id, head_version.tts, false, true);
         secondary_version.next_sn = head_version.next_sn;
         if (secondary_version.worker_id == cr::Worker::my().workerID() && secondary_version.tts == cr::Worker::my().TTS()) {
            secondary_version.commited_before_so = std::numeric_limits<u64>::max();
         } else {
            secondary_version.commited_before_so = cr::Worker::my().SOStart();
         }
         secondary_version.commited_after_so = head_version.commited_after_so;
         // -------------------------------------------------------------------------------------
         if (head_version.next_sn <= 1) {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<ChainSN>(1, std::numeric_limits<ChainSN>::max());
         } else {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<ChainSN>(1, head_version.next_sn);
         }
         iterator.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      // Write the ChainedTupleDelta
      while (true) {
         setSN(m_key, secondary_sn);
         ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         if (ret == OP_RESULT::OK) {
            break;
         } else {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<ChainSN>(1, std::numeric_limits<ChainSN>::max());
         }
      }
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_created[dt_id]++; }
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      {
         // Return to the head
         MutableSlice head_payload(nullptr, 0);
         if (FLAGS_vi_dangling_pointer && dangling_pointer.bf == iterator.leaf.bf && dangling_pointer.version == iterator.leaf.guard.latch->version) {
            dangling_pointer.secondary_slot = iterator.cur;
            iterator.cur = dangling_pointer.head_slot;
            head_payload = iterator.mutableValue();
         } else {
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            head_payload = iterator.mutableValue();
         }
         ChainedTuple& head_version = *reinterpret_cast<ChainedTuple*>(head_payload.data());
         // -------------------------------------------------------------------------------------
         // Head WTTS if needed
         u64 head_wtts = 0;
         if (head_version.versions_counter == 1) {
            head_wtts = cr::Worker::composeWTTS(head_version.worker_id, head_version.tts);
         }
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = o_key_length;
         wal_entry->delta_length = delta_and_descriptor_size;
         wal_entry->before_worker_id = head_version.worker_id;
         wal_entry->before_tts = head_version.tts;
         wal_entry->after_worker_id = cr::Worker::my().workerID();
         wal_entry->after_tts = cr::Worker::my().TTS();
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
         BTreeLL::generateDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), head_version.payload);
         callback(head_version.payload, head_payload.length() - sizeof(ChainedTuple));  // Update
         BTreeLL::generateXORDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), head_version.payload);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         head_version.worker_id = cr::Worker::my().workerID();
         head_version.tts = cr::Worker::my().TTS();
         head_version.next_sn = secondary_sn;
         head_version.versions_counter += 1;
         head_version.commited_after_so = cr::Worker::my().so_start;
         head_version.tmp = 1;
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_utodo && !head_version.is_gc_scheduled) {
            cr::Worker::my().stageTODO(
                head_version.worker_id, head_version.tts, dt_id, key_length + sizeof(TODOEntry),
                [&](u8* entry) {
                   auto& todo_entry = *new (entry) TODOEntry();
                   todo_entry.key_length = o_key_length;
                   todo_entry.sn = secondary_sn;
                   todo_entry.dangling_pointer = dangling_pointer;
                   std::memcpy(todo_entry.key, o_key, o_key_length);
                },
                head_wtts);
            head_version.is_gc_scheduled = true;
         }
         // -------------------------------------------------------------------------------------
         head_version.unlock();
         iterator.contentionSplit();
         jumpmu_return OP_RESULT::OK;
      }
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::insert(u8* o_key, u16 o_key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(ChainSN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   setSN(m_key, 0);
   const u16 payload_length = value_length + sizeof(ChainedTuple);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            MutableSlice primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
            if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
            ensure(false);  // Not implemented: maybe it has been removed but no GCed
         }
         ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(o_key_length + value_length);
         wal_entry->type = WAL_LOG_TYPE::WALInsert;
         wal_entry->key_length = o_key_length;
         wal_entry->value_length = value_length;
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, value, value_length);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         iterator.insertInCurrentNode(key, payload_length);
         MutableSlice payload = iterator.mutableValue();
         auto& primary_version = *new (payload.data()) ChainedTuple(cr::Worker::my().workerID(), cr::Worker::my().TTS());
         std::memcpy(primary_version.payload, value, value_length);
         primary_version.commited_after_so = cr::Worker::my().so_start;
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { UNREACHABLE(); }
   }
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::remove(u8* o_key, u16 o_key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key_buffer[o_key_length + sizeof(ChainSN)];
   const u16 key_length = o_key_length + sizeof(ChainSN);
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   setSN(m_key, 0);
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
      OP_RESULT ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         raise(SIGTRAP);
         jumpmu_return OP_RESULT::NOT_FOUND;
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_vi_fremove) {
         ret = iterator.removeCurrent();
         ensure(ret == OP_RESULT::OK);
         iterator.mergeIfNeeded();
         jumpmu_return OP_RESULT::OK;
      }
      // -------------------------------------------------------------------------------------
      u16 value_length, secondary_payload_length;
      u8 secondary_payload[PAGE_SIZE];
      auto& secondary_version = *reinterpret_cast<ChainedTupleDelta*>(secondary_payload);
      ChainSN secondary_sn;
      // -------------------------------------------------------------------------------------
      DanglingPointer dangling_pointer;
      {
         auto primary_payload = iterator.mutableValue();
         ChainedTuple& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_dangling_pointer) {
            dangling_pointer.bf = iterator.leaf.bf;
            dangling_pointer.version = iterator.leaf.guard.version;
            dangling_pointer.head_slot = iterator.cur;
         }
         // -------------------------------------------------------------------------------------
         ensure(primary_version.tuple_format == TupleFormat::CHAINED);  // TODO: removing fat tuple is not supported atm
         if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         ensure(primary_version.is_removed == false);
         primary_version.writeLock();
         // -------------------------------------------------------------------------------------
         value_length = iterator.value().length() - sizeof(ChainedTuple);
         secondary_payload_length = sizeof(ChainedTupleDelta) + value_length;
         new (secondary_payload) ChainedTupleDelta(primary_version.worker_id, primary_version.tts, false, false);
         secondary_version.worker_id = primary_version.worker_id;
         secondary_version.tts = primary_version.tts;
         secondary_version.next_sn = primary_version.next_sn;
         std::memcpy(secondary_version.payload, primary_payload.data(), value_length);
         iterator.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      {
         do {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<ChainSN>(0, std::numeric_limits<ChainSN>::max());
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         } while (ret != OP_RESULT::OK);
      }
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      {
         // Return to the head
         MutableSlice primary_payload(nullptr, 0);
         if (FLAGS_vi_dangling_pointer && dangling_pointer.bf == iterator.leaf.bf && dangling_pointer.version == iterator.leaf.guard.version) {
            dangling_pointer.secondary_slot = iterator.cur;
            iterator.cur = dangling_pointer.head_slot;
            ensure(dangling_pointer.secondary_slot > dangling_pointer.head_slot);
            primary_payload = iterator.mutableValue();
         } else {
            setSN(m_key, 0);
            ret = iterator.seekExactWithHint(key, false);
            ensure(ret == OP_RESULT::OK);
            primary_payload = iterator.mutableValue();
         }
         ChainedTuple old_primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length + value_length);
         wal_entry->type = WAL_LOG_TYPE::WALRemove;
         wal_entry->key_length = o_key_length;
         wal_entry->value_length = value_length;
         wal_entry->before_worker_id = old_primary_version.worker_id;
         wal_entry->before_tts = old_primary_version.tts;
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, iterator.value().data(), value_length);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         iterator.shorten(sizeof(ChainedTuple));
         primary_payload = iterator.mutableValue();
         auto& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
         primary_version = old_primary_version;
         primary_version.is_removed = true;
         primary_version.worker_id = cr::Worker::my().workerID();
         primary_version.tts = cr::Worker::my().TTS();
         primary_version.next_sn = secondary_sn;
         primary_version.commited_after_so = cr::Worker::my().SOStart();
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_rtodo && !primary_version.is_gc_scheduled) {
            const u64 wtts = cr::Worker::composeWTTS(old_primary_version.worker_id, old_primary_version.tts);
            cr::Worker::my().stageTODO(
                cr::Worker::my().workerID(), cr::Worker::my().TTS(), dt_id, key_length + sizeof(TODOEntry),
                [&](u8* entry) {
                   auto& todo_entry = *new (entry) TODOEntry();
                   todo_entry.key_length = o_key_length;
                   todo_entry.dangling_pointer = dangling_pointer;
                   std::memcpy(todo_entry.key, o_key, o_key_length);
                },
                wtts);
            primary_version.is_gc_scheduled = true;
         }
         primary_version.unlock();
      }
      // -------------------------------------------------------------------------------------
      jumpmu_return OP_RESULT::OK;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
// This undo implementation works only for rollback and not for undo operations during recovery
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   // TODO: accelerate using DanglingPointer
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   static_cast<void>(btree);
   const WALEntry& entry = *reinterpret_cast<const WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case WAL_LOG_TYPE::WALInsert: {  // Assuming no insert after remove
         auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
         jumpmuTry()
         {
            const u16 key_length = insert_entry.key_length + sizeof(ChainSN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, insert_entry.payload, insert_entry.key_length);
            *reinterpret_cast<ChainSN*>(key_buffer + insert_entry.key_length) = 0;
            Slice key(key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            auto ret = iterator.seekExact(key);
            ensure(ret == OP_RESULT::OK);
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
            iterator.markAsDirty();  // TODO: write CLS
            iterator.mergeIfNeeded();
         }
         jumpmuCatch() {}
         break;
      }
      case WAL_LOG_TYPE::WALUpdate: {
         auto& update_entry = *reinterpret_cast<const WALUpdateSSIP*>(&entry);
         jumpmuTry()
         {
            const u16 key_length = update_entry.key_length + sizeof(ChainSN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, update_entry.payload, update_entry.key_length);
            Slice key(key_buffer, key_length);
            MutableSlice m_key(key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            ChainSN undo_sn;
            OP_RESULT ret;
            u8 secondary_payload[PAGE_SIZE];
            u16 secondary_payload_length;
            // -------------------------------------------------------------------------------------
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            btree.setSN(m_key, 0);
            ret = iterator.seekExact(key);
            ensure(ret == OP_RESULT::OK);
            {
               auto& tuple = *reinterpret_cast<Tuple*>(iterator.mutableValue().data());
               ensure(!tuple.isWriteLocked());
               if (tuple.tuple_format == TupleFormat::FAT_TUPLE) {
                  reinterpret_cast<FatTuple*>(iterator.mutableValue().data())->undoLastUpdate();
                  jumpmu_return;
               }
            }
            {
               MutableSlice primary_payload = iterator.mutableValue();
               auto& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
               // -------------------------------------------------------------------------------------
               // Checks
               if (primary_version.worker_id != cr::Worker::my().workerID() || primary_version.tts != cr::Worker::my().TTS()) {
                  iterator.assembleKey();
                  ensure(std::memcmp(iterator.key().data(), key_buffer, key_length) == 0);
                  cerr << primary_version.tmp << "," << u64(primary_version.worker_id) << "!=" << u64(cr::Worker::my().workerID()) << ", "
                       << primary_version.tts << "!=" << cr::Worker::my().TTS() << endl;
               }
               ensure(primary_version.worker_id == cr::Worker::my().workerID());
               ensure(primary_version.tts == cr::Worker::my().TTS());
               ensure(!primary_version.isWriteLocked());
               // -------------------------------------------------------------------------------------
               primary_version.writeLock();
               undo_sn = primary_version.next_sn;
               iterator.markAsDirty();
            }
            {
               btree.setSN(m_key, undo_sn);
               ret = iterator.seekExactWithHint(key, true);
               ensure(ret == OP_RESULT::OK);
               secondary_payload_length = iterator.value().length();
               std::memcpy(secondary_payload, iterator.value().data(), secondary_payload_length);
               iterator.markAsDirty();
            }
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExactWithHint(key, false);
               ensure(ret == OP_RESULT::OK);
               MutableSlice primary_payload = iterator.mutableValue();
               auto& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
               const auto& secondary_version = *reinterpret_cast<ChainedTupleDelta*>(secondary_payload);
               primary_version.next_sn = secondary_version.next_sn;
               primary_version.tts = secondary_version.tts;
               primary_version.worker_id = secondary_version.worker_id;
               primary_version.versions_counter--;
               primary_version.tmp = 2;
               // -------------------------------------------------------------------------------------
               const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(secondary_version.payload);
               BTreeLL::applyDiff(update_descriptor, primary_version.payload, secondary_version.payload + update_descriptor.size());
               // -------------------------------------------------------------------------------------
               primary_version.unlock();
               iterator.markAsDirty();  // TODO: write CLS
            }
            {
               btree.setSN(m_key, undo_sn);
               ret = iterator.seekExactWithHint(key, true);
               ensure(ret == OP_RESULT::OK);
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               iterator.markAsDirty();
               iterator.mergeIfNeeded();
            }
         }
         jumpmuCatch() { UNREACHABLE(); }
         break;
      }
      case WAL_LOG_TYPE::WALRemove: {
         auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
         const u16 key_length = remove_entry.key_length + sizeof(ChainSN);
         u8 key_buffer[key_length];
         std::memcpy(key_buffer, remove_entry.payload, remove_entry.key_length);
         Slice key(key_buffer, key_length);
         MutableSlice m_key(key_buffer, key_length);
         const u16 payload_length = remove_entry.value_length + sizeof(ChainedTuple);
         // -------------------------------------------------------------------------------------
         jumpmuTry()
         {
            ChainSN secondary_sn, undo_next_sn;
            OP_RESULT ret;
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            u16 removed_value_length;
            u8 removed_value[PAGE_SIZE];
            u8 undo_worker_id;
            u64 undo_tts;
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               auto& primary_version = *reinterpret_cast<ChainedTuple*>(iterator.mutableValue().data());
               secondary_sn = primary_version.next_sn;
               if (primary_version.worker_id != cr::Worker::my().workerID()) {
                  raise(SIGTRAP);
               }
               ensure(primary_version.worker_id == cr::Worker::my().workerID());
               ensure(primary_version.tts == cr::Worker::my().TTS());
               primary_version.writeLock();
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, secondary_sn);
               ret = iterator.seekExactWithHint(key, true);
               ensure(ret == OP_RESULT::OK);
               auto secondary_payload = iterator.value();
               auto const secondary_version = *reinterpret_cast<const ChainedTupleDelta*>(secondary_payload.data());
               removed_value_length = secondary_payload.length() - sizeof(ChainedTupleDelta);
               std::memcpy(removed_value, secondary_version.payload, removed_value_length);
               undo_worker_id = secondary_version.worker_id;
               undo_tts = secondary_version.tts;
               undo_next_sn = secondary_version.next_sn;
               iterator.markAsDirty();
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExactWithHint(key, 0);
               ensure(ret == OP_RESULT::OK);
               const u16 required_extra_space_in_node = payload_length - iterator.value().length();
               ret = iterator.enoughSpaceInCurrentNode(key, required_extra_space_in_node);  // TODO:
               bool should_reset_to_head = false;
               while (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
                  iterator.splitForKey(key);
                  ret = iterator.enoughSpaceInCurrentNode(key, required_extra_space_in_node);
                  should_reset_to_head = true;
               }
               if (should_reset_to_head) {
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
               }
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               const u16 primary_payload_length = removed_value_length + sizeof(ChainedTuple);
               iterator.insertInCurrentNode(key, primary_payload_length);
               auto primary_payload = iterator.mutableValue();
               auto& primary_version = *new (primary_payload.data()) ChainedTuple(undo_worker_id, undo_tts);
               std::memcpy(primary_version.payload, removed_value, removed_value_length);
               primary_version.next_sn = undo_next_sn;
               primary_version.tmp = 5;
               ensure(primary_version.is_removed == false);
               primary_version.unlock();
               iterator.markAsDirty();
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, secondary_sn);
               ret = iterator.seekExactWithHint(key, true);
               ensure(ret == OP_RESULT::OK);
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               iterator.markAsDirty();
            }
         }
         jumpmuCatch() { UNREACHABLE(); }
         break;
      }
      default: {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTreeVI::todo(void* btree_object, const u8* entry_ptr, const u64 version_worker_id, const u64 version_tts)
{
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   const TODOEntry& todo_entry = *reinterpret_cast<const TODOEntry*>(entry_ptr);
   // -------------------------------------------------------------------------------------
   if (FLAGS_vi_dangling_pointer && todo_entry.dangling_pointer.isValid()) {
      // Optimistic fast path
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree), todo_entry.dangling_pointer.bf,
                                         todo_entry.dangling_pointer.version + 1);
         assert(todo_entry.dangling_pointer.bf != nullptr);
         auto& node = iterator.leaf;
         auto& head = *reinterpret_cast<ChainedTuple*>(node->getPayload(todo_entry.dangling_pointer.head_slot));
         // Being chained is implicit because we check for version, so the state can not be changed after staging the todo
         ensure(head.tuple_format == TupleFormat::CHAINED && !head.isWriteLocked());
         ensure(head.worker_id == version_worker_id && head.tts == version_tts);
         ensure(head.versions_counter <= FLAGS_vi_max_chain_length);
         if (head.is_removed) {
            node->removeSlot(todo_entry.dangling_pointer.secondary_slot);
            node->removeSlot(todo_entry.dangling_pointer.head_slot);
         } else {
            head.versions_counter = 1;
            head.is_gc_scheduled = false;
            head.next_sn = reinterpret_cast<ChainedTupleDelta*>(node->getPayload(todo_entry.dangling_pointer.secondary_slot))->next_sn;
            node->removeSlot(todo_entry.dangling_pointer.secondary_slot);
         }
         iterator.mergeIfNeeded();
         jumpmu_return;
      }
      jumpmuCatch() {}
   }
   // -------------------------------------------------------------------------------------
   const u16 key_length = todo_entry.key_length + sizeof(ChainSN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, todo_entry.key, todo_entry.key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   btree.setSN(m_key, 0);
   OP_RESULT ret;
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
      ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {  // Legit case
         jumpmu_return;
      }
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_chains[btree.dt_id]++; }
      // -------------------------------------------------------------------------------------
      MutableSlice primary_payload = iterator.mutableValue();
      {
         // Checks
         const auto& tuple = *reinterpret_cast<const Tuple*>(primary_payload.data());
         if (tuple.tuple_format == TupleFormat::FAT_TUPLE) {
            jumpmu_return;
         }
      }
      // -------------------------------------------------------------------------------------
      ChainedTuple& primary_version = *reinterpret_cast<ChainedTuple*>(primary_payload.data());
      primary_version.is_gc_scheduled = false;
      const bool is_removed = primary_version.is_removed;
      const bool safe_to_gc =
          (primary_version.worker_id == version_worker_id && primary_version.tts == version_tts) && !primary_version.isWriteLocked();
      if (safe_to_gc) {
         ChainSN next_sn = primary_version.next_sn;
         if (is_removed) {
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
            iterator.mergeIfNeeded();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_remove[btree.dt_id]++; }
         } else {
            primary_version.versions_counter = 1;
            primary_version.next_sn = 0;
            primary_version.tmp = -1;
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_updates[btree.dt_id]++; }
         }
         iterator.markAsDirty();
         // -------------------------------------------------------------------------------------
         bool next_sn_higher = true;
         while (next_sn != 0) {
            next_sn_higher = next_sn >= btree.getSN(key);
            btree.setSN(m_key, next_sn);
            ret = iterator.seekExactWithHint(key, next_sn_higher);
            if (ret != OP_RESULT::OK) {
               // raise(SIGTRAP); should be fine if the tuple got converted to fat
               break;
            }
            // -------------------------------------------------------------------------------------
            Slice secondary_payload = iterator.value();
            const auto& secondary_version = *reinterpret_cast<const ChainedTupleDelta*>(secondary_payload.data());
            next_sn = secondary_version.next_sn;
            // -------------------------------------------------------------------------------------
            iterator.removeCurrent();
            iterator.mergeIfNeeded();
            iterator.markAsDirty();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_updates_versions_removed[btree.dt_id]++; }
         }
         iterator.mergeIfNeeded();
      } else {
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_wasted[btree.dt_id]++; }
         // Cross workers TODO: better solution?
         if (!primary_version.isWriteLocked()) {
            u64 new_todo_worker_id, new_todo_tts;
            if (cr::Worker::my().isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
               new_todo_worker_id = primary_version.worker_id;
               new_todo_tts = primary_version.tts;
            } else {
               // Any worker or version tts, just a placeholder
               new_todo_worker_id = version_worker_id;
               new_todo_tts = version_tts;
            }
            cr::Worker::my().commitTODO(new_todo_worker_id, new_todo_tts, cr::Worker::my().so_start, btree.dt_id,
                                        todo_entry.key_length + sizeof(TODOEntry),
                                        [&](u8* new_entry) { std::memcpy(new_entry, &todo_entry, sizeof(TODOEntry) + todo_entry.key_length); });
            primary_version.is_gc_scheduled = true;
         }
      }
   }
   jumpmuCatch() { UNREACHABLE(); }
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
std::tuple<OP_RESULT, u16> BTreeVI::reconstructChainedTuple(BTreeSharedIterator& iterator,
                                                            MutableSlice key,
                                                            std::function<void(Slice value)> callback)
{
   assert(getSN(key) == 0);
   u16 chain_length = 1;
   OP_RESULT ret;
   u16 materialized_value_length;
   std::unique_ptr<u8[]> materialized_value;
   ChainSN secondary_sn;
   {
      Slice primary_payload = iterator.value();
      const ChainedTuple& primary_version = *reinterpret_cast<const ChainedTuple*>(primary_payload.data());
      if (isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
         if (primary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, 1};
         }
         callback(Slice(primary_version.payload, primary_payload.length() - sizeof(ChainedTuple)));
         return {OP_RESULT::OK, 1};
      }
      if (primary_version.isFinal()) {
         return {OP_RESULT::NOT_FOUND, 1};
      }
      materialized_value_length = primary_payload.length() - sizeof(ChainedTuple);
      materialized_value = std::make_unique<u8[]>(materialized_value_length);
      std::memcpy(materialized_value.get(), primary_version.payload, materialized_value_length);
      secondary_sn = primary_version.next_sn;
   }
   // -------------------------------------------------------------------------------------
   bool next_sn_higher = true;
   while (secondary_sn != 0) {
      setSN(key, secondary_sn);
      ret = iterator.seekExactWithHint(Slice(key.data(), key.length()), next_sn_higher);
      if (ret != OP_RESULT::OK) {
         // Happens either due to undo or garbage collection
         setSN(key, 0);
         ret = iterator.seekExact(Slice(key.data(), key.length()));
         ensure(ret == OP_RESULT::OK);
         jumpmu::jump();
      }
      chain_length++;
      ensure(chain_length <= FLAGS_vi_max_chain_length);
      Slice payload = iterator.value();
      const auto& secondary_version = *reinterpret_cast<const ChainedTupleDelta*>(payload.data());
      if (secondary_version.is_delta) {
         // Apply delta
         const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(secondary_version.payload);
         BTreeLL::applyDiff(update_descriptor, materialized_value.get(), secondary_version.payload + update_descriptor.size());
      } else {
         materialized_value_length = payload.length() - sizeof(ChainedTupleDelta);
         materialized_value = std::make_unique<u8[]>(materialized_value_length);
         std::memcpy(materialized_value.get(), secondary_version.payload, materialized_value_length);
      }
      ensure(!secondary_version.is_removed);
      if (isVisibleForMe(secondary_version.worker_id, secondary_version.tts)) {
         if (secondary_version.is_removed) {
            raise(SIGTRAP);
            return {OP_RESULT::NOT_FOUND, chain_length};
         }
         callback(Slice(materialized_value.get(), materialized_value_length));
         return {OP_RESULT::OK, chain_length};
      }
      if (secondary_version.isFinal()) {
         // cout << chain_length << endl;
         // raise(SIGTRAP);
         return {OP_RESULT::NOT_FOUND, chain_length};
      } else {
         next_sn_higher = secondary_version.next_sn >= secondary_sn;
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
