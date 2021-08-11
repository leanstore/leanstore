#include "BTreeVI.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>

#include <map>
#include <set>
#include <unordered_map>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace std
{
template <>
class hash<leanstore::UpdateSameSizeInPlaceDescriptor::Slot>
{
  public:
   size_t operator()(const leanstore::UpdateSameSizeInPlaceDescriptor::Slot& slot) const
   {
      size_t result = (slot.length << 16) | slot.offset;
      return result;
   }
};
}  // namespace std
// -------------------------------------------------------------------------------------
namespace leanstore::storage::btree
{
// -------------------------------------------------------------------------------------
void BTreeVI::FatTupleDifferentAttributes::undoLastUpdate()
{
   ensure(deltas_count >= 1);
   auto& delta = *reinterpret_cast<Delta*>(payload + value_length);
   worker_id = delta.worker_id;
   worker_commit_mark = delta.worker_commit_mark;
   deltas_count -= 1;
   const u32 total_freed_space = sizeof(Delta) + delta.getDescriptor().size() + delta.getDescriptor().diffLength();
   BTreeLL::applyDiff(delta.getDescriptor(), getValue(), delta.payload + delta.getDescriptor().size());
   {
      const u32 dest = value_length;
      const u32 src = value_length + total_freed_space;
      const u32 bytes_to_move = used_space - src;
      std::memmove(payload + dest, payload + src, bytes_to_move);
   }
   used_space -= total_freed_space;
}
// -------------------------------------------------------------------------------------
void BTreeVI::FatTupleDifferentAttributes::garbageCollection(BTreeVI& btree)
{
   u32 offset = value_length, delta_i = 0;
   auto delta = reinterpret_cast<Delta*>(payload + offset);
   const bool pgc = FLAGS_pgc && deltas_count >= FLAGS_vi_pgc_batch_size &&
                    !(worker_id == cr::Worker::my().workerID() && worker_commit_mark == cr::activeTX().TTS());
   // -------------------------------------------------------------------------------------
   if (deltas_count > 1 && cr::Worker::my().isVisibleForAll(delta->committed_before_sat)) {
      const u16 removed_deltas = deltas_count - 1;
      used_space = value_length + sizeof(Delta) + delta->getDescriptor().size() +
                   delta->getDescriptor().diffLength();  // Delete everything after the first delta
      deltas_count = 1;
      debug = 500;
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += removed_deltas; }
   } else if (pgc) {
      // -------------------------------------------------------------------------------------
      // Precise garbage collection
      // Note: we can't use so ordering to decide whether to remove a version
      // SO ordering helps in one case, if it tells visible then it is and nothing else
      // Gonna get complicated here
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc[btree.dt_id]++; }
      cr::Worker::my().sortWorkers();
      u64 other_worker_index = 0;  // In the sorted array
      u64 other_worker_id = cr::Worker::my().local_workers_sta_sorted[other_worker_index] & cr::Worker::WORKERS_MASK;
      bool needs_the_loop = true;
      auto is_visible_to_it_optimized = [&](const u64 w_id, const u64 so) { return (cr::Worker::my().local_workers_sta[w_id]) > so; };
      // -------------------------------------------------------------------------------------
      delta_i = 0;
      offset = value_length;
      std::vector<Delta*> deltas_to_merge;
      // other_worker_index does not see the main version and current_version_offset points to the first delta
      while (needs_the_loop) {
         if (is_visible_to_it_optimized(other_worker_id, delta->committed_before_sat) ||
             cr::Worker::my().isVisibleForIt(other_worker_id, delta->worker_id, delta->worker_commit_mark)) {
            if (deltas_to_merge.size()) {
               // Merge all deltas in deltas_to_merge in delta*
               using Slot = UpdateSameSizeInPlaceDescriptor::Slot;
               std::unordered_map<Slot, std::basic_string<u8>> slots_map;
               for (auto m_delta : deltas_to_merge) {
                  auto& m_descriptor = m_delta->getDescriptor();
                  u8* delta_diff_ptr = m_delta->payload + m_descriptor.size();
                  for (u64 s_i = 0; s_i < m_delta->getDescriptor().count; s_i++) {
                     slots_map[m_descriptor.slots[s_i]] = std::basic_string<u8>(delta_diff_ptr, m_descriptor.slots[s_i].length);
                     delta_diff_ptr += m_descriptor.slots[s_i].length;
                  }
               }
               u8 merge_result[PAGE_SIZE];
               auto& merge_delta = *reinterpret_cast<Delta*>(merge_result);
               merge_delta = *delta;
               UpdateSameSizeInPlaceDescriptor& merge_descriptor = merge_delta.getDescriptor();
               merge_delta.getDescriptor().count = slots_map.size();
               u8* merge_diff_ptr = merge_delta.payload + merge_descriptor.size();
               u32 s_i = 0;
               for (auto& slot_itr : slots_map) {
                  merge_descriptor.slots[s_i++] = slot_itr.first;
                  std::memcpy(merge_diff_ptr, slot_itr.second.c_str(), slot_itr.second.size());
                  merge_diff_ptr += slot_itr.second.size();
               }
               const u32 total_merge_delta_size = merge_diff_ptr - merge_result;
               const u32 space_to_replace = (reinterpret_cast<u8*>(delta) - reinterpret_cast<u8*>(deltas_to_merge.front())) + sizeof(Delta) +
                                            delta->getDescriptor().size() + delta->getDescriptor().diffLength();
               ensure(space_to_replace >= total_merge_delta_size);
               const u16 copy_dst = reinterpret_cast<u8*>(deltas_to_merge.front()) - payload;
               std::memcpy(payload + copy_dst, merge_result, total_merge_delta_size);
               std::memmove(payload + copy_dst + total_merge_delta_size, payload + copy_dst + space_to_replace,
                            used_space - (copy_dst + space_to_replace));
               used_space -= space_to_replace - total_merge_delta_size;
               // -------------------------------------------------------------------------------------
               delta_i -= deltas_to_merge.size();
               deltas_count -= deltas_to_merge.size();
               offset = copy_dst;
               delta = deltas_to_merge.front();
               // -------------------------------------------------------------------------------------
               deltas_to_merge.clear();
            } else {
               if (++other_worker_index < cr::Worker::my().workers_count) {
                  other_worker_id = cr::Worker::my().local_workers_sta_sorted[other_worker_index] & cr::Worker::WORKERS_MASK;
                  explainWhen(other_worker_id == 255);
                  continue;
               } else {
                  break;
               }
            }
         } else {
            deltas_to_merge.push_back(delta);
            // Next version
            delta_i++;
            offset += sizeof(Delta) + delta->getDescriptor().size() + delta->getDescriptor().diffLength();
            delta = reinterpret_cast<Delta*>(payload + offset);
            if (delta_i < deltas_count) {
               continue;
            } else {
               break;
            }
         }
      }
      // -------------------------------------------------------------------------------------
      if (other_worker_index == cr::Worker::my().workers_count) {
         // Means that the current delta is seen by all, prune everything after delta*
         const u16 removed_deltas = deltas_count - (delta_i + 1);
         assert(removed_deltas < deltas_count);
         deltas_count -= removed_deltas;
         const u32 end_offset = offset + sizeof(Delta) + delta->getDescriptor().size() + delta->getDescriptor().diffLength();
         assert(end_offset <= used_space);
         used_space = end_offset;
      } else if (deltas_to_merge.size()) {
         assert(delta_i == deltas_count);
         // Prune everything starting from the first entry in the vector
         used_space = reinterpret_cast<u8*>(deltas_to_merge.front()) - payload;
         deltas_count -= deltas_to_merge.size();
      }
   }
   explainWhen(deltas_count > (cr::Worker::my().workers_count + 2));
}
// -------------------------------------------------------------------------------------
// Pre: tuple is write locked
bool BTreeVI::FatTupleDifferentAttributes::update(BTreeExclusiveIterator& iterator,
                                                  u8* o_key,
                                                  u16 o_key_length,
                                                  function<void(u8* value, u16 value_size)> cb,
                                                  UpdateSameSizeInPlaceDescriptor& update_descriptor,
                                                  BTreeVI& btree)
{
   auto fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(iterator.mutableValue().data());
   if (FLAGS_vi_fupdate_fat_tuple) {
      cb(fat_tuple->getValue(), fat_tuple->value_length);
      return true;
   }
   // -------------------------------------------------------------------------------------
   // Attention: we have to disable garbage collection if the latest delta was from us and not committed yet!
   // Otherwise we would crash during undo although the end result is the same if the transaction would commit (overwrite)
   const u32 descriptor_and_diff_length = update_descriptor.size() + update_descriptor.diffLength();
   const u32 needed_space = sizeof(Delta) + descriptor_and_diff_length;
   if (fat_tuple->deltas_count > 0) {
      // Garbage collection first
      fat_tuple->garbageCollection(btree);
   }
   ensure((needed_space + fat_tuple->used_space) <= 3 * 1024);  // TODO:
   if (fat_tuple->total_space < (needed_space + fat_tuple->used_space)) {
      const u32 fat_tuple_length = needed_space + fat_tuple->used_space + sizeof(FatTupleDifferentAttributes);
      assert(fat_tuple_length < PAGE_SIZE);
      u8 buffer[PAGE_SIZE];
      std::memcpy(buffer, iterator.value().data(), iterator.value().length());
      iterator.extendPayload(fat_tuple_length);
      std::memcpy(iterator.mutableValue().data(), buffer, fat_tuple_length);
      fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(iterator.mutableValue().data());
      fat_tuple->total_space = needed_space + fat_tuple->used_space;
   }
   if (fat_tuple->deltas_count > 0) {
      // Make place for a new delta
      const u64 src = fat_tuple->value_length;
      const u64 dst = fat_tuple->value_length + needed_space;
      std::memmove(fat_tuple->payload + dst, fat_tuple->payload + src, fat_tuple->used_space - src);
   }
   // -------------------------------------------------------------------------------------
   {
      // Insert the new delta
      auto& new_delta = *new (fat_tuple->payload + fat_tuple->value_length) Delta();
      new_delta.worker_id = fat_tuple->worker_id;
      new_delta.worker_commit_mark = fat_tuple->worker_commit_mark;
      // Attention: we should not timestamp a delta that we created as committed!
      if (fat_tuple->worker_id == cr::Worker::my().workerID() && fat_tuple->worker_commit_mark == cr::activeTX().TTS()) {
         new_delta.committed_before_sat = std::numeric_limits<u64>::max();
      } else {
         new_delta.committed_before_sat = cr::Worker::my().snapshotAcquistionTime();
      }
      std::memcpy(new_delta.payload, &update_descriptor, update_descriptor.size());
      BTreeLL::generateDiff(update_descriptor, new_delta.payload + update_descriptor.size(), fat_tuple->getValue());
      fat_tuple->used_space += needed_space;
      fat_tuple->deltas_count++;
   }
   ensure(fat_tuple->total_space >= fat_tuple->used_space);
   // -------------------------------------------------------------------------------------
   {
      // WAL
      const u16 delta_and_descriptor_size = update_descriptor.size() + update_descriptor.diffLength();
      auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
      wal_entry->type = WAL_LOG_TYPE::WALUpdate;
      wal_entry->key_length = o_key_length;
      wal_entry->delta_length = delta_and_descriptor_size;
      wal_entry->before_worker_id = fat_tuple->worker_id;
      wal_entry->before_worker_commit_mark = fat_tuple->worker_commit_mark;
      fat_tuple->worker_id = cr::Worker::my().workerID();
      fat_tuple->worker_commit_mark = cr::activeTX().TTS();
      wal_entry->after_worker_id = fat_tuple->worker_id;
      wal_entry->after_worker_commit_mark = fat_tuple->worker_commit_mark;
      std::memcpy(wal_entry->payload, o_key, o_key_length);
      std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
      // Update the value in-place
      BTreeLL::generateDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), fat_tuple->getValue());
      cb(fat_tuple->getValue(), fat_tuple->value_length);
      fat_tuple->debug = 1;
      BTreeLL::generateXORDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), fat_tuple->getValue());
      wal_entry.submit();
      // -------------------------------------------------------------------------------------
      if (cr::activeTX().isSerializable()) {
         fat_tuple->read_ts = cr::activeTX().TTS();
      }
   }
   return true;
}
// -------------------------------------------------------------------------------------
std::tuple<OP_RESULT, u16> BTreeVI::FatTupleDifferentAttributes::reconstructTuple(std::function<void(Slice value)> cb) const
{
   if (cr::Worker::my().isVisibleForMe(worker_id, worker_commit_mark)) {
      // Latest version is visible
      cb(Slice(getValueConstant(), value_length));
      return {OP_RESULT::OK, 1};
   } else if (deltas_count > 0) {
      u8 materialized_value[value_length];
      std::memcpy(materialized_value, getValueConstant(), value_length);
      // we have to apply the diffs
      u16 delta_i = 0;
      u32 offset = value_length;
      auto delta = reinterpret_cast<const Delta*>(payload + offset);
      while (delta_i < deltas_count) {
         if (cr::Worker::my().isVisibleForMe(delta->worker_id, delta->worker_commit_mark)) {
            BTreeLL::applyDiff(delta->getConstantDescriptor(), materialized_value,
                               delta->payload + delta->getConstantDescriptor().size());  // Apply diff
            cb(Slice(materialized_value, value_length));
            return {OP_RESULT::OK, delta_i + 2};
         }
         // -------------------------------------------------------------------------------------
         delta_i++;
         offset += sizeof(Delta) + delta->getConstantDescriptor().size() + delta->getConstantDescriptor().diffLength();
         delta = reinterpret_cast<const Delta*>(payload + offset);
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
bool BTreeVI::convertChainedToFatTupleDifferentAttributes(BTreeExclusiveIterator& iterator, MutableSlice& m_key)
{
   Slice key(m_key.data(), m_key.length());
   u16 number_of_deltas_to_replace = 0;
   std::vector<u8> dynamic_buffer;
   dynamic_buffer.resize(PAGE_SIZE * 4);
   u8* fat_tuple_payload = dynamic_buffer.data();
   ChainSN next_sn;
   auto& fat_tuple = *new (fat_tuple_payload) FatTupleDifferentAttributes();
   fat_tuple.total_space = 3 * 1024;
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
      fat_tuple.worker_commit_mark = chain_head.worker_commit_mark;
      // -------------------------------------------------------------------------------------
      next_sn = chain_head.next_sn;
   }
   // TODO: check for used_space overflow
   {
      // Iterate over the rest
      std::set<ChainSN> processed_sns;
      while (next_sn != 0) {
         number_of_deltas_to_replace++;
         const bool next_higher = next_sn >= getSN(m_key);
         setSN(m_key, next_sn);
         OP_RESULT ret = iterator.seekExactWithHint(key, next_higher);
         if (ret != OP_RESULT::OK) {
            break;
         }
         processed_sns.insert(next_sn);
         // -------------------------------------------------------------------------------------
         const Slice delta_slice = iterator.value();
         const auto& chain_delta = *reinterpret_cast<const ChainedTupleVersion*>(delta_slice.data());
         // -------------------------------------------------------------------------------------
         const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(chain_delta.payload);
         const u32 descriptor_and_diff_length = update_descriptor.size() + update_descriptor.diffLength();
         const u32 needed_space = sizeof(FatTupleDifferentAttributes::Delta) + descriptor_and_diff_length;
         // -------------------------------------------------------------------------------------
         if ((fat_tuple.used_space + sizeof(FatTupleDifferentAttributes::Delta) + needed_space) >= dynamic_buffer.size()) {
            dynamic_buffer.resize(fat_tuple.used_space + sizeof(FatTupleDifferentAttributes::Delta) + needed_space);
         }
         // -------------------------------------------------------------------------------------
         // Add a FatTuple::Delta
         auto& new_delta = *new (fat_tuple.payload + fat_tuple.used_space) FatTupleDifferentAttributes::Delta();
         fat_tuple.used_space += sizeof(FatTupleDifferentAttributes::Delta);
         new_delta.worker_id = chain_delta.worker_id;
         new_delta.worker_commit_mark = chain_delta.worker_commit_mark;
         new_delta.committed_before_sat = chain_delta.committed_before_sat;
         // -------------------------------------------------------------------------------------
         // Copy Descriptor + Diff
         std::memcpy(fat_tuple.payload + fat_tuple.used_space, &update_descriptor, descriptor_and_diff_length);
         fat_tuple.used_space += descriptor_and_diff_length;
         // -------------------------------------------------------------------------------------
         fat_tuple.deltas_count++;
         next_sn = chain_delta.next_sn;
         fat_tuple.garbageCollection(*this);  // TODO: temporary hack to calm down overflow bugs
         // -------------------------------------------------------------------------------------
         if (processed_sns.count(next_sn)) {
            break;
         }
      }
   }
   {
      setSN(m_key, 0);
      OP_RESULT ret = iterator.seekExactWithHint(key, false);
      ensure(ret == OP_RESULT::OK);
   }
   auto& chain_head = *reinterpret_cast<ChainedTuple*>(iterator.mutableValue().data());
   if (fat_tuple.used_space > fat_tuple.total_space) {
      chain_head.unlock();
      return false;
   }
   fat_tuple.total_space = fat_tuple.used_space;
   if (number_of_deltas_to_replace >= cr::Worker::my().workers_count) {
      // Finalize the new FatTuple
      // TODO: corner cases, more careful about space usage
      // -------------------------------------------------------------------------------------
      setSN(m_key, 0);
      OP_RESULT ret = iterator.seekExactWithHint(key, false);
      ensure(ret == OP_RESULT::OK);
      ensure(reinterpret_cast<Tuple*>(iterator.mutableValue().data())->isWriteLocked());
      // -------------------------------------------------------------------------------------
      ensure(fat_tuple.total_space >= fat_tuple.used_space);
      const u16 fat_tuple_length = sizeof(FatTupleDifferentAttributes) + fat_tuple.total_space;
      if (iterator.value().length() < fat_tuple_length) {
         iterator.extendPayload(fat_tuple_length);
      } else {
         iterator.shorten(fat_tuple_length);
      }
      std::memcpy(iterator.mutableValue().data(), fat_tuple_payload, fat_tuple_length);
      return true;
   } else {
      chain_head.unlock();
      return false;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore::storage::btree
