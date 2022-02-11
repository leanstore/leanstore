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
   auto& delta = getDelta(deltas_count - 1);
   worker_id = delta.worker_id;
   tx_ts = delta.tx_ts;
   deltas_count -= 1;
   const u32 delta_total_length = delta.totalLength();
   data_offset += delta_total_length;
   used_space -= delta_total_length + sizeof(u16);
   BTreeLL::applyDiff(delta.getDescriptor(), getValue(), delta.payload + delta.getDescriptor().size());
}
// -------------------------------------------------------------------------------------
// Attention: we have to disable garbage collection if the latest delta was from us and not committed yet!
// Otherwise we would crash during undo although the end result is the same if the transaction would commit (overwrite)
void BTreeVI::FatTupleDifferentAttributes::garbageCollection()
{
   if (deltas_count == 0) {
      return;
   }
   // -------------------------------------------------------------------------------------
   // Delete for all visible deltas, atm using cheap visibility check
   if (cr::Worker::my().isVisibleForAll(worker_id, tx_ts)) {
      deltas_count = 0;
      data_offset = total_space;
      used_space = value_length;
      return;
   }
   u16 deltas_visible_by_all_counter = 0;
   u32 freed_space = 0;
   for (u32 d_i = 0; (1 + d_i) < deltas_count; d_i++) {
      auto& delta = getDelta(d_i + 1);
      if (cr::Worker::my().isVisibleForAll(delta.worker_id, delta.tx_ts)) {
         deltas_visible_by_all_counter++;
         freed_space += delta.totalLength();
      } else {
         break;
      }
   }
   if (deltas_visible_by_all_counter) {
      used_space -= (sizeof(u16) * deltas_visible_by_all_counter) + freed_space;
      std::memmove(payload + data_offset + freed_space, payload + data_offset, (total_space - data_offset - freed_space));
      data_offset += freed_space;
      std::memmove(getDeltaOffsets(), getDeltaOffsets() + deltas_visible_by_all_counter,
                   (deltas_count - deltas_visible_by_all_counter) * sizeof(u16));
      deltas_count -= deltas_visible_by_all_counter;
      for (u32 d_i = 0; d_i < deltas_count; d_i++) {
         getDeltaOffsets()[d_i] += freed_space;
      }
   }
   // -------------------------------------------------------------------------------------
   // Identify tuples we should merge
   u16 deltas_to_merge[deltas_count];
   u16 deltas_to_merge_counter = 0;
   for (u32 d_i = 0; d_i < deltas_count; d_i++) {
      auto& delta = getDelta(d_i);
      if (delta.tx_ts >= cr::Worker::my().global_oldest_oltp) {
         break;
      }
      if (delta.tx_ts > cr::Worker::my().global_newest_olap) {
         deltas_to_merge[deltas_to_merge_counter++] = d_i;
      }
   }
   if (deltas_to_merge_counter <= 1) {
      return;
   } else {
      deltas_to_merge_counter--;
   }
   // -------------------------------------------------------------------------------------
   u32 buffer_size = total_space + sizeof(FatTupleDifferentAttributes);
   u8 buffer[buffer_size];
   auto& new_fat_tuple = *new (buffer) FatTupleDifferentAttributes(total_space);
   new_fat_tuple.worker_id = worker_id;
   new_fat_tuple.tx_ts = tx_ts;
   new_fat_tuple.used_space += value_length;
   new_fat_tuple.value_length = value_length;
   std::memcpy(new_fat_tuple.payload, payload, value_length);  // Copy value
   // -------------------------------------------------------------------------------------
   auto append_ll = [](FatTupleDifferentAttributes& fat_tuple, u8* delta, u16 delta_length) {
      assert(fat_tuple.total_space >= (fat_tuple.used_space + delta_length + sizeof(u16)));
      const u16 d_i = fat_tuple.deltas_count++;
      fat_tuple.used_space += delta_length + sizeof(u16);
      fat_tuple.data_offset -= delta_length;
      fat_tuple.getDeltaOffsets()[d_i] = fat_tuple.data_offset;
      std::memcpy(fat_tuple.payload + fat_tuple.data_offset, delta, delta_length);
   };
   u16 zone_begin = deltas_to_merge[0], zone_end = deltas_to_merge[deltas_to_merge_counter];  // [zone_begin, zone_end)
   for (u32 d_i = 0; d_i < zone_begin; d_i++) {
      auto& delta = getDelta(d_i);
      append_ll(new_fat_tuple, reinterpret_cast<u8*>(&delta), delta.totalLength());
   }
   // -------------------------------------------------------------------------------------
   // TODO:
   // Merge from newest to oldest, i.e., from end of array into beginning
   using Slot = UpdateSameSizeInPlaceDescriptor::Slot;
   std::unordered_map<Slot, std::basic_string<u8>> slots_map;
   for (s32 d_i = zone_end - 1; d_i >= zone_begin; d_i--) {
      auto& delta_i = getDelta(d_i);
      auto& descriptor_i = delta_i.getDescriptor();
      u8* delta_diff_ptr = delta_i.payload + descriptor_i.size();
      for (s16 s_i = 0; s_i < descriptor_i.count; s_i++) {
         slots_map[descriptor_i.slots[s_i]] = std::basic_string<u8>(delta_diff_ptr, descriptor_i.slots[s_i].length);
         delta_diff_ptr += descriptor_i.slots[s_i].length;
         // s16 match_i = -1;
         // u16 offset = 0;
         // for (s16 m_s_i = 0; m_s_i < merge_delta.getDescriptor().count; m_s_i++) {
         //    auto& m_slot = merge_delta.getDescriptor().slots[m_s_i];
         //    if (m_slot == delta_i.getDescriptor().slots[o_i]) {
         //       match_i = m_s_i;
         //       break;
         //    } else {
         //       offset += m_slot.length;
         //    }
         // }
         // if (match_i != -1) {
         // }
      }
   }
   u32 new_delta_total_length =
       sizeof(Delta) + sizeof(UpdateSameSizeInPlaceDescriptor) + (sizeof(UpdateSameSizeInPlaceDescriptor::Slot) * slots_map.size());
   for (auto& slot_itr : slots_map) {
      new_delta_total_length += slot_itr.second.size();
   }
   auto& merge_delta = new_fat_tuple.allocateDelta(new_delta_total_length);
   merge_delta = getDelta(zone_begin);
   UpdateSameSizeInPlaceDescriptor& merge_descriptor = merge_delta.getDescriptor();
   merge_descriptor.count = slots_map.size();
   u8* merge_diff_ptr = merge_delta.payload + merge_descriptor.size();
   u32 s_i = 0;
   for (auto& slot_itr : slots_map) {
      merge_descriptor.slots[s_i++] = slot_itr.first;
      std::memcpy(merge_diff_ptr, slot_itr.second.c_str(), slot_itr.second.size());
      merge_diff_ptr += slot_itr.second.size();
   }
   // -------------------------------------------------------------------------------------
   for (u32 d_i = zone_end; d_i < deltas_count; d_i++) {
      auto& delta = getDelta(d_i);
      append_ll(new_fat_tuple, reinterpret_cast<u8*>(&delta), delta.totalLength());
   }
   // -------------------------------------------------------------------------------------
   std::memcpy(this, buffer, buffer_size);
   assert(total_space >= used_space);
   // -------------------------------------------------------------------------------------
   DEBUG_BLOCK()
   {
      u32 should_used_space = value_length;
      for (u32 d_i = 0; d_i < deltas_count; d_i++) {
         should_used_space += sizeof(u16) + getDelta(d_i).totalLength();
      }
      assert(used_space == should_used_space);
   }
}  // namespace leanstore::storage::btree
// -------------------------------------------------------------------------------------
bool BTreeVI::FatTupleDifferentAttributes::hasSpaceFor(const UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   const u32 needed_space = update_descriptor.size() + update_descriptor.diffLength() + sizeof(u16) + sizeof(Delta);
   return (data_offset - (value_length + (deltas_count * sizeof(u16)))) >= needed_space;
}
// -------------------------------------------------------------------------------------
BTreeVI::FatTupleDifferentAttributes::Delta& BTreeVI::FatTupleDifferentAttributes::allocateDelta(u32 delta_total_length)
{
   assert((total_space - used_space) >= (delta_total_length + sizeof(u16)));
   used_space += delta_total_length + sizeof(u16);
   data_offset -= delta_total_length;
   const u32 delta_i = deltas_count++;
   getDeltaOffsets()[delta_i] = data_offset;
   return *new (&getDelta(delta_i)) Delta();
}
// -------------------------------------------------------------------------------------
// Caller should take care of WAL
void BTreeVI::FatTupleDifferentAttributes::append(UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   const u32 delta_total_length = update_descriptor.size() + update_descriptor.diffLength() + sizeof(Delta);
   auto& new_delta = allocateDelta(delta_total_length);
   new_delta.worker_id = this->worker_id;
   new_delta.tx_ts = this->tx_ts;
   new_delta.command_id = this->command_id;
   std::memcpy(new_delta.payload, &update_descriptor, update_descriptor.size());
   BTreeLL::generateDiff(update_descriptor, new_delta.payload + update_descriptor.size(), this->getValue());
}
// -------------------------------------------------------------------------------------
// Pre: tuple is write locked
bool BTreeVI::FatTupleDifferentAttributes::update(BTreeExclusiveIterator& iterator,
                                                  u8* o_key,
                                                  u16 o_key_length,
                                                  function<void(u8* value, u16 value_size)> cb,
                                                  UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
cont : {
   auto fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(iterator.mutableValue().data());
   if (FLAGS_vi_fupdate_fat_tuple) {
      cb(fat_tuple->getValue(), fat_tuple->value_length);
      return true;
   }
   // -------------------------------------------------------------------------------------
   if (fat_tuple->hasSpaceFor(update_descriptor)) {
      fat_tuple->append(update_descriptor);
      // WAL
      const u16 delta_and_descriptor_size = update_descriptor.size() + update_descriptor.diffLength();
      auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
      wal_entry->type = WAL_LOG_TYPE::WALUpdate;
      wal_entry->key_length = o_key_length;
      wal_entry->delta_length = delta_and_descriptor_size;
      wal_entry->before_worker_id = fat_tuple->worker_id;
      wal_entry->before_tx_id = fat_tuple->tx_ts;
      // -------------------------------------------------------------------------------------
      fat_tuple->worker_id = cr::Worker::my().workerID();
      fat_tuple->tx_ts = cr::activeTX().TTS();
      fat_tuple->command_id = cr::Worker::my().command_id++;  // A version is not inserted in versions space however. Needed for decompose
      // -------------------------------------------------------------------------------------
      std::memcpy(wal_entry->payload, o_key, o_key_length);
      std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
      // Update the value in-place
      BTreeLL::generateDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), fat_tuple->getValue());
      cb(fat_tuple->getValue(), fat_tuple->value_length);
      BTreeLL::generateXORDiff(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), fat_tuple->getValue());
      wal_entry.submit();
      // -------------------------------------------------------------------------------------
      if (cr::activeTX().isSerializable()) {
         fat_tuple->read_ts = cr::activeTX().TTS();
      }
   } else {
      fat_tuple->garbageCollection();
      if (fat_tuple->hasSpaceFor(update_descriptor)) {
         goto cont;
      } else {
         raise(SIGTRAP);
         fat_tuple->garbageCollection();
         TODOException();
         if (fat_tuple->total_space < maxFatTupleLength()) {
            const u32 new_fat_tuple_length = std::min<u32>(maxFatTupleLength(), fat_tuple->total_space * 2);
            u8 buffer[PAGE_SIZE];
            ensure(iterator.value().length() <= PAGE_SIZE);
            std::memcpy(buffer, iterator.value().data(), iterator.value().length());
            // -------------------------------------------------------------------------------------
            const bool did_extend = iterator.extendPayload(new_fat_tuple_length);
            ensure(did_extend);
            // -------------------------------------------------------------------------------------
            std::memcpy(iterator.mutableValue().data(), buffer, new_fat_tuple_length);
            fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(iterator.mutableValue().data());  // ATTENTION
            fat_tuple->total_space = new_fat_tuple_length - sizeof(FatTupleDifferentAttributes);
            goto cont;
         } else {
            TODOException();
         }
      }
   }
   assert(fat_tuple->total_space >= fat_tuple->used_space);
   return true;
}
}
// -------------------------------------------------------------------------------------
std::tuple<OP_RESULT, u16> BTreeVI::FatTupleDifferentAttributes::reconstructTuple(std::function<void(Slice value)> cb) const
{
   if (cr::Worker::my().isVisibleForMe(worker_id, tx_ts)) {
      // Latest version is visible
      cb(Slice(getValueConstant(), value_length));
      return {OP_RESULT::OK, 1};
   } else if (deltas_count > 0) {
      u8 materialized_value[value_length];
      std::memcpy(materialized_value, getValueConstant(), value_length);
      // we have to apply the diffs
      u16 chain_length = 2;
      for (s16 d_i = deltas_count - 1; d_i >= 0; d_i--) {
         auto& delta = getDeltaConstant(d_i);
         BTreeLL::applyDiff(delta.getConstantDescriptor(), materialized_value,
                            delta.payload + delta.getConstantDescriptor().size());  // Apply diff
         if (cr::Worker::my().isVisibleForMe(delta.worker_id, delta.tx_ts)) {
            cb(Slice(materialized_value, value_length));
            return {OP_RESULT::OK, chain_length};
         } else {
            chain_length++;
         }
      }
      // -------------------------------------------------------------------------------------
      explainWhen(cr::activeTX().isOLTP());
      return {OP_RESULT::NOT_FOUND, chain_length};
   } else {
      explainWhen(cr::activeTX().isOLTP());
      return {OP_RESULT::NOT_FOUND, 1};
   }
}
// -------------------------------------------------------------------------------------
bool BTreeVI::convertChainedToFatTupleDifferentAttributes(BTreeExclusiveIterator& iterator)
{
   u16 number_of_deltas_to_replace = 0;
   std::vector<u8> dynamic_buffer;
   dynamic_buffer.resize(maxFatTupleLength());
   auto fat_tuple = new (dynamic_buffer.data()) FatTupleDifferentAttributes(dynamic_buffer.size() - sizeof(FatTupleDifferentAttributes));
   // -------------------------------------------------------------------------------------
   WORKERID next_worker_id;
   TXID next_tx_id;
   COMMANDID next_command_id;
   // -------------------------------------------------------------------------------------
   // Process the chain head
   MutableSlice head = iterator.mutableValue();
   auto& chain_head = *reinterpret_cast<ChainedTuple*>(head.data());
   ensure(chain_head.isWriteLocked());
   // -------------------------------------------------------------------------------------
   fat_tuple->value_length = head.length() - sizeof(ChainedTuple);
   std::memcpy(fat_tuple->payload + fat_tuple->used_space, chain_head.payload, fat_tuple->value_length);
   fat_tuple->used_space += fat_tuple->value_length;
   fat_tuple->worker_id = chain_head.worker_id;
   fat_tuple->tx_ts = chain_head.tx_ts;
   // -------------------------------------------------------------------------------------
   next_worker_id = chain_head.worker_id;
   next_tx_id = chain_head.tx_ts;
   next_command_id = chain_head.command_id;
   // TODO: check for used_space overflow
   bool abort_conversion = false;
   while (!abort_conversion) {
      if (cr::Worker::my().isVisibleForAll(next_worker_id, next_tx_id)) {  // Pruning versions space might get delayed
         break;
      }
      // -------------------------------------------------------------------------------------
      if (!cr::Worker::my().retrieveVersion(next_worker_id, next_tx_id, next_command_id, [&](const u8* version, [[maybe_unused]] u64 payload_length) {
             number_of_deltas_to_replace++;
             const auto& chain_delta = *reinterpret_cast<const UpdateVersion*>(version);
             ensure(chain_delta.type == Version::TYPE::UPDATE);
             ensure(chain_delta.is_delta);
             const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(chain_delta.payload);
             const u32 descriptor_and_diff_length = update_descriptor.size() + update_descriptor.diffLength();
             const u32 needed_space = sizeof(FatTupleDifferentAttributes::Delta) + descriptor_and_diff_length;
             // -------------------------------------------------------------------------------------
             if (needed_space > 400) {
                abort_conversion = true;
                return;
             }
             if (!fat_tuple->hasSpaceFor(update_descriptor)) {
                fat_tuple->garbageCollection();
                if (!fat_tuple->hasSpaceFor(update_descriptor)) {
                   abort_conversion = true;
                   return;
                }
             }
             // -------------------------------------------------------------------------------------
             // Add a FatTuple::Delta
             auto& new_delta = fat_tuple->allocateDelta(needed_space);
             new_delta.worker_id = chain_delta.worker_id;
             new_delta.tx_ts = chain_delta.tx_id;
             new_delta.command_id = chain_delta.command_id;
             // -------------------------------------------------------------------------------------
             // Copy Descriptor + Diff
             std::memcpy(new_delta.payload, &update_descriptor, descriptor_and_diff_length);
             // -------------------------------------------------------------------------------------
             next_worker_id = chain_delta.worker_id;
             next_tx_id = chain_delta.tx_id;
             next_command_id = chain_delta.command_id;
          })) {
         break;
      }
   }
   if (abort_conversion) {
      chain_head.unlock();
      return false;
   }
   // -------------------------------------------------------------------------------------
   // Problem: chain order is from newest to oldest. FatTuple order is O2N
   // Solution: naive, reverse the order at the end
   std::reverse(fat_tuple->getDeltaOffsets(), fat_tuple->getDeltaOffsets() + fat_tuple->deltas_count);
   // -------------------------------------------------------------------------------------
   fat_tuple->garbageCollection();
   if (fat_tuple->used_space > maxFatTupleLength()) {
      chain_head.unlock();
      raise(SIGTRAP);
      return false;
   }
   assert(fat_tuple->total_space >= fat_tuple->used_space);
   // We can not simply change total_space because this affects data_offset
   if (number_of_deltas_to_replace > convertToFatTupleThreshold() && cr::Worker::my().local_olap_lwm != cr::Worker::my().local_oltp_lwm) {
      // Finalize the new FatTuple
      // TODO: corner cases, more careful about space usage
      // -------------------------------------------------------------------------------------
      ensure(fat_tuple->total_space >= fat_tuple->used_space);
      const u16 fat_tuple_length = sizeof(FatTupleDifferentAttributes) + fat_tuple->total_space;
      if (iterator.value().length() < fat_tuple_length) {
         ensure(reinterpret_cast<const Tuple*>(iterator.value().data())->tuple_format == TupleFormat::CHAINED);
         const bool did_extend = iterator.extendPayload(fat_tuple_length);
         ensure(did_extend);
      } else {
         iterator.shorten(fat_tuple_length);
      }
      std::memcpy(iterator.mutableValue().data(), dynamic_buffer.data(), fat_tuple_length);
      iterator.markAsDirty();
      return true;
   } else {
      chain_head.unlock();
      return false;
   }
}  // namespace leanstore::storage::btree
// -------------------------------------------------------------------------------------
}  // namespace leanstore::storage::btree
