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
   tx_ts = delta.tx_ts;
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
void BTreeVI::FatTupleDifferentAttributes::garbageCollection(BTreeVI& btree, bool heavyweight)
{
   if (deltas_count == 0) {
      return;
   }
   if (deltas_count >= cr::Worker::my().workers_count) {
      heavyweight = true;
   }
   // -------------------------------------------------------------------------------------
   const bool pgc = (FLAGS_pgc) && deltas_count >= 1;
   // -------------------------------------------------------------------------------------
   // assert(this->tx_ts & MSB);
   if (cr::Worker::my().isVisibleForAll(worker_id, tx_ts)) {
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += deltas_count; }
      used_space = value_length;
      deltas_count = 0;
   } else if (pgc) {
      cr::Worker::my().prepareForIntervalGC();
      tx_ts = cr::Worker::my().getCommitTimestamp(worker_id, tx_ts) | MSB;
      // -------------------------------------------------------------------------------------
      u64 w_s_i = 0, w_i = cr::Worker::my().local_workers_sorted_start_ts[w_s_i] & cr::Worker::WORKERS_MASK;
      u64 delta_i = 0;
      auto delta = reinterpret_cast<Delta*>(payload + value_length);
      delta->tx_ts = cr::Worker::my().getCommitTimestamp(delta->worker_id, delta->tx_ts) | MSB;
      u64 offset = value_length;
      std::vector<Delta*> deltas_to_merge;
      cr::Worker::VISIBILITY visibility;
      while (true) {
         visibility = cr::Worker::my().isVisibleForIt(w_i, worker_id, tx_ts);
         if (visibility == cr::Worker::VISIBILITY::VISIBLE_ALREADY) {
            // Nothing
            if (++w_s_i < cr::Worker::my().workers_count) {
               w_i = cr::Worker::my().local_workers_sorted_start_ts[w_s_i] & cr::Worker::WORKERS_MASK;
            } else {
               break;
            }
         } else if (visibility == cr::Worker::VISIBILITY::VISIBLE_NEXT_ROUND) {
            deltas_to_merge.push_back(nullptr);
            break;
         } else {
            UNREACHABLE();
         }
      }
      while (w_s_i < cr::Worker::my().workers_count && delta_i < deltas_count) {
         if (delta->worker_id == cr::Worker::my().workerID() && delta->tx_ts == cr::activeTX().TTS()) {
            deltas_to_merge.clear();
            break;
         }
         visibility = cr::Worker::my().isVisibleForIt(w_i, delta->worker_id, delta->tx_ts);
         if (visibility == cr::Worker::VISIBILITY::VISIBLE_NEXT_ROUND) {
            // TODO: add to delete
            deltas_to_merge.push_back(delta);
            // TODO: get next version
            delta_i++;
            offset += sizeof(Delta) + delta->getDescriptor().size() + delta->getDescriptor().diffLength();
            if (delta_i < deltas_count) {
               delta = reinterpret_cast<Delta*>(payload + offset);
            }
         } else if (visibility == cr::Worker::VISIBILITY::VISIBLE_ALREADY) {
            // TODO: merge everything in-between
            if (deltas_to_merge.size() > 1) {
               deltas_to_merge.erase(deltas_to_merge.begin());
               // Merge all deltas in deltas_to_merge in delta*
               // Check whether all attributes are the same
               bool same_attributes = true;
               UpdateSameSizeInPlaceDescriptor& last_descriptor = delta->getDescriptor();
               for (u64 d_i = 0; same_attributes && d_i < deltas_to_merge.size(); d_i++) {
                  same_attributes &= last_descriptor == deltas_to_merge[d_i]->getDescriptor();
               }
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += deltas_to_merge.size(); }
               if (same_attributes) {
                  ensure(used_space >= (reinterpret_cast<u8*>(delta) - payload));
                  std::memmove(deltas_to_merge.front(), delta, used_space - (reinterpret_cast<u8*>(delta) - payload));
                  const u64 freed_space = reinterpret_cast<u8*>(delta) - reinterpret_cast<u8*>(deltas_to_merge.front());
                  deltas_count -= deltas_to_merge.size();
                  delta_i -= deltas_to_merge.size();
                  used_space -= freed_space;
                  delta = deltas_to_merge.front();
                  offset = reinterpret_cast<u8*>(delta) - payload;
                  deltas_to_merge.clear();
               } else {
                  using Slot = UpdateSameSizeInPlaceDescriptor::Slot;
                  std::unordered_map<Slot, std::basic_string<u8>> slots_map;
                  for (auto d_iter = deltas_to_merge.rbegin(); d_iter != deltas_to_merge.rend(); d_iter++) {
                     auto& m_delta = *d_iter;
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
                  merge_descriptor.count = slots_map.size();
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
               }
            } else {
               // The previous one was not visible but this one is
               deltas_to_merge.clear();
            }
            w_s_i++;
            if (w_s_i == cr::Worker::my().workers_count) {
               break;
            }
            w_i = cr::Worker::my().local_workers_sorted_start_ts[w_s_i] & cr::Worker::WORKERS_MASK;
         } else {
            ensure(false);
         }
      }
      // -------------------------------------------------------------------------------------
      if (deltas_to_merge.size() > 1) {
         deltas_to_merge.erase(deltas_to_merge.begin());
         used_space = reinterpret_cast<u8*>(deltas_to_merge.front()) - payload;
         deltas_count -= deltas_to_merge.size();
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[btree.dt_id] += deltas_to_merge.size(); }
      }
   }
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
   bool heavyweight_gc = false;
cont : {
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
   if (fat_tuple->deltas_count > 0) {  // fat_tuple->total_space < (needed_space + fat_tuple->used_space) &&
                                       // Garbage collection first
      fat_tuple->garbageCollection(btree, heavyweight_gc);
   }
   if (fat_tuple->total_space < (needed_space + fat_tuple->used_space)) {
      const u32 fat_tuple_length = needed_space + fat_tuple->used_space + sizeof(FatTupleDifferentAttributes);
      u8 buffer[PAGE_SIZE];
      ensure(iterator.value().length() <= PAGE_SIZE);
      std::memcpy(buffer, iterator.value().data(), iterator.value().length());
      // -------------------------------------------------------------------------------------
      const bool did_extend = iterator.extendPayload(fat_tuple_length);
      if (did_extend == false) {
         if (heavyweight_gc == true) {
            TODOException();  // TODO; Fallback to chained (decompose)
         }
         heavyweight_gc = true;
         goto cont;
      }
      // -------------------------------------------------------------------------------------
      std::memcpy(iterator.mutableValue().data(), buffer, fat_tuple_length);
      fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(iterator.mutableValue().data());  // ATTENTION
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
      new_delta.tx_ts = fat_tuple->tx_ts;
      new_delta.command_id = fat_tuple->command_id;
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
   }
   assert(fat_tuple->deltas_count > 0);
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
      u16 delta_i = 0;
      u32 offset = value_length;
      auto delta = reinterpret_cast<const Delta*>(payload + offset);
      while (delta_i < deltas_count) {
         if (cr::Worker::my().isVisibleForMe(delta->worker_id, delta->tx_ts)) {
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
      explainWhen(cr::activeTX().isOLTP());
      return {OP_RESULT::NOT_FOUND, delta_i + 1};
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
   dynamic_buffer.resize(PAGE_SIZE * 4);
   auto fat_tuple = new (dynamic_buffer.data()) FatTupleDifferentAttributes();
   fat_tuple->total_space = EFFECTIVE_PAGE_SIZE * 3.0 / 4.0;  // TODO: set a proper maximum
   fat_tuple->used_space = 0;
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
   while (true) {
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
             if ((fat_tuple->used_space + sizeof(FatTupleDifferentAttributes) + needed_space) >= dynamic_buffer.size()) {
                dynamic_buffer.resize(fat_tuple->used_space + sizeof(FatTupleDifferentAttributes) + needed_space);
                fat_tuple = reinterpret_cast<FatTupleDifferentAttributes*>(dynamic_buffer.data());
             }
             // -------------------------------------------------------------------------------------
             // Add a FatTuple::Delta
             auto& new_delta = *new (fat_tuple->payload + fat_tuple->used_space) FatTupleDifferentAttributes::Delta();
             fat_tuple->used_space += sizeof(FatTupleDifferentAttributes::Delta);
             new_delta.worker_id = chain_delta.worker_id;
             new_delta.tx_ts = chain_delta.tx_id;
             new_delta.command_id = chain_delta.command_id;
             // -------------------------------------------------------------------------------------
             // Copy Descriptor + Diff
             std::memcpy(fat_tuple->payload + fat_tuple->used_space, &update_descriptor, descriptor_and_diff_length);
             fat_tuple->deltas_count++;
             fat_tuple->used_space += descriptor_and_diff_length;
             // -------------------------------------------------------------------------------------
             next_worker_id = chain_delta.worker_id;
             next_tx_id = chain_delta.tx_id;
             next_command_id = chain_delta.command_id;
             fat_tuple->garbageCollection(*this, true);
          })) {
         break;
      }
   }
   if (fat_tuple->used_space > fat_tuple->total_space) {
      chain_head.unlock();
      raise(SIGTRAP);
      return false;
   }
   fat_tuple->total_space = fat_tuple->used_space;
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
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore::storage::btree
