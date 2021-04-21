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
DEFINE_bool(vi_flookup, false, "");
DEFINE_bool(vi_fremove, false, "");
DEFINE_bool(vi_fupdate, false, "");
DEFINE_uint64(vi_pgc_batch_size, 0, "");
DEFINE_bool(vi_pgc_so_method, false, "");
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
   // TODO: use optimistic latches for leaf 5K (optimistic scans)
   // -------------------------------------------------------------------------------------
   if (!FLAGS_vi_flookup) {
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
         [[maybe_unused]] const auto primary_version =
             *reinterpret_cast<const PrimaryVersion*>(iterator.value().data() + iterator.value().length() - sizeof(PrimaryVersion));
         auto reconstruct = reconstructTuple(iterator, key, [&](Slice value) { payload_callback(value.data(), value.length()); });
         COUNTERS_BLOCK()
         {
            WorkerCounters::myCounters().cc_read_chains[dt_id]++;
            WorkerCounters::myCounters().cc_read_versions_visited[dt_id]++;
         }
         ret = std::get<0>(reconstruct);
         if (ret != OP_RESULT::OK) {  // For debugging
            const auto& primary_version =
                *reinterpret_cast<const PrimaryVersion*>(iterator.value().data() + iterator.value().length() - sizeof(PrimaryVersion));
            cout << endl;
            cout << u64(std::get<1>(reconstruct)) << endl;
            raise(SIGTRAP);
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
         jumpmu_return ret;
      }
      jumpmuCatch() { ensure(false); }
   } else {
      while (true) {
         jumpmuTry()
         {
            HybridPageGuard<BTreeNode> leaf;
            findLeafCanJump(leaf, o_key, o_key_length);
            // -------------------------------------------------------------------------------------
            s16 pos = leaf->lowerBound<false>(o_key, o_key_length);
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
         jumpmuCatch() { ensure(false); }
      }
   }
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
      if (FLAGS_vi_fupdate) {
         auto current_value = iterator.mutableValue();
         callback(current_value.data(), current_value.length());
         jumpmu_return OP_RESULT::OK;
      }
      // -------------------------------------------------------------------------------------
      u16 delta_and_descriptor_size, secondary_payload_length;
      u8 secondary_payload[PAGE_SIZE];
      std::memcpy(secondary_payload, &update_descriptor, update_descriptor.size());
      SN secondary_sn;
      SN gc_next_sn = 0;
      // -------------------------------------------------------------------------------------
      u8 primary_version_worker_id;
      u64 primary_version_tts;
      u64 primary_version_versions_counter;
      u64 primary_version_commited_after_so;
      // -------------------------------------------------------------------------------------
      {
         auto primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         primary_version.writeLock();
         // -------------------------------------------------------------------------------------
         delta_and_descriptor_size = update_descriptor.size() + BTreeLL::calculateDeltaSize(update_descriptor);
         secondary_payload_length = delta_and_descriptor_size + sizeof(SecondaryVersion);
         // -------------------------------------------------------------------------------------
         BTreeLL::copyDiffTo(update_descriptor, secondary_payload + update_descriptor.size(), primary_payload.data());
         // -------------------------------------------------------------------------------------
         SecondaryVersion& secondary_version =
             *new (secondary_payload + delta_and_descriptor_size) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, true);
         secondary_version.next_sn = primary_version.next_sn;  // !: must update when purging the whole chain
         if (secondary_version.worker_id == cr::Worker::my().worker_id && secondary_version.tts == myTTS()) {
            secondary_version.commited_before_so = std::numeric_limits<u64>::max();
         } else {
            secondary_version.commited_before_so = cr::Worker::my().so_start;
         }
         // -------------------------------------------------------------------------------------
         gc_next_sn = primary_version.next_sn;
         primary_version_worker_id = primary_version.worker_id;
         primary_version_tts = primary_version.tts;
         primary_version_versions_counter = primary_version.versions_counter;
         primary_version_commited_after_so = primary_version.commited_after_so;
         // -------------------------------------------------------------------------------------
         iterator.markAsDirty();
      }
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains[dt_id]++; }
      // -------------------------------------------------------------------------------------
      // TODO: As a first step
      // precise garbage collection
      // Invariant: max (#versions) = #workers
      //
      u64 removed_versions_counter = 0;
      SN recycled_sn = 0;
      bool next_higher = true;
      if (gc_next_sn) {
         SN prev_sn = 0, cur_sn = gc_next_sn;
         u8 buffer[PAGE_SIZE];
         auto gc_descriptor = reinterpret_cast<UpdateSameSizeInPlaceDescriptor*>(buffer);
         u64 w = 0;
         u64 i = 0, pi = 0;  // debug
         const bool enable_pgc = FLAGS_pgc && primary_version_versions_counter >= FLAGS_vi_pgc_batch_size;
         // -------------------------------------------------------------------------------------
         if (enable_pgc) {
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc[dt_id]++; }
            cr::Worker::my().sortWorkers();  // TODO: 200 L1 miss!
            auto is_visible = [&]() {
               if (FLAGS_vi_pgc_so_method) {
                  const u64 cb = cr::Worker::my().getCB(primary_version_worker_id, primary_version_commited_after_so);
                  return (cb &&
                          cr::Worker::my().isVisibleForItCommitedBeforeSO(cr::Worker::my().all_sorted_so_starts[w] & cr::Worker::WORKERS_MASK, cb));
               } else {
                  return cr::Worker::my().isVisibleForIt(cr::Worker::my().all_sorted_so_starts[w] & cr::Worker::WORKERS_MASK,
                                                         primary_version_worker_id, primary_version_tts);
               }
            };
            while (w < cr::Worker::my().workers_count && is_visible()) {
               w++;
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc_workers_visited[dt_id]++; }
            }
            WorkerCounters::myCounters().cc_update_chains_pgc_skipped[dt_id] += w;
         }
         // -------------------------------------------------------------------------------------
         while (cur_sn != 0) {
            if (w == cr::Worker::my().workers_count) {  // Reached the end of the needed part of the chain
               if (prev_sn == 0) {
                  reinterpret_cast<PrimaryVersion*>(iterator.mutableValue().data() + iterator.mutableValue().length() - sizeof(PrimaryVersion))
                      ->next_sn = 0;
                  reinterpret_cast<SecondaryVersion*>(secondary_payload + delta_and_descriptor_size)->next_sn = 0;
               } else {
                  reinterpret_cast<SecondaryVersion*>(iterator.mutableValue().data() + iterator.mutableValue().length() - sizeof(SecondaryVersion))
                      ->next_sn = 0;
               }
               // Remove the rest of the chain
               while (cur_sn != 0) {
                  next_higher = cur_sn > getSN(key);
                  setSN(m_key, cur_sn);
                  ret = iterator.seekExactWithHint(key, next_higher);
                  ensure(ret == OP_RESULT::OK);
                  // COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_visited[dt_id]++; }
                  // -------------------------------------------------------------------------------------
                  auto secondary_gc_payload = iterator.mutableValue();
                  auto& secondary_gc_version =
                      *reinterpret_cast<SecondaryVersion*>(secondary_gc_payload.data() + secondary_gc_payload.length() - sizeof(SecondaryVersion));
                  // -------------------------------------------------------------------------------------
                  if (recycled_sn == 0 && iterator.mutableValue().length() >= secondary_payload_length) {
                     recycled_sn = cur_sn;
                     secondary_sn = cur_sn;  // TODO:
                     cur_sn = secondary_gc_version.next_sn;
                     // -------------------------------------------------------------------------------------
                     std::memcpy(iterator.mutableValue().data(), secondary_payload, secondary_payload_length);
                     iterator.shorten(secondary_payload_length);
                     // COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_recycled[dt_id]++; }
                  } else {
                     cur_sn = secondary_gc_version.next_sn;
                     ret = iterator.removeCurrent();
                     ensure(ret == OP_RESULT::OK);
                     // iterator.mergeIfNeeded();
                     // COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[dt_id]++; }
                     removed_versions_counter++;
                  }
                  iterator.markAsDirty();
               }
               break;
            }
            if (!enable_pgc) {
               break;
            }
            u64 cur_w = cr::Worker::my().all_sorted_so_starts[w] & cr::Worker::WORKERS_MASK;
            i++;
            ensure(cur_sn != 0);
            next_higher = cur_sn > getSN(m_key);
            setSN(m_key, cur_sn);
            ret = iterator.seekExactWithHint(key, next_higher);
            ensure(ret == OP_RESULT::OK);
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_visited[dt_id]++; }
            // -------------------------------------------------------------------------------------
            auto gc_payload = iterator.value();
            const auto& gc_version = *reinterpret_cast<const SecondaryVersion*>(gc_payload.data() + gc_payload.length() - sizeof(SecondaryVersion));
            ensure(!gc_version.is_removed);
            std::memcpy(buffer, gc_payload.data(), reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(gc_payload.data())->size());
            gc_next_sn = gc_version.next_sn;
            auto is_visible = [&]() {
               if (FLAGS_vi_pgc_so_method) {
                  return cr::Worker::my().isVisibleForItCommitedBeforeSO(cur_w, gc_version.commited_before_so);
               } else {
                  return cr::Worker::my().isVisibleForIt(cur_w, gc_version.worker_id, gc_version.tts);
               }
            };
            if (is_visible()) {
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_kept[dt_id]++; }
               w++;
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc_workers_visited[dt_id]++; }
               while (gc_next_sn != 0 && w < cr::Worker::my().workers_count) {
                  cur_w = cr::Worker::my().all_sorted_so_starts[w] & cr::Worker::WORKERS_MASK;
                  if (is_visible()) {
                     w++;
                     COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_chains_pgc_workers_visited[dt_id]++; }
                  } else {
                     break;
                  }
               }
               prev_sn = cur_sn;
               cur_sn = gc_next_sn;
            } else if (!gc_version.isFinal()) {
               // We can prune this version
               next_higher = prev_sn > getSN(m_key);
               setSN(m_key, prev_sn);
               ret = iterator.seekExactWithHint(key, next_higher);
               ensure(ret == OP_RESULT::OK);
               // -------------------------------------------------------------------------------------
               auto prev_payload = iterator.mutableValue();
               if (prev_sn == 0) {
                  auto& prev_version = *reinterpret_cast<PrimaryVersion*>(prev_payload.data() + prev_payload.length() - sizeof(PrimaryVersion));
                  ensure(prev_version.next_sn == cur_sn);
                  prev_version.next_sn = gc_next_sn;
                  reinterpret_cast<SecondaryVersion*>(secondary_payload + delta_and_descriptor_size)->next_sn = gc_next_sn;
               } else {
                  auto& prev_version = *reinterpret_cast<SecondaryVersion*>(prev_payload.data() + prev_payload.length() - sizeof(SecondaryVersion));
                  prev_version.next_sn = gc_next_sn;
               }
               iterator.markAsDirty();
               // -------------------------------------------------------------------------------------
               next_higher = cur_sn > getSN(m_key);
               setSN(m_key, cur_sn);
               ret = iterator.seekExactWithHint(key, next_higher);
               ensure(ret == OP_RESULT::OK);
               if (recycled_sn == 0 && iterator.mutableValue().length() >= secondary_payload_length) {
                  recycled_sn = cur_sn;
                  secondary_sn = cur_sn;
                  COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_recycled[dt_id]++; }
               } else {
                  ret = iterator.removeCurrent();
                  ensure(ret == OP_RESULT::OK);
                  // iterator.mergeIfNeeded();
                  removed_versions_counter++;
                  COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_removed[dt_id]++; }
               }
               // -------------------------------------------------------------------------------------
               cur_sn = gc_next_sn;
               iterator.markAsDirty();
            } else {
               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_kept[dt_id]++; }
               prev_sn = cur_sn;
               cur_sn = gc_next_sn;
            }
         }
         WorkerCounters::myCounters().cc_update_versions_kept_max[dt_id] =
             std::max<u64>(WorkerCounters::myCounters().cc_update_versions_kept_max[dt_id], i);
      }
      // -------------------------------------------------------------------------------------
      if (recycled_sn) {
         next_higher = recycled_sn > getSN(m_key);
         setSN(m_key, recycled_sn);
         ret = iterator.seekExactWithHint(key, next_higher);
         ensure(ret == OP_RESULT::OK);
         // -------------------------------------------------------------------------------------
         std::memcpy(iterator.mutableValue().data(), secondary_payload, secondary_payload_length);
         iterator.shorten(secondary_payload_length);
      } else {
         do {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         } while (ret != OP_RESULT::OK);
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_update_versions_created[dt_id]++; }
      }
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      {
         setSN(m_key, 0);
         ret = iterator.seekExactWithHint(key, false);
         // ret = iterator.seekExact(key);
         ensure(ret == OP_RESULT::OK);
         MutableSlice primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         // -------------------------------------------------------------------------------------
         // WAL
         auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdateSSIP>(o_key_length + delta_and_descriptor_size);
         wal_entry->type = WAL_LOG_TYPE::WALUpdate;
         wal_entry->key_length = o_key_length;
         wal_entry->delta_length = delta_and_descriptor_size;
         wal_entry->before_worker_id = primary_version.worker_id;
         wal_entry->before_tts = primary_version.tts;
         wal_entry->after_worker_id = myWorkerID();
         wal_entry->after_tts = myTTS();
         std::memcpy(wal_entry->payload, o_key, o_key_length);
         std::memcpy(wal_entry->payload + o_key_length, &update_descriptor, update_descriptor.size());
         callback(primary_payload.data(), primary_payload.length() - sizeof(PrimaryVersion));  // Update
         BTreeLL::XORDiffTo(update_descriptor, wal_entry->payload + o_key_length + update_descriptor.size(), primary_payload.data());
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         primary_version.worker_id = myWorkerID();
         primary_version.tts = myTTS();
         primary_version.next_sn = secondary_sn;
         ensure(primary_version.versions_counter >= removed_versions_counter);
         primary_version.versions_counter -= removed_versions_counter;
         primary_version.versions_counter += (recycled_sn == 0) ? 1 : 0;
         primary_version.commited_after_so = cr::Worker::my().so_start;
         primary_version.tmp = 1;
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_utodo && !primary_version.is_gc_scheduled) {
            cr::Worker::my().stageTODO(primary_version.worker_id, primary_version.tts, dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
               auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
               todo_entry.key_length = o_key_length;
               todo_entry.sn = secondary_sn;
               std::memcpy(todo_entry.key, o_key, o_key_length);
            });
            primary_version.is_gc_scheduled = true;
         }
         primary_version.unlock();
         iterator.contentionSplit();
         jumpmu_return OP_RESULT::OK;
      }
   }
   jumpmuCatch() { ensure(false); }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTreeVI::insert(u8* o_key, u16 o_key_length, u8* value, u16 value_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 key_length = o_key_length + sizeof(SN);
   u8 key_buffer[key_length];
   std::memcpy(key_buffer, o_key, o_key_length);
   MutableSlice m_key(key_buffer, key_length);
   Slice key(key_buffer, key_length);
   setSN(m_key, 0);
   const u16 payload_length = value_length + sizeof(PrimaryVersion);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            MutableSlice primary_payload = iterator.mutableValue();
            auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
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
         std::memcpy(wal_entry->payload + o_key_length, iterator.value().data(), value_length);
         wal_entry.submit();
         // -------------------------------------------------------------------------------------
         iterator.insertInCurrentNode(key, payload_length);
         MutableSlice payload = iterator.mutableValue();
         std::memcpy(payload.data(), value, value_length);
         auto& primary_version = *new (payload.data() + value_length) PrimaryVersion(myWorkerID(), myTTS());
         primary_version.commited_after_so = cr::Worker::my().so_start;
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
      SN secondary_sn;
      // -------------------------------------------------------------------------------------
      {
         auto primary_payload = iterator.mutableValue();
         PrimaryVersion& primary_version =
             *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
         if (primary_version.isWriteLocked() || !isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            jumpmu_return OP_RESULT::ABORT_TX;
         }
         ensure(primary_version.is_removed == false);
         primary_version.writeLock();
         // -------------------------------------------------------------------------------------
         value_length = iterator.value().length() - sizeof(PrimaryVersion);
         secondary_payload_length = value_length + sizeof(SecondaryVersion);
         std::memcpy(secondary_payload, primary_payload.data(), value_length);
         auto& secondary_version =
             *new (secondary_payload + value_length) SecondaryVersion(primary_version.worker_id, primary_version.tts, false, false);
         secondary_version.worker_id = primary_version.worker_id;
         secondary_version.tts = primary_version.tts;
         secondary_version.next_sn = primary_version.next_sn;
         iterator.markAsDirty();
      }
      // -------------------------------------------------------------------------------------
      {
         do {
            secondary_sn = leanstore::utils::RandomGenerator::getRand<SN>(0, std::numeric_limits<SN>::max());
            // -------------------------------------------------------------------------------------
            setSN(m_key, secondary_sn);
            ret = iterator.insertKV(key, Slice(secondary_payload, secondary_payload_length));
         } while (ret != OP_RESULT::OK);
      }
      iterator.markAsDirty();
      // -------------------------------------------------------------------------------------
      {
         setSN(m_key, 0);
         ret = iterator.seekExactWithHint(key, false);
         ensure(ret == OP_RESULT::OK);
         MutableSlice primary_payload = iterator.mutableValue();
         auto old_primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
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
         iterator.shorten(sizeof(PrimaryVersion));
         primary_payload = iterator.mutableValue();
         auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data());
         primary_version = old_primary_version;
         primary_version.is_removed = true;
         primary_version.worker_id = myWorkerID();
         primary_version.tts = myTTS();
         primary_version.next_sn = secondary_sn;
         primary_version.unlock();
         // -------------------------------------------------------------------------------------
         if (FLAGS_vi_rtodo && !primary_version.is_gc_scheduled) {
            cr::Worker::my().stageTODO(myWorkerID(), myTTS(), dt_id, key_length + sizeof(TODOEntry), [&](u8* entry) {
               auto& todo_entry = *reinterpret_cast<TODOEntry*>(entry);
               todo_entry.key_length = o_key_length;
               std::memcpy(todo_entry.key, o_key, o_key_length);
            });
            primary_version.is_gc_scheduled = true;
         }
      }
   }
   jumpmuCatch() { ensure(false); }
   // -------------------------------------------------------------------------------------
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
// This undo implementation works only for rollback and not for undo operations during recovery
void BTreeVI::undo(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   auto& btree = *reinterpret_cast<BTreeVI*>(btree_object);
   static_cast<void>(btree);
   const WALEntry& entry = *reinterpret_cast<const WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case WAL_LOG_TYPE::WALInsert: {  // Assuming no insert after remove
         auto& insert_entry = *reinterpret_cast<const WALInsert*>(&entry);
         jumpmuTry()
         {
            const u16 key_length = insert_entry.key_length + sizeof(SN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, insert_entry.payload, insert_entry.key_length);
            *reinterpret_cast<SN*>(key_buffer + insert_entry.key_length) = 0;
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
            const u16 key_length = update_entry.key_length + sizeof(SN);
            u8 key_buffer[key_length];
            std::memcpy(key_buffer, update_entry.payload, update_entry.key_length);
            Slice key(key_buffer, key_length);
            MutableSlice m_key(key_buffer, key_length);
            // -------------------------------------------------------------------------------------
            SN undo_sn;
            OP_RESULT ret;
            u8 secondary_payload[PAGE_SIZE];
            u16 secondary_payload_length;
            // -------------------------------------------------------------------------------------
            BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(&btree));
            {
               btree.setSN(m_key, 0);
               ret = iterator.seekExact(key);
               ensure(ret == OP_RESULT::OK);
               MutableSlice primary_payload = iterator.mutableValue();
               auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
               ensure(primary_version.worker_id == btree.myWorkerID());
               ensure(primary_version.tts == btree.myTTS());
               ensure(!primary_version.isWriteLocked());
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
               auto& primary_version = *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
               const auto& secondary_version =
                   *reinterpret_cast<SecondaryVersion*>(secondary_payload + secondary_payload_length - sizeof(SecondaryVersion));
               primary_version.next_sn = secondary_version.next_sn;
               primary_version.tts = secondary_version.tts;
               primary_version.worker_id = secondary_version.worker_id;
               primary_version.versions_counter--;
               primary_version.tmp = 2;
               // -------------------------------------------------------------------------------------
               const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(secondary_payload);
               btree.copyDiffFrom(update_descriptor, primary_payload.data(), secondary_payload + update_descriptor.size());
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
         jumpmuCatch() { ensure(false); }
         break;
      }
      case WAL_LOG_TYPE::WALRemove: {
         auto& remove_entry = *reinterpret_cast<const WALRemove*>(&entry);
         const u16 key_length = remove_entry.key_length + sizeof(SN);
         u8 key_buffer[key_length];
         std::memcpy(key_buffer, remove_entry.payload, remove_entry.key_length);
         Slice key(key_buffer, key_length);
         MutableSlice m_key(key_buffer, key_length);
         const u16 payload_length = remove_entry.value_length + sizeof(PrimaryVersion);
         // -------------------------------------------------------------------------------------
         jumpmuTry()
         {
            SN secondary_sn, undo_next_sn;
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
               auto& primary_version =
                   *reinterpret_cast<PrimaryVersion*>(iterator.mutableValue().data() + iterator.mutableValue().length() - sizeof(PrimaryVersion));
               secondary_sn = primary_version.next_sn;
               if (primary_version.worker_id != btree.myWorkerID()) {
                  raise(SIGTRAP);
               }
               ensure(primary_version.worker_id == btree.myWorkerID());
               ensure(primary_version.tts == btree.myTTS());
               primary_version.writeLock();
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, secondary_sn);
               ret = iterator.seekExactWithHint(key, true);
               ensure(ret == OP_RESULT::OK);
               auto secondary_payload = iterator.value();
               removed_value_length = secondary_payload.length() - sizeof(SecondaryVersion);
               std::memcpy(removed_value, secondary_payload.data(), removed_value_length);
               auto const secondary_version =
                   *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
               undo_worker_id = secondary_version.worker_id;
               undo_tts = secondary_version.tts;
               undo_next_sn = secondary_version.next_sn;
               iterator.markAsDirty();
            }
            // -------------------------------------------------------------------------------------
            {
               btree.setSN(m_key, 0);
               while (true) {
                  ret = iterator.seekExact(key);
                  ensure(ret == OP_RESULT::OK);
                  ret = iterator.enoughSpaceInCurrentNode(key, payload_length);  // TODO:
                  if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
                     iterator.splitForKey(key);
                     continue;
                  }
                  break;
               }
               ret = iterator.removeCurrent();
               ensure(ret == OP_RESULT::OK);
               const u16 primary_payload_length = removed_value_length + sizeof(PrimaryVersion);
               iterator.insertInCurrentNode(key, primary_payload_length);
               auto primary_payload = iterator.mutableValue();
               std::memcpy(primary_payload.data(), removed_value, removed_value_length);
               auto primary_version = new (primary_payload.data() + removed_value_length) PrimaryVersion(undo_worker_id, undo_tts);
               primary_version->next_sn = undo_next_sn;
               primary_version->tmp = 5;
               primary_version->unlock();
               ensure(primary_version->is_removed == false);
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
         jumpmuCatch() { ensure(false); }
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
   const u16 key_length = todo_entry.key_length + sizeof(SN);
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
      if (ret != OP_RESULT::OK) {  // Because of rollbacks
         jumpmu_return;
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_chains[btree.dt_id]++; }
      MutableSlice primary_payload = iterator.mutableValue();
      PrimaryVersion& primary_version =
          *reinterpret_cast<PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
      const bool safe_to_gc =
          (primary_version.worker_id == version_worker_id && primary_version.tts == version_tts) && !primary_version.isWriteLocked();
      if (safe_to_gc) {
         SN next_sn = primary_version.next_sn;
         const bool is_removed = primary_version.is_removed;
         if (is_removed) {
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
            iterator.mergeIfNeeded();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_remove[btree.dt_id]++; }
         } else {
            primary_version.versions_counter = 1;
            primary_version.next_sn = 0;
            primary_version.is_gc_scheduled = false;
            primary_version.tmp = -1;
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_updates[btree.dt_id]++; }
         }
         iterator.markAsDirty();
         // -------------------------------------------------------------------------------------
         while (next_sn != 0) {
            btree.setSN(m_key, next_sn);
            ret = iterator.seekExact(key);
            if (ret != OP_RESULT::OK) {
               raise(SIGTRAP);
               break;
            }
            // -------------------------------------------------------------------------------------
            Slice secondary_payload = iterator.value();
            const auto& secondary_version =
                *reinterpret_cast<const SecondaryVersion*>(secondary_payload.data() + secondary_payload.length() - sizeof(SecondaryVersion));
            next_sn = secondary_version.next_sn;
            iterator.removeCurrent();
            iterator.markAsDirty();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_updates_versions_removed[btree.dt_id]++; }
         }
         if (is_removed) {
            iterator.mergeIfNeeded();
         }
      } else {
         // Cross workers TODO:
         if (cr::Worker::my().isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            cr::Worker::my().commitTODO(primary_version.worker_id, primary_version.tts, cr::Worker::my().so_start, btree.dt_id,
                                        todo_entry.key_length + sizeof(TODOEntry), [&](u8* new_entry) {
                                           auto& new_todo_entry = *reinterpret_cast<TODOEntry*>(new_entry);
                                           new_todo_entry.key_length = todo_entry.key_length;
                                           std::memcpy(new_todo_entry.key, todo_entry.key, new_todo_entry.key_length);
                                        });
         } else {
            cr::Worker::my().commitTODO(version_worker_id, version_tts, cr::Worker::my().so_start, btree.dt_id,
                                        todo_entry.key_length + sizeof(TODOEntry), [&](u8* new_entry) {
                                           auto& new_todo_entry = *reinterpret_cast<TODOEntry*>(new_entry);
                                           new_todo_entry.key_length = todo_entry.key_length;
                                           std::memcpy(new_todo_entry.key, todo_entry.key, new_todo_entry.key_length);
                                        });
         }
         primary_version.is_gc_scheduled = true;
      }
   }
   jumpmuCatch() { ensure(false); }
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
restart : {
   assert(getSN(key) == 0);
   u16 chain_length = 1;
   OP_RESULT ret;
   u16 materialized_value_length;
   std::unique_ptr<u8[]> materialized_value;
   SN secondary_sn;
   {
      Slice primary_payload = iterator.value();
      const PrimaryVersion& primary_version =
          *reinterpret_cast<const PrimaryVersion*>(primary_payload.data() + primary_payload.length() - sizeof(PrimaryVersion));
      if (isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
         if (primary_version.is_removed) {
            return {OP_RESULT::NOT_FOUND, 1};
         }
         callback(primary_payload.substr(0, primary_payload.length() - sizeof(PrimaryVersion)));
         return {OP_RESULT::OK, 1};
      }
      if (primary_version.isFinal()) {
         return {OP_RESULT::NOT_FOUND, 1};
      }
      materialized_value_length = primary_payload.length() - sizeof(PrimaryVersion);
      materialized_value = std::make_unique<u8[]>(materialized_value_length);
      std::memcpy(materialized_value.get(), primary_payload.data(), materialized_value_length);
      secondary_sn = primary_version.next_sn;
   }
   // -------------------------------------------------------------------------------------
   bool next_sn_higher = true;
   while (secondary_sn != 0) {
      setSN(key, secondary_sn);
      ret = iterator.seekExactWithHint(Slice(key.data(), key.length()), next_sn_higher);
      COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_read_versions_visited[dt_id]++; }
      if (ret != OP_RESULT::OK) {
         // Happens either due to undo or garbage collection
         setSN(key, 0);
         ret = iterator.seekExact(Slice(key.data(), key.length()));
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_read_versions_visited[dt_id]++; }
         ensure(ret == OP_RESULT::OK);
         goto restart;
      }
      chain_length++;  // TODO: add average chain length to DT Table (stats)
      ensure(chain_length < FLAGS_chain_max_length);
      Slice payload = iterator.value();
      const auto& secondary_version = *reinterpret_cast<const SecondaryVersion*>(payload.data() + payload.length() - sizeof(SecondaryVersion));
      if (secondary_version.is_delta) {
         // Apply delta
         const auto& update_descriptor = *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload.data());
         BTreeLL::copyDiffFrom(update_descriptor, materialized_value.get(), payload.data() + update_descriptor.size());
      } else {
         materialized_value_length = payload.length() - sizeof(SecondaryVersion);
         materialized_value = std::make_unique<u8[]>(materialized_value_length);
         std::memcpy(materialized_value.get(), payload.data(), materialized_value_length);
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
         cout << chain_length << endl;
         raise(SIGTRAP);
         return {OP_RESULT::NOT_FOUND, chain_length};
      } else {
         next_sn_higher = secondary_version.next_sn > secondary_sn;
         secondary_sn = secondary_version.next_sn;
      }
   }
   raise(SIGTRAP);
   return {OP_RESULT::NOT_FOUND, chain_length};
}
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
