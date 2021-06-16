// BTreeVI and BTreeVW are work in progress!
#pragma once
#include "BTreeLL.hpp"
#include "core/BTreeGenericIterator.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
class BTreeVI : public BTreeLL
{
  public:
   struct WALBeforeAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInitPage : WALEntry {
      DTID dt_id;
   };
   struct WALAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALLogicalSplit : WALEntry {
      PID parent_pid = -1;
      PID left_pid = -1;
      PID right_pid = -1;
      s32 right_pos = -1;
   };
   struct WALInsert : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdateSSIP : WALEntry {
      u16 key_length;
      u64 delta_length;
      u8 before_worker_id;
      u8 after_worker_id;
      u64 before_tts;
      u64 after_tts;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 before_worker_id;
      u64 before_tts;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   // Skip invisible pages optimization
   struct LeafStatistics {
      u64 skip_if_gsn_equal = 0;  // And
      u64 skip_if_your_so_start_older = 0;
   };
   static_assert(sizeof(LeafStatistics) <= 64, "LeafStatistics does not fit in BTreeNode magic box");
   // -------------------------------------------------------------------------------------
   /*
     Plan: we should handle frequently and infrequently updated tuples differently when it comes to maintaining
     versions in the b-tree.
     For frequently updated tuples, we store them in a FatTuple

     Prepartion phase: iterate over the chain and check whether all updated attributes are the same
     and whether they fit on a page
     If both conditions are fullfiled then we can store them in a fat tuple
     When FatTuple runs out of space, we simply crash for now (real solutions approx variable-size pages or fallback to chained keys)
     ----------------------------------------------------------------------------
     How to convert CHAINED to FAT_TUPLE:
     using versions_counter and value_length, allocate fat_tuple on the stack and append all diffs to the fat tuple
     FOR NOW, we assume same_diffs
     ----------------------------------------------------------------------------
     Glossary:
        UpdateDescriptor: (offset, length)[]
        Diff: raw bytes copied from src/dst next to each other according to the descriptor
        Delta: WWTS + diff + (descriptor)?
    */
   enum class TupleFormat : u8 { FAT_TUPLE = 0, CHAINED = 1, VISIBLE_FOR_ALL = 2 };
   struct __attribute__((packed)) Tuple {
      TupleFormat tuple_format;
      u8 worker_id : 8;
      u64 tts : 56;
      u8 write_locked : 1;
      // -------------------------------------------------------------------------------------
      Tuple(TupleFormat tuple_format, u8 worker_id, u64 tts) : tuple_format(tuple_format), worker_id(worker_id), tts(tts) { write_locked = false; }
      bool isWriteLocked() const { return write_locked; }
      void writeLock() { write_locked = true; }
      void unlock() { write_locked = false; }
   };
   // -------------------------------------------------------------------------------------
   using ChainSN = u32;
   // -------------------------------------------------------------------------------------
   // No PGC for chained, always TODO
   struct __attribute__((packed)) ChainedTuple : Tuple {
      u8 is_removed : 1;
      u8 is_gc_scheduled : 1;
      // -------------------------------------------------------------------------------------
      u64 versions_counter = 1;
      u64 commited_after_so = 0;
      ChainSN next_sn = 0;
      s64 tmp = 0;
      u8 payload[];  // latest version in-place
                     // -------------------------------------------------------------------------------------
      ChainedTuple(u8 worker_id, u64 tts) : Tuple(TupleFormat::CHAINED, worker_id, tts), is_removed(false), is_gc_scheduled(false) {}
      bool isFinal() const { return next_sn == 0; }
   };
   struct __attribute__((packed)) ChainedTupleDelta {
      u8 worker_id : 8;
      u64 tts : 56;
      u64 commited_before_so;
      u64 commited_after_so;
      u8 is_removed : 1;
      u8 is_delta : 1;  // TODO: atm, always true
      ChainSN next_sn;
      u8 payload[];  // UpdateDescriptor + Diff
      // -------------------------------------------------------------------------------------
      ChainedTupleDelta(u8 worker_id, u64 tts, bool is_removed, bool is_delta, ChainSN next_sn = 0)
          : worker_id(worker_id), tts(tts), is_removed(is_removed), is_delta(is_delta), next_sn(next_sn)
      {
      }
      bool isFinal() const { return next_sn == 0; }
   };
   // -------------------------------------------------------------------------------------
   struct __attribute__((packed)) FatTuple : Tuple {
      // No TODOs for FatTuple, always PGC
      struct Delta {
         u64 tts : 56;
         u8 worker_id : 8;
         u64 commited_before_so;
         u8 payload[];  // (desriptor + diff) OR diff
      };
      // -------------------------------------------------------------------------------------
      u64 latest_commited_after_so;
      u64 prev_commited_after_so;
      // -------------------------------------------------------------------------------------
      u8 same_attributes : 1;
      u16 value_length;
      u16 total_space, used_space;  // from the payload bytes array
      u16 deltas_count = 0;         // Attention: coupled with used_space
      u16 delta_and_diff_length = 0;
      s64 debug = 0;
      u8 payload[];
      // same_attributes: value, update descriptor, DeltaWithoutDescriptor[] N2O
      // TODO: not sure if we really want it: !same_attributes: value, DeltaWithDescriptor[] N2O
      // -------------------------------------------------------------------------------------
      FatTuple() : Tuple(TupleFormat::FAT_TUPLE, 0, 0)  // TOOD: refactor
      {
         same_attributes = true;
      }
      // returns false to fallback to chained mode
      bool update(BTreeExclusiveIterator& iterator,
                  u8* key,
                  u16 o_key_length,
                  function<void(u8* value, u16 value_size)>,
                  UpdateSameSizeInPlaceDescriptor&,
                  BTreeVI& btree);
      void undoLastUpdate();
      const UpdateSameSizeInPlaceDescriptor& updatedAttributesDescriptor() const;
      inline constexpr u8* value() { return payload; }
      inline const u8* cvalue() const { return payload; }
      Delta* saDelta(u16 delta_i);
      const Delta* csaDelta(u16 delta_i) const;
      std::tuple<OP_RESULT, u16> reconstructTuple(std::function<void(Slice value)> callback) const;
   };
   struct DanglingPointer {
      BufferFrame* bf = nullptr;
      u64 version = -1;
      s32 head_slot = -1, secondary_slot = -1;
      bool isValid() const { return secondary_slot != -1; }
   };
   // -------------------------------------------------------------------------------------
   struct TODOEntry {
      // TODO converts chained to fat when: it failes to prune more than x times (not sure about this trigger)
      u16 key_length;
      ChainSN sn;
      DanglingPointer dangling_pointer = {0, 0, -1, -1};
      u8 key[];
   };
   // -------------------------------------------------------------------------------------
   void convertChainedToFatTuple(BTreeExclusiveIterator& iterator, MutableSlice& s_key);
   // -------------------------------------------------------------------------------------
   OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   OP_RESULT updateSameSizeInPlace(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, UpdateSameSizeInPlaceDescriptor&) override;
   OP_RESULT remove(u8* key, u16 key_length) override;
   OP_RESULT scanAsc(u8* start_key,
                     u16 key_length,
                     function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                     function<void()>) override;
   OP_RESULT scanDesc(u8* start_key,
                      u16 key_length,
                      function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                      function<void()>) override;
   // -------------------------------------------------------------------------------------
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 version_worker_id, const u64 tts);
   static void deserialize(void*, std::unordered_map<std::string, std::string>) {}      // TODO:
   static std::unordered_map<std::string, std::string> serialize(void*) { return {}; }  // TODO:
   static DTRegistry::DTMeta getMeta();
   // -------------------------------------------------------------------------------------

  private:
   template <bool asc = true>
   void scan(u8* o_key, u16 o_key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
   {
      u64 counter = 0;
      volatile bool keep_scanning = true;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
         // -------------------------------------------------------------------------------------
         MutableSlice s_key = iterator.mutableKeyInBuffer(o_key_length + sizeof(ChainSN));
         std::memcpy(s_key.data(), o_key, o_key_length);
         setSN(s_key, 0);
         OP_RESULT ret;
         if (asc) {
            ret = iterator.seek(Slice(s_key.data(), s_key.length()));
         } else {
            ret = iterator.seekForPrev(Slice(s_key.data(), s_key.length()));
         }
         // -------------------------------------------------------------------------------------
         bool visible_chain_found = false;
         if (FLAGS_vi_skip_trash_leaves) {  // TODO: still buggy
            iterator.registerBeforeChangingLeafHook([&](HybridPageGuard<BTreeNode>& leaf) {
               if (!visible_chain_found) {
                  auto& leaf_statistics = *reinterpret_cast<LeafStatistics*>(leaf->meta_box);
                  leaf.bf->header.meta_data_in_shared_mode_mutex.lock();
                  if (leaf_statistics.skip_if_gsn_equal < leaf.bf->page.GSN) {
                     bool skippable = true;
                     for (u64 t_i = 0; t_i < leaf->count && skippable; t_i++) {
                        ensure(leaf->getKeyLen(t_i) >= sizeof(BTreeVI::ChainSN));
                        auto& sn = *reinterpret_cast<ChainSN*>(leaf->getKey(t_i) + leaf->getKeyLen(t_i) - sizeof(BTreeVI::ChainSN));
                        if (sn == 0) {
                           auto& primary_version =
                               *reinterpret_cast<ChainedTuple*>(leaf->getPayload(t_i) + leaf->getPayloadLength(t_i) - sizeof(ChainedTuple));
                           skippable &= primary_version.is_removed && isVisibleForMe(primary_version.worker_id, primary_version.tts);
                        }
                     }
                     if (skippable) {
                        leaf_statistics.skip_if_gsn_equal = leaf.bf->page.GSN;
                        leaf_statistics.skip_if_your_so_start_older = cr::Worker::my().so_start;
                     }
                  }
                  leaf.bf->header.meta_data_in_shared_mode_mutex.unlock();
               }
               visible_chain_found = false;
            });
            iterator.registerAfterChangingLeafHook([&](HybridPageGuard<BTreeNode>& leaf) {
               // TODO: Rewrite
               auto& leaf_statistics = *reinterpret_cast<LeafStatistics*>(leaf->meta_box);
               leaf.bf->header.meta_data_in_shared_mode_mutex.lock_shared();
               if (leaf_statistics.skip_if_gsn_equal == leaf.bf->page.GSN &&
                   leaf_statistics.skip_if_your_so_start_older < cr::Worker::my().so_start) {
                  leaf.bf->header.meta_data_in_shared_mode_mutex.unlock_shared();
                  COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_skipped_leaf[dt_id]++; }
                  return true;
               }
               leaf.bf->header.meta_data_in_shared_mode_mutex.unlock_shared();
               return false;
            });
         }
         // -------------------------------------------------------------------------------------
         while (ret == OP_RESULT::OK) {
            iterator.assembleKey();
            Slice key = iterator.key();
            s_key = iterator.mutableKeyInBuffer();
            // -------------------------------------------------------------------------------------
            while (getSN(key) != 0) {
               if (asc) {
                  ret = iterator.next();
               } else {
                  ret = iterator.prev();
               }
               if (ret != OP_RESULT::OK) {
                  jumpmu_return;
               }
               iterator.assembleKey();
               key = iterator.key();
               s_key = iterator.mutableKeyInBuffer();
            }
            // -------------------------------------------------------------------------------------
            // costs 2K
            auto reconstruct = reconstructTuple(iterator, s_key, [&](Slice value) {
               keep_scanning = callback(s_key.data(), s_key.length() - sizeof(ChainSN), value.data(), value.length());
               visible_chain_found = true;
               counter++;
            });
            const u16 chain_length = std::get<1>(reconstruct);
            COUNTERS_BLOCK()
            {
               WorkerCounters::myCounters().cc_read_chains[dt_id]++;
               WorkerCounters::myCounters().cc_read_versions_visited[dt_id] += chain_length;
               if (std::get<0>(reconstruct) != OP_RESULT::OK) {
                  WorkerCounters::myCounters().cc_read_chains_not_found[dt_id]++;
                  WorkerCounters::myCounters().cc_read_versions_visited_not_found[dt_id] += chain_length;
               }
            }
            if (!keep_scanning) {
               jumpmu_return;
            }
            if (chain_length > 1) {
               setSN(s_key, 0);
               ret = iterator.seekExact(Slice(s_key.data(), s_key.length()));
               ensure(ret == OP_RESULT::OK);
            }
            // -------------------------------------------------------------------------------------
            if (asc) {
               ret = iterator.next();
            } else {
               ret = iterator.prev();
            }
         }
         jumpmu_return;
      }
      jumpmuCatch() { ensure(false); }
   }
   // -------------------------------------------------------------------------------------
   inline bool isVisibleForMe(u8 worker_id, u64 tts) { return cr::Worker::my().isVisibleForMe(worker_id, tts); }
   inline bool isVisibleForMe(u64 wtts) { return cr::Worker::my().isVisibleForMe(wtts); }
   inline SwipType sizeToVT(u64 size) { return SwipType(reinterpret_cast<BufferFrame*>(size)); }
   // -------------------------------------------------------------------------------------
   template <typename T>
   inline ChainSN getSN(T key)
   {
      return swap(*reinterpret_cast<const ChainSN*>(key.data() + key.length() - sizeof(ChainSN)));
   }
   inline void setSN(MutableSlice key, ChainSN sn) { *reinterpret_cast<ChainSN*>(key.data() + key.length() - sizeof(ChainSN)) = swap(sn); }
   static void applyDelta(u8* dst, const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* src);
   inline std::tuple<OP_RESULT, u16> reconstructTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback)
   {
   restart : {
      Slice payload = iterator.value();
      assert(getSN(key) == 0);
      if (reinterpret_cast<const Tuple*>(payload.data())->tuple_format == TupleFormat::CHAINED) {
         const ChainedTuple& primary_version = *reinterpret_cast<const ChainedTuple*>(payload.data());
         if (isVisibleForMe(primary_version.worker_id, primary_version.tts)) {
            if (primary_version.is_removed) {
               return {OP_RESULT::NOT_FOUND, 1};
            }
            callback(Slice(primary_version.payload, payload.length() - sizeof(ChainedTuple)));
            return {OP_RESULT::OK, 1};
         } else {
            if (primary_version.isFinal()) {
               return {OP_RESULT::NOT_FOUND, 1};
            } else {
               jumpmuTry()
               {
                  auto ret = reconstructChainedTuple(iterator, key, callback);
                  jumpmu_return ret;
               }
               jumpmuCatch() { goto restart; }
            }
         }
      } else {
         return reinterpret_cast<const FatTuple*>(payload.data())->reconstructTuple(callback);
      }
   }
   }
   std::tuple<OP_RESULT, u16> reconstructChainedTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
