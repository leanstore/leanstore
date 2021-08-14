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
      u64 before_worker_commit_mark;
      u64 after_worker_commit_mark;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 before_worker_id;
      u64 before_worker_commit_mark;
      u8 payload[];
   };
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
     Random number generation, similar to contention split, don't eagerly remove the deltas to allow concurrent readers to continue without
     complicating the logic if we fail
     ----------------------------------------------------------------------------
     Glossary:
        UpdateDescriptor: (offset, length)[]
        Diff: raw bytes copied from src/dst next to each other according to the descriptor
        Delta: WWTS + diff + (descriptor)?
    */
   enum class TupleFormat : u8 { CHAINED = 0, FAT_TUPLE_DIFFERENT_ATTRIBUTES = 1, FAT_TUPLE_SAME_ATTRIBUTES = 2, VISIBLE_FOR_ALL = 3 };
   struct __attribute__((packed)) Tuple {
      union {
         u128 read_ts = 0;
         u128 read_lock_counter;
      };
      TupleFormat tuple_format;
      u8 worker_id : 8;
      u64 worker_commit_mark : 56;
      u8 write_locked : 1;
      // -------------------------------------------------------------------------------------
      Tuple(TupleFormat tuple_format, u8 worker_id, u64 worker_commit_mark)
          : tuple_format(tuple_format), worker_id(worker_id), worker_commit_mark(worker_commit_mark)
      {
         write_locked = false;
      }
      bool isWriteLocked() const { return write_locked; }
      void writeLock() { write_locked = true; }
      void unlock() { write_locked = false; }
   };
   static_assert(sizeof(Tuple) <= 32, "");
   // -------------------------------------------------------------------------------------
   using ChainSN = u16;
   // -------------------------------------------------------------------------------------
   // Chained: only scheduled gc todos. FatTuple: eager pgc, no scheduled gc todos
   struct __attribute__((packed)) ChainedTuple : Tuple {
      u8 can_convert_to_fat_tuple : 1;
      u8 is_removed : 1;
      // -------------------------------------------------------------------------------------
      ChainSN next_sn = 0;
      u8 payload[];  // latest version in-place
                     // -------------------------------------------------------------------------------------
      ChainedTuple(u8 worker_id, u64 worker_commit_mark) : Tuple(TupleFormat::CHAINED, worker_id, worker_commit_mark), is_removed(false) { reset(); }
      bool isFinal() const { return next_sn == 0; }
      void reset() { can_convert_to_fat_tuple = 1; }
   };
   static_assert(sizeof(ChainedTuple) <= 42, "");
   // -------------------------------------------------------------------------------------
   struct __attribute__((packed)) ChainedTupleVersion {
      u8 worker_id : 8;
      u64 worker_commit_mark : 56;
      u64 committed_before_sat;  // Helpful for garbage collection
      u8 is_removed : 1;
      u8 is_delta : 1;  // TODO: atm, always true
      u64 gc_trigger;
      ChainSN next_sn;
      u8 payload[];  // UpdateDescriptor + Diff
      // -------------------------------------------------------------------------------------
      ChainedTupleVersion(u8 worker_id, u64 worker_commit_mark, bool is_removed, bool is_delta, u64 gc_trigger, ChainSN next_sn = 0)
          : worker_id(worker_id),
            worker_commit_mark(worker_commit_mark),
            is_removed(is_removed),
            is_delta(is_delta),
            gc_trigger(gc_trigger),
            next_sn(next_sn)
      {
      }
      bool isFinal() const { return next_sn == 0; }
   };
   static_assert(sizeof(ChainedTupleVersion) <= 27, "");
   // -------------------------------------------------------------------------------------
   // We always append the descriptor, one format to keep simple
   struct __attribute__((packed)) FatTupleDifferentAttributes : Tuple {
      struct __attribute__((packed)) Delta {
         u8 worker_id : 8;
         u64 worker_commit_mark : 56;
         u64 committed_before_sat;
         u8 payload[];  // Descriptor + Diff
         UpdateSameSizeInPlaceDescriptor& getDescriptor() { return *reinterpret_cast<UpdateSameSizeInPlaceDescriptor*>(payload); }
         const UpdateSameSizeInPlaceDescriptor& getConstantDescriptor() const
         {
            return *reinterpret_cast<const UpdateSameSizeInPlaceDescriptor*>(payload);
         }
      };
      // -------------------------------------------------------------------------------------
      u16 value_length;
      u16 total_space;       // From the payload bytes array
      u32 used_space;        // u32 instead of u16 to make it easier to detect overflow while converting
      u16 deltas_count = 0;  // Attention: coupled with used_space
      s64 debug = 0;
      u8 payload[];  // value, Delta+Descriptor+Diff[] N2O
      // -------------------------------------------------------------------------------------
      FatTupleDifferentAttributes() : Tuple(TupleFormat::FAT_TUPLE_DIFFERENT_ATTRIBUTES, 0, 0) {}
      // returns false to fallback to chained mode
      static bool update(BTreeExclusiveIterator& iterator,
                         u8* key,
                         u16 o_key_length,
                         function<void(u8* value, u16 value_size)>,
                         UpdateSameSizeInPlaceDescriptor&,
                         BTreeVI& btree);
      void garbageCollection(BTreeVI& btree);
      void undoLastUpdate();
      inline constexpr u8* getValue() { return payload; }
      inline const u8* getValueConstant() const { return payload; }
      std::tuple<OP_RESULT, u16> reconstructTuple(std::function<void(Slice value)> callback) const;
   };
   // -------------------------------------------------------------------------------------
   struct DanglingPointer {
      BufferFrame* bf = nullptr;
      u64 latch_version_should_be = -1;
      s32 head_slot = -1, secondary_slot = -1;
      bool remove_operation = false;
      bool valid = false;
      bool isValid() const { return valid; }
   };
   // -------------------------------------------------------------------------------------
   struct TODOEntry {
      enum class TYPE : u8 { POINT, PAGE };
      TYPE type;
   };
   struct TODOPage : public TODOEntry {
      BufferFrame* bf;
      u64 latch_version_should_be;
      TODOPage() { type = TODOEntry::TYPE::PAGE; }
   };
   struct TODOPoint : public TODOEntry {
      u16 key_length;
      ChainSN sn;
      DanglingPointer dangling_pointer = {0, 0, -1, -1};
      u8 key[];
      TODOPoint() { type = TODOEntry::TYPE::POINT; }
   };
   // -------------------------------------------------------------------------------------
   bool convertChainedToFatTupleDifferentAttributes(BTreeExclusiveIterator& iterator, MutableSlice& s_key);
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
   static SpaceCheckResult checkSpaceUtilization(void* btree_object, BufferFrame&);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* entry_ptr, const u64 version_worker_id, const u64 tts);
   static void deserialize(void*, std::unordered_map<std::string, std::string>) {}      // TODO:
   static std::unordered_map<std::string, std::string> serialize(void*) { return {}; }  // TODO:
   static DTRegistry::DTMeta getMeta();
   // -------------------------------------------------------------------------------------
   struct UnlockEntry {
      u16 key_length;  // SN always = 0
      DanglingPointer dangling_pointer = {0, 0, -1, -1};
      u8 key[];
   };
   static void unlock(void* btree_object, const u8* entry_ptr);

  private:
   OP_RESULT lookupPessimistic(u8* key, const u16 key_length, function<void(const u8*, u16)> payload_callback);
   OP_RESULT lookupOptimistic(const u8* key, const u16 key_length, function<void(const u8*, u16)> payload_callback);
   // -------------------------------------------------------------------------------------
   template <bool asc = true>
   OP_RESULT scan(u8* o_key, u16 o_key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
   {
      // TODO: index range lock for serializability
      u64 counter = 0;
      volatile bool keep_scanning = true;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this),
                                      cr::activeTX().isSerializable() ? LATCH_FALLBACK_MODE::EXCLUSIVE : LATCH_FALLBACK_MODE::SHARED);
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
         bool skip_current_leaf = false;
         if (FLAGS_vi_skip_stale_leaves) {
            iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
               if (leaf->skip_if_gsn_equal == leaf.bf->page.GSN && leaf->and_if_your_sat_older < cr::Worker::my().snapshotAcquistionTime()) {
                  skip_current_leaf = true;
                  COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_skipped_leaf[dt_id]++; }
               }
               if (!cr::activeTX().atLeastSI()) {
                  return;
               }
               if (triggerPageWiseGarbageCollection(leaf) && leaf->upper_fence.offset > 0) {
                  std::basic_string<u8> key(leaf->getUpperFenceKey(), leaf->upper_fence.length);
                  BufferFrame* to_find = leaf.bf;
                  BTreeGeneric* btree_generic = static_cast<BTreeGeneric*>(reinterpret_cast<BTreeVI*>(this));
                  iterator.cleanup_cb = [&, key, btree_generic, to_find]() {
                     jumpmuTry()
                     {
                        HybridPageGuard<BTreeNode> leaf;
                        this->findLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(leaf, key.c_str(), key.length());
                        const bool should_freeze_leaf = precisePageWiseGarbageCollection(leaf);
                        // -------------------------------------------------------------------------------------
                        if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
                           leaf.unlock();
                           tryMerge(*to_find);
                           jumpmu_return;
                        }
                        // -------------------------------------------------------------------------------------
                        leaf.unlock();
                        if (FLAGS_vi_skip_stale_swips && should_freeze_leaf) {
                           ParentSwipHandler parent_handler = BTreeGeneric::findParent(*btree_generic, *to_find);
                           HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
                           HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
                           auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
                           auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
                           if (p_x_guard->getPayloadLength(parent_handler.pos) == 8 && parent_handler.pos < p_x_guard->count) {
                              if (p_x_guard->canExtendPayload(parent_handler.pos, 16)) {
                                 auto swip_backup = *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos));
                                 p_x_guard->extendPayload(parent_handler.pos, 16);
                                 *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos)) = swip_backup;
                                 *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos) + 8) = leaf->and_if_your_sat_older;
                              } else {
                                 // TODO: trySplit parent
                              }
                           }
                        }
                     }
                     jumpmuCatch() {}
                  };
               }
            });
         }
         // -------------------------------------------------------------------------------------
         while (ret == OP_RESULT::OK) {
            iterator.assembleKey();
            Slice key = iterator.key();
            s_key = iterator.mutableKeyInBuffer();
            // -------------------------------------------------------------------------------------
            while (getSN(key) != 0) {
               if constexpr (asc) {
                  if (skip_current_leaf) {
                     iterator.cur = iterator.leaf->count;
                     skip_current_leaf = false;
                  }
                  ret = iterator.next();
               } else {
                  if (skip_current_leaf) {
                     iterator.cur = 0;
                     skip_current_leaf = false;
                  }
                  ret = iterator.prev();
               }
               if (ret != OP_RESULT::OK) {
                  jumpmu_return ret;
               }
               iterator.assembleKey();
               key = iterator.key();
               s_key = iterator.mutableKeyInBuffer();
            }
            // -------------------------------------------------------------------------------------
            if (!skip_current_leaf) {
               auto reconstruct = reconstructTuple(iterator, s_key, [&](Slice value) {
                  keep_scanning = callback(s_key.data(), s_key.length() - sizeof(ChainSN), value.data(), value.length());
                  counter++;
               });
               if (cr::activeTX().isSerializable()) {
                  if (std::get<0>(reconstruct) == OP_RESULT::ABORT_TX) {
                     jumpmu_return OP_RESULT::ABORT_TX;
                  }
               }
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
                  jumpmu_return OP_RESULT::OK;
               }
               if (chain_length > 1) {
                  setSN(s_key, 0);
                  ret = iterator.seekExact(Slice(s_key.data(), s_key.length()));
                  ensure(ret == OP_RESULT::OK);
               }
            }
            // -------------------------------------------------------------------------------------
            if constexpr (asc) {
               if (skip_current_leaf) {
                  iterator.cur = iterator.leaf->count;
                  skip_current_leaf = false;
               }
               ret = iterator.next();
            } else {
               if (skip_current_leaf) {
                  iterator.cur = 0;
                  skip_current_leaf = false;
               }
               ret = iterator.prev();
            }
         }
         jumpmu_return OP_RESULT::OK;
      }
      jumpmuCatch() { ensure(false); }
   }
   // -------------------------------------------------------------------------------------
   inline bool isVisibleForMe(u8 worker_id, u64 worker_commit_mark, bool to_write = true)
   {
      return cr::Worker::my().isVisibleForMe(worker_id, worker_commit_mark, to_write);
   }
   inline SwipType sizeToVT(u64 size) { return SwipType(reinterpret_cast<BufferFrame*>(size)); }
   static inline bool triggerPageWiseGarbageCollection(HybridPageGuard<BTreeNode>& guard)
   {
      return (guard->gc_space_used >= (FLAGS_garbage_in_page_pct * PAGE_SIZE * 1.0 / 100));
   }
   bool precisePageWiseGarbageCollection(HybridPageGuard<BTreeNode>& guard);
   // -------------------------------------------------------------------------------------
   template <typename T>
   inline ChainSN getSN(T key)
   {
      return swap(*reinterpret_cast<const ChainSN*>(key.data() + key.length() - sizeof(ChainSN)));
   }
   inline void setSN(MutableSlice key, ChainSN sn) { *reinterpret_cast<ChainSN*>(key.data() + key.length() - sizeof(ChainSN)) = swap(sn); }
   inline std::tuple<OP_RESULT, u16> reconstructTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback)
   {
      Slice payload = iterator.value();
      assert(getSN(key) == 0);
      if (reinterpret_cast<const Tuple*>(payload.data())->tuple_format == TupleFormat::CHAINED) {
         const ChainedTuple& primary_version = *reinterpret_cast<const ChainedTuple*>(payload.data());
         if (isVisibleForMe(primary_version.worker_id, primary_version.worker_commit_mark, false)) {
            if (primary_version.is_removed) {
               return {OP_RESULT::NOT_FOUND, 1};
            }
            callback(Slice(primary_version.payload, payload.length() - sizeof(ChainedTuple)));
            if (cr::activeTX().isSerializable()) {
               if (!cr::activeTX().isSafeSnapshot()) {
                  if (FLAGS_2pl) {
                     const_cast<ChainedTuple&>(primary_version).read_lock_counter |= 1ull << cr::Worker::my().worker_id;
                     iterator.assembleKey();
                     const Slice key = iterator.key();
                     cr::Worker::my().addUnlockTask(dt_id, sizeof(UnlockEntry) + key.length(), [&](u8* entry) {
                        auto& unlock_entry = *new (entry) UnlockEntry();
                        unlock_entry.key_length = key.length();
                        std::memcpy(unlock_entry.key, key.data(), key.length());
                     });
                  } else {
                     const_cast<ChainedTuple&>(primary_version).read_ts = std::max<u128>(primary_version.read_ts, cr::activeTX().TTS());
                  }
               }
            }
            return {OP_RESULT::OK, 1};
         } else {
            if (cr::activeTX().isSerializable() && !cr::activeTX().isSafeSnapshot()) {
               return {OP_RESULT::ABORT_TX, 1};
            }
            if (primary_version.isFinal()) {
               return {OP_RESULT::NOT_FOUND, 1};
            } else {
               return reconstructChainedTuple(iterator, key, callback);
            }
         }
      } else {
         return reinterpret_cast<const FatTupleDifferentAttributes*>(payload.data())->reconstructTuple(callback);
      }
   }
   std::tuple<OP_RESULT, u16> reconstructChainedTuple(BTreeSharedIterator& iterator, MutableSlice key, std::function<void(Slice value)> callback);
};  // namespace btree
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
