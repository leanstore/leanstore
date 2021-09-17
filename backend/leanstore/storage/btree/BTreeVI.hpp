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
      u64 before_tx_id;
      u64 before_command_id;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 before_worker_id;
      u64 before_tx_id;
      u64 before_command_id;
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
   // -------------------------------------------------------------------------------------
   // NEVER SHADOW A MEMBER!!!
   struct __attribute__((packed)) Tuple {
      union {
         u128 read_ts = 0;
         u128 read_lock_counter;
      };
      TupleFormat tuple_format;
      WORKERID worker_id;
      TXID tx_id;
      COMMANDID command_id;
      u8 write_locked : 1;
      // -------------------------------------------------------------------------------------
      Tuple(TupleFormat tuple_format, u8 worker_id, u64 worker_commit_mark)
          : tuple_format(tuple_format), worker_id(worker_id), tx_id(worker_commit_mark), command_id(9999)
      {
         write_locked = false;
      }
      bool isWriteLocked() const { return write_locked; }
      void writeLock() { write_locked = true; }
      void unlock() { write_locked = false; }
   };
   // static_assert(sizeof(Tuple) <= 32, "");
   // -------------------------------------------------------------------------------------
   // Chained: only scheduled gc todos. FatTuple: eager pgc, no scheduled gc todos
   struct __attribute__((packed)) ChainedTuple : Tuple {
      u8 can_convert_to_fat_tuple : 1;
      u8 is_removed : 1;
      // -------------------------------------------------------------------------------------
      u8 payload[];  // latest version in-place
                     // -------------------------------------------------------------------------------------
      ChainedTuple(u8 worker_id, u64 worker_commit_mark) : Tuple(TupleFormat::CHAINED, worker_id, worker_commit_mark), is_removed(false) { reset(); }
      bool isFinal() const { return false; }
      void reset() { can_convert_to_fat_tuple = 1; }
   };
   // static_assert(sizeof(ChainedTuple) <= 42, "");
   // -------------------------------------------------------------------------------------
   // We always append the descriptor, one format to keep simple
   struct __attribute__((packed)) FatTupleDifferentAttributes : Tuple {
      struct __attribute__((packed)) Delta {
         u8 worker_id : 8;
         u64 worker_txid : 56;
         u64 committed_before_txid;
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
      s32 head_slot = -1;
   };
   struct __attribute__((packed)) Version {
      enum class TYPE : u8 { UPDATE, REMOVE };
      TYPE type;
      WORKERID worker_id;
      TXID tx_id;
      COMMANDID command_id;
      Version(TYPE type, WORKERID worker_id, TXID tx_id, COMMANDID command_id)
          : type(type), worker_id(worker_id), tx_id(tx_id), command_id(command_id)
      {
      }
   };
   struct __attribute__((packed)) UpdateVersion : Version {
      u8 is_delta : 1;
      u8 payload[];  // UpdateDescriptor + Diff
      // -------------------------------------------------------------------------------------
      UpdateVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, bool is_delta)
          : Version(Version::TYPE::UPDATE, worker_id, tx_id, command_id), is_delta(is_delta)
      {
      }
      bool isFinal() const { return command_id == 0; }
   };
   struct __attribute__((packed)) RemoveVersion : Version {
      u16 key_length;
      u16 value_length;
      DanglingPointer dangling_pointer;
      bool moved_to_graveway = false;
      u8 payload[];  // Key + Value
      RemoveVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, u16 key_length, u16 value_length)
          : Version(Version::TYPE::REMOVE, worker_id, tx_id, command_id), key_length(key_length), value_length(value_length)
      {
      }
   };
   // -------------------------------------------------------------------------------------
   // KVInterface
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
   void create(DTID dtid, bool enable_wal, BTreeLL* graveyard_btree)
   {
      this->graveyard = graveyard_btree;
      BTreeLL::create(dtid, enable_wal);
   }
   // -------------------------------------------------------------------------------------
   static SpaceCheckResult checkSpaceUtilization(void* btree_object, BufferFrame&);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tx_id);
   static void todo(void* btree_object, const u8* entry_ptr, const u64 version_worker_id, const u64 version_tx_id, const bool called_before);
   static void deserialize(void*, std::unordered_map<std::string, std::string>) {}      // TODO:
   static std::unordered_map<std::string, std::string> serialize(void*) { return {}; }  // TODO:
   static DTRegistry::DTMeta getMeta();
   // -------------------------------------------------------------------------------------
   struct UnlockEntry {
      u16 key_length;  // SN always = 0
      DanglingPointer dangling_pointer;
      u8 key[];
   };
   static void unlock(void* btree_object, const u8* entry_ptr);

  private:
   BTreeLL* graveyard;
   // -------------------------------------------------------------------------------------
   bool convertChainedToFatTupleDifferentAttributes(BTreeExclusiveIterator& iterator);
   // -------------------------------------------------------------------------------------
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
         if (FLAGS_vi_skip_stale_swips) {
            if constexpr (!asc) {
               iterator.shift_to_right_on_frozen_swips = false;
            }
         }
         // -------------------------------------------------------------------------------------
         Slice key(o_key, o_key_length);
         OP_RESULT ret;
         if (asc) {
            ret = iterator.seek(key);
         } else {
            ret = iterator.seekForPrev(key);
         }
         // -------------------------------------------------------------------------------------
         bool skip_current_leaf = false;
         if (FLAGS_vi_skip_stale_leaves) {
            iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
               if (!cr::activeTX().atLeastSI()) {
                  return;
               }
               if (triggerPageWiseGarbageCollection(leaf) && leaf->upper_fence.offset > 0) {
                  std::basic_string<u8> key(leaf->getUpperFenceKey(), leaf->upper_fence.length);
                  BufferFrame* to_find = leaf.bf;
                  BTreeGeneric* btree_generic = static_cast<BTreeGeneric*>(reinterpret_cast<BTreeVI*>(this));
                  iterator.cleanUpCallback([&, key, btree_generic, to_find]() {
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
                           ParentSwipHandler parent_handler = BTreeGeneric::findParentEager(*btree_generic, *to_find);
                           HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
                           HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
                           auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
                           auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
                           if (p_x_guard->getPayloadLength(parent_handler.pos) == 8 && parent_handler.pos < p_x_guard->count) {
                              if (p_x_guard->canExtendPayload(parent_handler.pos, 16)) {
                                 auto swip_backup = *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos));
                                 p_x_guard->extendPayload(parent_handler.pos, 16);
                                 *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos)) = swip_backup;
                                 *reinterpret_cast<u64*>(p_x_guard->getPayload(parent_handler.pos) + 8) = cr::Worker::my().snapshotAcquistionTime();
                              } else {
                                 // TODO: trySplit parent
                              }
                           }
                        }
                     }
                     jumpmuCatch() {}
                  });
               }
            });
         }
         // -------------------------------------------------------------------------------------
         while (ret == OP_RESULT::OK) {
            if (!skip_current_leaf) {
               iterator.assembleKey();
               Slice s_key = iterator.key();
               auto reconstruct = reconstructTuple(s_key, iterator.value(), [&](Slice value) {
                  keep_scanning = callback(s_key.data(), s_key.length(), value.data(), value.length());
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
      UNREACHABLE();
      jumpmu_return OP_RESULT::OTHER;
   }
   // -------------------------------------------------------------------------------------
   // TODO: atm, only ascending
   template <bool asc = true>
   OP_RESULT scanLight(u8* o_key, u16 o_key_length, function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback)
   {
      volatile bool keep_scanning = true;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
         Slice key(o_key, o_key_length);
         OP_RESULT o_ret;
         BTreeSharedIterator g_iterator(*static_cast<BTreeGeneric*>(graveyard));
         OP_RESULT g_ret;
         Slice g_lower_bound, g_upper_bound;
         g_lower_bound = key;
         // -------------------------------------------------------------------------------------
         o_ret = iterator.seek(key);
         if (o_ret != OP_RESULT::OK) {
            jumpmu_return OP_RESULT::OK;
         }
         iterator.assembleKey();
         // -------------------------------------------------------------------------------------
         // Now it begins
         g_upper_bound = Slice(iterator.leaf->getUpperFenceKey(), iterator.leaf->upper_fence.length);
         auto g_range = [&]() {
            // TODO: optimistic
            g_ret = g_iterator.seek(g_lower_bound);
            if (g_ret == OP_RESULT::OK) {
               g_iterator.assembleKey();
               if (g_iterator.key() > g_upper_bound) {
                  g_ret = OP_RESULT::OTHER;
                  g_iterator.reset();
               }
            }
         };
         g_range();
         auto take_from_oltp = [&]() {
            reconstructTuple(iterator.key(), iterator.value(), [&](Slice value) {
               keep_scanning = callback(iterator.key().data(), iterator.key().length(), value.data(), value.length());
            });
            if (!keep_scanning) {
               return false;
            }
            const bool is_last_one = iterator.isLastOne();
            if (is_last_one) {
               g_iterator.reset();
            }
            o_ret = iterator.next();
            if (is_last_one) {
               g_lower_bound = Slice(iterator.buffer, iterator.fence_length + 1);
               g_upper_bound = Slice(iterator.leaf->getUpperFenceKey(), iterator.leaf->upper_fence.length);
               g_range();
            }
            return true;
         };
         while (true) {
            if (g_ret != OP_RESULT::OK && o_ret == OP_RESULT::OK) {
               iterator.assembleKey();
               if (!take_from_oltp()) {
                  jumpmu_return OP_RESULT::OK;
               }
            } else if (g_ret == OP_RESULT::OK && o_ret != OP_RESULT::OK) {
               g_iterator.assembleKey();
               Slice g_key = g_iterator.key();
               reconstructTuple(g_key, g_iterator.value(),
                                [&](Slice value) { keep_scanning = callback(g_key.data(), g_key.length(), value.data(), value.length()); });
               if (!keep_scanning) {
                  jumpmu_return OP_RESULT::OK;
               }
               g_ret = g_iterator.next();
            } else if (g_ret == OP_RESULT::OK && o_ret == OP_RESULT::OK) {
               iterator.assembleKey();
               g_iterator.assembleKey();
               Slice g_key = g_iterator.key();
               Slice oltp_key = iterator.key();
               if (oltp_key <= g_key) {
                  if (!take_from_oltp()) {
                     jumpmu_return OP_RESULT::OK;
                  }
               } else {
                  reconstructTuple(g_key, g_iterator.value(),
                                   [&](Slice value) { keep_scanning = callback(g_key.data(), g_key.length(), value.data(), value.length()); });
                  if (!keep_scanning) {
                     jumpmu_return OP_RESULT::OK;
                  }
                  g_ret = g_iterator.next();
               }
            } else {
               jumpmu_return OP_RESULT::OK;
            }
         }
      }
      jumpmuCatch() { ensure(false); }
      UNREACHABLE();
      jumpmu_return OP_RESULT::OTHER;
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
   inline COMMANDID getSN(T key)
   {
      return swap(*reinterpret_cast<const COMMANDID*>(key.data() + key.length() - sizeof(COMMANDID)));
   }
   inline void setSN(MutableSlice key, COMMANDID sn) { *reinterpret_cast<COMMANDID*>(key.data() + key.length() - sizeof(COMMANDID)) = swap(sn); }
   inline std::tuple<OP_RESULT, u16> reconstructTuple(Slice key, Slice payload, std::function<void(Slice value)> callback)
   {
      while (true) {
         jumpmuTry()
         {
            if (reinterpret_cast<const Tuple*>(payload.data())->tuple_format == TupleFormat::CHAINED) {
               const ChainedTuple& primary_version = *reinterpret_cast<const ChainedTuple*>(payload.data());
               if (isVisibleForMe(primary_version.worker_id, primary_version.tx_id, false)) {
                  if (primary_version.is_removed) {
                     jumpmu_return{OP_RESULT::NOT_FOUND, 1};
                  }
                  callback(Slice(primary_version.payload, payload.length()));
                  if (cr::activeTX().isSerializable()) {
                     if (!cr::activeTX().isSafeSnapshot()) {
                        if (FLAGS_2pl) {
                           const_cast<ChainedTuple&>(primary_version).read_lock_counter |= 1ull << cr::Worker::my().worker_id;
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
                  jumpmu_return{OP_RESULT::OK, 1};
               } else {
                  if (cr::activeTX().isSerializable() && !cr::activeTX().isSafeSnapshot()) {
                     jumpmu_return{OP_RESULT::ABORT_TX, 1};
                  }
                  if (primary_version.isFinal()) {
                     jumpmu_return{OP_RESULT::NOT_FOUND, 1};
                  } else {
                     auto ret = reconstructChainedTuple(key, payload, callback);
                     jumpmu_return ret;
                  }
               }
            } else {
               auto ret = reinterpret_cast<const FatTupleDifferentAttributes*>(payload.data())->reconstructTuple(callback);
               jumpmu_return ret;
            }
         }
         jumpmuCatch() {}
      }
   }
   std::tuple<OP_RESULT, u16> reconstructChainedTuple(Slice key, Slice payload, std::function<void(Slice value)> callback);
};  // namespace btree
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
