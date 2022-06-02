#pragma once
#include "BTreeIteratorInterface.hpp"
#include "BTreeNode.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
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
enum class WAL_LOG_TYPE : u8 {
   WALInsert = 1,
   WALUpdate = 2,
   WALRemove = 3,
   WALAfterBeforeImage = 4,
   WALAfterImage = 5,
   WALLogicalSplit = 10,
   WALInitPage = 11
};
struct WALEntry {
   WAL_LOG_TYPE type;
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
// -------------------------------------------------------------------------------------
class BTreeGeneric
{
  public:
   // -------------------------------------------------------------------------------------
   friend class BTreePessimisticIterator;
   // -------------------------------------------------------------------------------------
   Swip<BufferFrame> meta_node_bf;  // kept in memory
   atomic<u64> height = 1;
   DTID dt_id;
   struct Config {
      bool enable_wal = true;
      bool use_bulk_insert = false;
   };
   Config config;
   // -------------------------------------------------------------------------------------
   BTreeGeneric() = default;
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, Config config);
   // -------------------------------------------------------------------------------------
   bool tryMerge(BufferFrame& to_split, bool swizzle_sibling = true);
   // -------------------------------------------------------------------------------------
   void trySplit(BufferFrame& to_split, s16 pos = -1);
   s16 mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                          s16 left_pos,
                          ExclusivePageGuard<BTreeNode>& from_left,
                          ExclusivePageGuard<BTreeNode>& to_right,
                          bool full_merge_or_nothing);
   enum class XMergeReturnCode : u8 { NOTHING, FULL_MERGE, PARTIAL_MERGE };
   XMergeReturnCode XMerge(HybridPageGuard<BTreeNode>& p_guard, HybridPageGuard<BTreeNode>& c_guard, ParentSwipHandler&);
   // -------------------------------------------------------------------------------------
   static SpaceCheckResult checkSpaceUtilization(void* btree_object, BufferFrame&);
   static ParentSwipHandler findParent(BTreeGeneric& btree_object, BufferFrame& to_find);
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static void checkpoint(BTreeGeneric&, BufferFrame& bf, u8* dest);
   static std::unordered_map<std::string, std::string> serialize(BTreeGeneric&);
   static void deserialize(BTreeGeneric&, std::unordered_map<std::string, std::string>);
   // -------------------------------------------------------------------------------------
   ~BTreeGeneric();
   // -------------------------------------------------------------------------------------
   // Helpers
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   inline void findLeafCanJump(HybridPageGuard<BTreeNode>& target_guard, const u8* key, const u16 key_length)
   {
      target_guard.unlock();
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         WorkerCounters::myCounters().dt_inner_page[dt_id]++;
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(p_guard, c_swip, mode);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.unlock();
   }
   // -------------------------------------------------------------------------------------
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void findLeafAndLatch(HybridPageGuard<BTreeNode>& target_guard, const u8* key, u16 key_length)
   {
      while (true) {
         jumpmuTry()
         {
            findLeafCanJump<mode>(target_guard, key, key_length);
            if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
               target_guard.toExclusive();
            } else {
               target_guard.toShared();
            }
            jumpmu_return;
         }
         jumpmuCatch() {}
      }
   }
   // -------------------------------------------------------------------------------------
   static struct ParentSwipHandler findParentJump(BTreeGeneric& btree, BufferFrame& to_find);
   static struct ParentSwipHandler findParentEager(BTreeGeneric& btree, BufferFrame& to_find);
   // -------------------------------------------------------------------------------------
   // Note on Synchronization: findParent is called by the page provide thread which are not allowed to block
   // Therefore, we jump whenever we encounter a latched node on our way
   // Moreover, we jump if any page on the path is already evicted or of the bf could not be found
   // Pre: to_find is not exclusively latched
   template <bool jumpIfEvicted = true>
   static struct ParentSwipHandler findParent(BTreeGeneric& btree, BufferFrame& to_find)
   {
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent[btree.dt_id]++; }
      if (FLAGS_optimistic_parent_pointer) {
         jumpmuTry()
         {
            Guard c_guard(to_find.header.latch);
            c_guard.toOptimisticOrJump();
            BufferFrame::Header::OptimisticParentPointer optimistic_parent_pointer = to_find.header.optimistic_parent_pointer;
            BufferFrame* parent_bf = optimistic_parent_pointer.parent_bf;
            c_guard.recheck();
            if (parent_bf != nullptr) {
               Guard p_guard(parent_bf->header.latch);
               p_guard.toOptimisticOrJump();
               if (parent_bf->page.PLSN == optimistic_parent_pointer.parent_plsn && parent_bf->header.pid == optimistic_parent_pointer.parent_pid) {
                  if (*(optimistic_parent_pointer.swip_ptr) == &to_find) {
                     p_guard.recheck();
                     c_guard.recheck();
                     ParentSwipHandler ret = {.swip = *reinterpret_cast<Swip<BufferFrame>*>(optimistic_parent_pointer.swip_ptr),
                                              .parent_guard = std::move(p_guard),
                                              .parent_bf = parent_bf,
                                              .pos = optimistic_parent_pointer.pos_in_parent};
                     COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_fast[btree.dt_id]++; }
                     jumpmu_return ret;
                  }
               }
            }
         }
         jumpmuCatch() {}
      }
      // -------------------------------------------------------------------------------------
      auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
      // LATCH_FALLBACK_MODE latch_mode = (jumpIfEvicted) ? LATCH_FALLBACK_MODE::JUMP : LATCH_FALLBACK_MODE::EXCLUSIVE;
      LATCH_FALLBACK_MODE latch_mode = LATCH_FALLBACK_MODE::JUMP;  // : LATCH_FALLBACK_MODE::EXCLUSIVE;
      // -------------------------------------------------------------------------------------
      HybridPageGuard<BTreeNode> p_guard(btree.meta_node_bf);
      u16 level = 0;
      // -------------------------------------------------------------------------------------
      Swip<BTreeNode>* c_swip = &p_guard->upper;
      if (btree.dt_id != to_find.page.dt_id || p_guard->upper.isEVICTED()) {
         // Wrong Tree or Root is evicted
         jumpmu::jump();
      }
      // -------------------------------------------------------------------------------------
      const bool infinity = c_node.upper_fence.offset == 0;
      const u16 key_length = c_node.upper_fence.length;
      u8* key = c_node.getUpperFenceKey();
      // -------------------------------------------------------------------------------------
      // check if bf is the root node
      if (&c_swip->asBufferFrameMasked() == &to_find) {
         p_guard.recheck();
         COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_root[btree.dt_id]++; }
         return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = &btree.meta_node_bf.asBufferFrame()};
      }
      // -------------------------------------------------------------------------------------
      if (p_guard->upper.isCOOL()) {
         // Root is cool => every node below is evicted
         jumpmu::jump();
      }
      // -------------------------------------------------------------------------------------
      HybridPageGuard c_guard(p_guard, p_guard->upper,
                              latch_mode);  // The parent of the bf we are looking for (to_find)
      s16 pos = -1;
      auto search_condition = [&](HybridPageGuard<BTreeNode>& guard) {
         if (infinity) {
            c_swip = &(guard->upper);
            pos = guard->count;
         } else {
            pos = guard->lowerBound<false>(key, key_length);
            if (pos == guard->count) {
               c_swip = &(guard->upper);
            } else {
               c_swip = &(guard->getChild(pos));
            }
         }
         return (&c_swip->asBufferFrameMasked() != &to_find);
      };
      while (!c_guard->is_leaf && search_condition(c_guard)) {
         p_guard = std::move(c_guard);
         if constexpr (jumpIfEvicted) {
            if (c_swip->isEVICTED()) {
               jumpmu::jump();
            }
         }
         c_guard = HybridPageGuard(p_guard, c_swip->cast<BTreeNode>(), latch_mode);
         level++;
      }
      p_guard.unlock();
      const bool found = &c_swip->asBufferFrameMasked() == &to_find;
      c_guard.recheck();
      if (!found) {
         jumpmu::jump();
      }
      // -------------------------------------------------------------------------------------
      ParentSwipHandler parent_handler = {
          .swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.guard), .parent_bf = c_guard.bf, .pos = pos};
      if (FLAGS_optimistic_parent_pointer) {
         jumpmuTry()
         {
            Guard c_guard(to_find.header.latch);
            c_guard.toOptimisticOrJump();
            c_guard.tryToExclusive();
            to_find.header.optimistic_parent_pointer.update(parent_handler.parent_bf, parent_handler.parent_bf->header.pid,
                                                            parent_handler.parent_bf->page.PLSN, reinterpret_cast<BufferFrame**>(&parent_handler.swip),
                                                            parent_handler.pos);
            c_guard.unlock();
            parent_handler.is_bf_updated = true;
         }
         jumpmuCatch() {}
      }
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_slow[btree.dt_id]++; }
      return parent_handler;
   }
   // -------------------------------------------------------------------------------------
   // Helpers
   // -------------------------------------------------------------------------------------
   inline bool isMetaNode(HybridPageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf; }
   inline bool isMetaNode(ExclusivePageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf(); }
   s64 iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   s64 iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   u64 countInner();
   u64 countPages();
   u64 countEntries();
   u64 getHeight();
   double averageSpaceUsage();
   u32 bytesFree();
   void printInfos(uint64_t totalSize);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
