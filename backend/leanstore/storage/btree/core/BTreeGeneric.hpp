#pragma once
#include "BTreeInterface.hpp"
#include "BTreeIteratorInterface.hpp"
#include "BTreeNode.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/sync-primitives/JumpMU.hpp"
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
   template <LATCH_FALLBACK_MODE mode>
   friend class BTreePessimisticIterator;
   // -------------------------------------------------------------------------------------
   BufferFrame* meta_node_bf;  // kept in memory
   atomic<u64> height = 1;
   DTID dt_id;
   // -------------------------------------------------------------------------------------
   BTreeGeneric();
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, BufferFrame* meta_bf);
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
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
   inline static bool findParentNoJump(BTreeGeneric& btree, BufferFrame& to_find, ParentSwipHandler& ret) {
      raise(SIGINT);
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent[btree.dt_id]++; }
      if (FLAGS_optimistic_parent_pointer) {
         Guard c_guard(to_find.header.latch);
         if (c_guard.tryToOptimistic()) {
            BufferFrame::Header::OptimisticParentPointer::Child opp = to_find.header.optimistic_parent_pointer.child;
            auto p_guard = opp.checkNoJump(c_guard);
            if (p_guard) {
               if (!((Swip<void>*)(opp.swip_ptr))->isEVICTED() && ((Swip<void>*)(opp.swip_ptr))->bfPtrAsHot() == &to_find) {
                  if (p_guard->tryRecheck() && c_guard.tryRecheck()) {
                     ret = {.swip = reinterpret_cast<Swip<BufferFrame>*>(opp.swip_ptr),//opp.swip_ptr),
                        .parent_guard = std::move(p_guard.value()),
                        .parent_bf = opp.parent_bf,
                        .pos = opp.pos_in_parent};
                     COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_fast[btree.dt_id]++; }
                     ensure(false);
                     return true;
                  }
               }
            }
         }
      }
      jumpmuTry() {
         findParentSlowPath(btree, to_find, ret);
         jumpmu_return true;
      } jumpmuCatch() { return false;}
   }
   inline static void findParent(BTreeGeneric& btree, BufferFrame& to_find, ParentSwipHandler& ret) {
      COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent[btree.dt_id]++; }
      if (FLAGS_optimistic_parent_pointer) {
         jumpmuTry()
         {
            Guard c_guard(to_find.header.latch);
            c_guard.toOptimisticOrJump();
            BufferFrame::Header::OptimisticParentPointer::Child opp = to_find.header.optimistic_parent_pointer.child;
            c_guard.recheck();
            auto p_guard = opp.check(btree.dt_id);
            //COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_dbg[btree.dt_id]++; }
            if (p_guard) {
               auto swip = (Swip<void>*)(opp.swip_ptr);
               if (!swip->isEVICTED()) {
                  if (swip->bfPtrAsHot() == &to_find) {
                     p_guard->recheck();
                     c_guard.recheck();
                     COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_find_parent_fast[btree.dt_id]++; }
                     ret = {.swip = reinterpret_cast<Swip<BufferFrame>*>(opp.swip_ptr),
                        .parent_guard = std::move(p_guard.value()),
                        .parent_bf = opp.parent_bf,
                        .pos = opp.pos_in_parent};
                     c_guard.recheck();
                     jumpmu_return;
                  }
               }
            }
         }
         jumpmuCatch() {}
      }
      findParentSlowPath(btree, to_find, ret);
   }
   static void findParentSlowPath(BTreeGeneric& btree_object, BufferFrame& to_find, ParentSwipHandler& parent_handler, bool opt = FLAGS_optimistic_parent_pointer);
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
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
      u32 volatile mask = 1;
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
         jumpmuCatch() { BACKOFF_STRATEGIES() }
      }
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
