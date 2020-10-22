#pragma once
#include "BTreeSlotted.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace btree
{
namespace vs
{
// -------------------------------------------------------------------------------------
struct BTree {
   enum class WAL_LOG_TYPE : u8 { WALInsert, WALUpdate, WALRemove, WALAfterBeforeImage, WALAfterImage };
   struct WALBeforeAfterImage {
      WAL_LOG_TYPE type;
      u16 image_size;
      u8 payload[];
   };
   struct WALAfterImage {
      WAL_LOG_TYPE type;
      u16 image_size;
      u8 payload[];
   };
   struct WALInsert {
      WAL_LOG_TYPE type;
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate {
      WAL_LOG_TYPE type;
      u16 key_length;
      u8 payload[];
   };
   struct WALRemove {
      WAL_LOG_TYPE type;
      u16 key_length;
      u8 payload[];
   };
   struct WALUpdateGenerator {
      void (*before)(u8* tuple, u8* entry);
      void (*after)(u8* tuple, u8* entry);
      u16 entry_size;
   };
   // -------------------------------------------------------------------------------------
   enum class OP_TYPE : u8 { POINT_READ, POINT_UPDATE, POINT_INSERT, POINT_DELETE, SCAN };
   // -------------------------------------------------------------------------------------
   BufferFrame* meta_node_bf;  // kept in memory
   // -------------------------------------------------------------------------------------
   atomic<u64> height = 1;
   DTID dt_id;
   // -------------------------------------------------------------------------------------
   BTree();
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, BufferFrame* meta_bf);
   // No side effects allowed!
   bool lookupOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
   // -------------------------------------------------------------------------------------
   // starts at the key >= start_key
   void rangeScanAsc(u8* start_key, u16 key_length, function<bool(u8* key, u8* value, u16 value_length)>, function<void()>);
   // starts at the key + 1 and downwards
   void rangeScanDesc(u8* start_key, u16 key_length, function<bool(u8* key, u8* value, u16 value_length)>, function<void()>);
   // starts at the key
   bool prefixMaxOne(u8* key, u16 key_length, function<void(const u8*, const u8*, u16)> value_callback);
   // -------------------------------------------------------------------------------------
   void insert(u8* key, u16 key_length, u64 valueLength, u8* value);
   void trySplit(BufferFrame& to_split, s16 pos = -1);
   // -------------------------------------------------------------------------------------
   void updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0});
   void update(u8* key, u16 key_length, u64 valueLength, u8* value);
   // -------------------------------------------------------------------------------------
   bool remove(u8* key, u16 key_length);
   bool tryMerge(BufferFrame& to_split, bool swizzle_sibling = true);
   // -------------------------------------------------------------------------------------
   s16 mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                          s16 left_pos,
                          ExclusivePageGuard<BTreeNode>& from_left,
                          ExclusivePageGuard<BTreeNode>& to_right,
                          bool full_merge_or_nothing);
   enum class XMergeReturnCode : u8 { NOTHING, FULL_MERGE, PARTIAL_MERGE };
   XMergeReturnCode XMerge(HybridPageGuard<BTreeNode>& p_guard, HybridPageGuard<BTreeNode>& c_guard, ParentSwipHandler&);
   // -------------------------------------------------------------------------------------
   // B*-tree
   bool tryBalanceRight(HybridPageGuard<BTreeNode>& parent, HybridPageGuard<BTreeNode>& left, s16 l_pos);
   bool tryBalanceLeft(HybridPageGuard<BTreeNode>& parent, HybridPageGuard<BTreeNode>& right, s16 l_pos);
   bool trySplitRight(HybridPageGuard<BTreeNode>& parent, HybridPageGuard<BTreeNode>& left, s16 l_pos);
   void tryBStar(BufferFrame&);
   // -------------------------------------------------------------------------------------
   static DTRegistry::DTMeta getMeta();
   static bool checkSpaceUtilization(void* btree_object, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static void iterateChildrenSwips(void* btree_object, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
   // -------------------------------------------------------------------------------------
   ~BTree();
   // -------------------------------------------------------------------------------------
   // Helpers
   template <OP_TYPE op_type = OP_TYPE::POINT_READ>
   inline void findLeafCanJump(HybridPageGuard<BTreeNode>& target_guard, u8* key, u16 key_length)
   {
      HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
      target_guard = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
      // -------------------------------------------------------------------------------------
      u16 volatile level = 0;
      // -------------------------------------------------------------------------------------
      while (!target_guard->is_leaf) {
         Swip<BTreeNode>& c_swip = target_guard->lookupInner(key, key_length);
         p_guard = std::move(target_guard);
         if (level == height - 1) {
            target_guard = HybridPageGuard(
                p_guard, c_swip,
                (op_type == OP_TYPE::POINT_UPDATE || op_type == OP_TYPE::POINT_INSERT) ? FALLBACK_METHOD::EXCLUSIVE : FALLBACK_METHOD::SHARED);
         } else {
            target_guard = HybridPageGuard(p_guard, c_swip);
         }
         level++;
      }
      // -------------------------------------------------------------------------------------
      p_guard.kill();
   }
   // -------------------------------------------------------------------------------------
   template <OP_TYPE op_type = OP_TYPE::POINT_READ>
   void findLeaf(HybridPageGuard<BTreeNode>& target_guard, u8* key, u16 key_length)
   {
      u32 volatile mask = 1;
      while (true) {
         jumpmuTry()
         {
            findLeafCanJump<op_type>(target_guard, key, key_length);
            jumpmu_return;
         }
         jumpmuCatch()
         {
            BACKOFF_STRATEGIES()
            // -------------------------------------------------------------------------------------
            if (op_type == OP_TYPE::POINT_READ || op_type == OP_TYPE::SCAN) {
               WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
            } else if (op_type == OP_TYPE::POINT_UPDATE) {
               WorkerCounters::myCounters().dt_restarts_update_same_size[dt_id]++;
            } else if (op_type == OP_TYPE::POINT_INSERT) {
               WorkerCounters::myCounters().dt_restarts_structural_change[dt_id]++;
            } else {
            }
         }
      }
   }
   // Helpers
   // -------------------------------------------------------------------------------------
   inline bool isMetaNode(HybridPageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf; }
   inline bool isMetaNode(ExclusivePageGuard<BTreeNode>& guard) { return meta_node_bf == guard.bf(); }
   s64 iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   s64 iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
   unsigned countInner();
   u32 countPages();
   u32 countEntries();
   double averageSpaceUsage();
   u32 bytesFree();
   void printInfos(uint64_t totalSize);
};
// -------------------------------------------------------------------------------------
}  // namespace vs
}  // namespace btree
}  // namespace leanstore
