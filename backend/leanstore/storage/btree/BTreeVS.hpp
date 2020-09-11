#pragma once
#include "BTreeSlotted.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
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
  DTID dtid;
  // -------------------------------------------------------------------------------------
  atomic<u16> height = 1;  // debugging
  HybridLatch root_lock = 0;
  Swip<BTreeNode> root_swip;
  // -------------------------------------------------------------------------------------
  BTree();
  // -------------------------------------------------------------------------------------
  void init(DTID dtid);
  // No side effects allowed!
  bool lookupOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
  // -------------------------------------------------------------------------------------
  // starts at the key >= start_key
  void rangeScanAsc(u8* start_key, u16 key_length, function<bool(u8* payload, u16 payload_length, function<string()>&)>, function<void()>);
  // starts at the key + 1 and downwards
  void rangeScanDesc(u8* start_key, u16 key_length, function<bool(u8* payload, u16 payload_length, function<string()>&)>, function<void()>);
  // starts at the key
  bool prefixMaxOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
  // -------------------------------------------------------------------------------------
  void insert(u8* key, u16 key_length, u64 payloadLength, u8* payload);
  void trySplit(BufferFrame& to_split, s16 pos = -1);
  // -------------------------------------------------------------------------------------
  void updateSameSize(u8* key, u16 key_length, function<void(u8* payload, u16 payload_size)>);
  void update(u8* key, u16 key_length, u64 payloadLength, u8* payload);
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
  static void iterateChildrenSwips(void* /*btree_object*/, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback);
  // -------------------------------------------------------------------------------------
  ~BTree();
  // -------------------------------------------------------------------------------------
  // Helpers
  template <int op_type = 0>  // 0 point lookup, 1 update same size, 2 structural change, 10 updatesamesize, 11 scan // TODO better code
  void findLeafForRead(HybridPageGuard<BTreeNode>& target_guard, u8* key, u16 key_length)
  {
    u32 volatile mask = 1;
    while (true) {
      jumpmuTry()
      {
        auto p_guard = HybridPageGuard<BTreeNode>::makeRootGuard(root_lock);
        HybridPageGuard<BTreeNode> c_guard(p_guard, root_swip, true);
        while (!c_guard->is_leaf) {
          Swip<BTreeNode>& c_swip = c_guard->lookupInner(key, key_length);
          p_guard = std::move(c_guard);
          if (FLAGS_mutex) {
            c_guard = HybridPageGuard(p_guard, c_swip, true);
          } else {
            c_guard = HybridPageGuard(p_guard, c_swip);
          }
        }
        p_guard.kill();
        target_guard = std::move(c_guard);
        jumpmu_return;
      }
      jumpmuCatch()
      {
        BACKOFF_STRATEGIES()
        // -------------------------------------------------------------------------------------
        if (op_type == 0 || op_type == 11) {
          WorkerCounters::myCounters().dt_restarts_read[dtid]++;
        } else if (op_type == 1) {
          WorkerCounters::myCounters().dt_restarts_update_same_size[dtid]++;
        } else if (op_type == 2) {
          WorkerCounters::myCounters().dt_restarts_structural_change[dtid]++;
        } else {
        }
      }
    }
  }
  // -------------------------------------------------------------------------------------
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
