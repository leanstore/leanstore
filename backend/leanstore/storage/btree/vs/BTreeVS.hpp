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
  OptimisticLatch root_lock = 0;
  Swip<BTreeNode> root_swip;
  // -------------------------------------------------------------------------------------
  BTree();
  // -------------------------------------------------------------------------------------
  void init(DTID dtid);
  // No side effects allowed!
  bool lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
  // -------------------------------------------------------------------------------------
  void scan(u8* start_key, u16 key_length, function<bool(u8* payload, u16 payload_length, function<string()>&)>, function<void()>);
  // -------------------------------------------------------------------------------------
  void insert(u8* key, u16 key_length, u64 payloadLength, u8* payload);
  void trySplit(BufferFrame& to_split, s32 pos = -1);
  // -------------------------------------------------------------------------------------
  void updateSameSize(u8* key, u16 key_length, function<void(u8* payload, u16 payload_size)>);
  void update(u8* key, u16 key_length, u64 payloadLength, u8* payload);
  // -------------------------------------------------------------------------------------
  bool remove(u8* key, u16 key_length);
  bool tryMerge(BufferFrame& to_split, bool swizzle_sibling = true);
  // -------------------------------------------------------------------------------------
  s32 mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                         s32 left_pos,
                         ExclusivePageGuard<BTreeNode>& from_left,
                         ExclusivePageGuard<BTreeNode>& to_right,
                         bool full_merge_or_nothing);
  bool kWayMerge(OptimisticPageGuard<BTreeNode>& p_guard, OptimisticPageGuard<BTreeNode>& c_guard, ParentSwipHandler&);
  // -------------------------------------------------------------------------------------
  // B*-tree
  bool tryBalanceRight(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& left, s32 l_pos);
  bool tryBalanceLeft(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& right, s32 l_pos);
  bool trySplitRight(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& left, s32 l_pos);
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
  template <int op_type = 0>  // 0 read, 1 update same size, 2 structural change // TODO better code
  OptimisticPageGuard<BTreeNode> findLeafForRead(u8* key, u16 key_length)
  {
    u32 volatile mask = 1;
    while (true) {
      jumpmuTry()
      {
        auto p_guard = OptimisticPageGuard<BTreeNode>::makeRootGuard(root_lock);
        OptimisticPageGuard c_guard(p_guard, root_swip);
        while (!c_guard->is_leaf) {
          Swip<BTreeNode>& c_swip = c_guard->lookupInner(key, key_length);
          p_guard = std::move(c_guard);
          c_guard = OptimisticPageGuard(p_guard, c_swip);
        }
        p_guard.kill();
        c_guard.recheck_done();
        jumpmu_return c_guard;
      }
      jumpmuCatch()
      {
        BACKOFF_STRATEGIES()
        // -------------------------------------------------------------------------------------
        if (op_type == 0) {
          WorkerCounters::myCounters().dt_restarts_read[dtid]++;
        } else if (op_type == 1) {
          WorkerCounters::myCounters().dt_restarts_update_same_size[dtid]++;
        } else if (op_type == 2) {
          WorkerCounters::myCounters().dt_restarts_structural_change[dtid]++;
        } else {}
      }
    }
  }
  // -------------------------------------------------------------------------------------
  s64 iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
  s64 iterateAllPagesRec(OptimisticPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf);
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
