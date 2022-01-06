#include "BTreeGeneric.hpp"

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
BTreeGeneric::BTreeGeneric() {}
// -------------------------------------------------------------------------------------
void BTreeGeneric::create(DTID dtid, BufferFrame* meta_bf)
{
   auto root_write_guard_h = HybridPageGuard<BTreeNode>(dtid);
   auto root_write_guard = ExclusivePageGuard<BTreeNode>(std::move(root_write_guard_h));
   root_write_guard.init(true);
   // -------------------------------------------------------------------------------------
   this->meta_node_bf = meta_bf;
   this->dt_id = dtid;
   HybridPageGuard<BTreeNode> meta_guard(meta_bf);
   ExclusivePageGuard meta_page(std::move(meta_guard));
   meta_page->upper = root_write_guard.bf();  // HACK: use upper of meta node as a swip to the storage root
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::trySplit(BufferFrame& to_split, s16 favored_split_pos)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   auto parent_handler = findParent(*this, to_split);
   HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
   HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
   if (c_guard->count <= 2)
      return;
   // -------------------------------------------------------------------------------------
   BTreeNode::SeparatorInfo sep_info;
   if (favored_split_pos < 0 || favored_split_pos >= c_guard->count - 1) {
      if (FLAGS_bulk_insert) {
         favored_split_pos = c_guard->count - 2;
         sep_info = BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos), static_cast<u16>(favored_split_pos), false};
      } else {
         sep_info = c_guard->findSep();
      }
   } else {
      // Split on a specified position, used by contention management
      sep_info = BTreeNode::SeparatorInfo{c_guard->getFullKeyLen(favored_split_pos), static_cast<u16>(favored_split_pos), false};
   }
   u8 sep_key[sep_info.length];
   if (isMetaNode(p_guard)) {  // root split
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      assert(height == 1 || !c_x_guard->is_leaf);
      // -------------------------------------------------------------------------------------
      // create new root
      auto new_root_h = HybridPageGuard<BTreeNode>(dt_id, false);
      auto new_root = ExclusivePageGuard<BTreeNode>(std::move(new_root_h));
      auto new_left_node_h = HybridPageGuard<BTreeNode>(dt_id);
      auto new_left_node = ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
      // -------------------------------------------------------------------------------------
      auto exec = [&]() {
         new_root.keepAlive();
         new_root.init(false);
         new_root->upper = c_x_guard.bf();
         p_x_guard->upper = new_root.bf();
         // -------------------------------------------------------------------------------------
         new_left_node.init(c_x_guard->is_leaf);
         c_x_guard->getSep(sep_key, sep_info);
         // -------------------------------------------------------------------------------------
         c_x_guard->split(new_root, new_left_node, sep_info.slot, sep_key, sep_info.length);
      };
      if (FLAGS_wal) {
         auto new_root_init_wal = new_root.reserveWALEntry<WALInitPage>(0);
         new_root_init_wal->type = WAL_LOG_TYPE::WALInitPage;
         new_root_init_wal->dt_id = dt_id;
         new_root_init_wal.submit();
         auto new_left_init_wal = new_left_node.reserveWALEntry<WALInitPage>(0);
         new_left_init_wal->type = WAL_LOG_TYPE::WALInitPage;
         new_left_init_wal->dt_id = dt_id;
         new_left_init_wal.submit();
         // -------------------------------------------------------------------------------------
         WALLogicalSplit logical_split_entry;
         logical_split_entry.type = WAL_LOG_TYPE::WALLogicalSplit;
         logical_split_entry.right_pid = c_x_guard.bf()->header.pid;
         logical_split_entry.parent_pid = new_root.bf()->header.pid;
         logical_split_entry.left_pid = new_left_node.bf()->header.pid;
         // -------------------------------------------------------------------------------------
         auto current_right_wal = c_x_guard.reserveWALEntry<WALLogicalSplit>(0);
         *current_right_wal = logical_split_entry;
         assert(current_right_wal->type == logical_split_entry.type);
         current_right_wal.submit();
         // -------------------------------------------------------------------------------------
         exec();
         // -------------------------------------------------------------------------------------
         auto root_wal = new_root.reserveWALEntry<WALLogicalSplit>(0);
         *root_wal = logical_split_entry;
         root_wal.submit();
         // -------------------------------------------------------------------------------------
         auto left_wal = new_left_node.reserveWALEntry<WALLogicalSplit>(0);
         *left_wal = logical_split_entry;
         left_wal.submit();
      } else {
         exec();
      }
      // -------------------------------------------------------------------------------------
      height++;
      return;
   } else {
      // Parent is not root
      const u16 space_needed_for_separator = p_guard->spaceNeeded(sep_info.length, sizeof(SwipType));
      if (p_guard->hasEnoughSpaceFor(space_needed_for_separator)) {  // Is there enough space in the parent
                                                                     // for the separator?
         auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         p_x_guard->requestSpaceFor(space_needed_for_separator);
         assert(meta_node_bf != p_x_guard.bf());
         assert(!p_x_guard->is_leaf);
         // -------------------------------------------------------------------------------------
         auto new_left_node_h = HybridPageGuard<BTreeNode>(dt_id);
         auto new_left_node = ExclusivePageGuard<BTreeNode>(std::move(new_left_node_h));
         // -------------------------------------------------------------------------------------
         auto exec = [&]() {
            new_left_node.init(c_x_guard->is_leaf);
            c_x_guard->getSep(sep_key, sep_info);
            c_x_guard->split(p_x_guard, new_left_node, sep_info.slot, sep_key, sep_info.length);
         };
         // -------------------------------------------------------------------------------------
         if (FLAGS_wal) {
            auto new_left_init_wal = new_left_node.reserveWALEntry<WALInitPage>(0);
            new_left_init_wal->type = WAL_LOG_TYPE::WALInitPage;
            new_left_init_wal->dt_id = dt_id;
            new_left_init_wal.submit();
            // -------------------------------------------------------------------------------------
            WALLogicalSplit logical_split_entry;
            logical_split_entry.type = WAL_LOG_TYPE::WALLogicalSplit;
            logical_split_entry.right_pid = c_x_guard.bf()->header.pid;
            logical_split_entry.parent_pid = p_x_guard.bf()->header.pid;
            logical_split_entry.left_pid = new_left_node.bf()->header.pid;
            // -------------------------------------------------------------------------------------
            auto current_right_wal = c_x_guard.reserveWALEntry<WALLogicalSplit>(0);
            *current_right_wal = logical_split_entry;
            current_right_wal.submit();
            // -------------------------------------------------------------------------------------
            exec();
            // -------------------------------------------------------------------------------------
            auto parent_wal = p_x_guard.reserveWALEntry<WALLogicalSplit>(0);
            *parent_wal = logical_split_entry;
            parent_wal.submit();
            // -------------------------------------------------------------------------------------
            auto left_init_wal = new_left_node.reserveWALEntry<WALInitPage>(0);
            left_init_wal->type = WAL_LOG_TYPE::WALInitPage;
            left_init_wal->dt_id = dt_id;
            left_init_wal.submit();
            auto left_wal = new_left_node.reserveWALEntry<WALLogicalSplit>(0);
            *left_wal = logical_split_entry;
            left_wal.submit();
         } else {
            exec();
         }
      } else {
         p_guard.unlock();
         c_guard.unlock();
         trySplit(*p_guard.bf);  // Must split parent head to make space for separator
      }
   }
}
// -------------------------------------------------------------------------------------
bool BTreeGeneric::tryMerge(BufferFrame& to_merge, bool swizzle_sibling)
{
   auto parent_handler = findParent(*this, to_merge);
   HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
   HybridPageGuard<BTreeNode> c_guard = HybridPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
   int pos = parent_handler.pos;
   if (isMetaNode(p_guard) || c_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
      p_guard.unlock();
      c_guard.unlock();
      return false;
   }
   // -------------------------------------------------------------------------------------
   if (pos >= p_guard->count) {
      // TODO: we do not merge the node if it is the upper swip of parent
      return false;
   }
   // -------------------------------------------------------------------------------------
   p_guard.recheck();
   c_guard.recheck();
   // -------------------------------------------------------------------------------------
   auto merge_left = [&]() {
      Swip<BTreeNode>& l_swip = p_guard->getChild(pos - 1);
      if (!swizzle_sibling && !l_swip.isHOT()) {
         return false;
      }
      auto l_guard = HybridPageGuard(p_guard, l_swip);
      if (l_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
         l_guard.unlock();
         return false;
      }
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto l_x_guard = ExclusivePageGuard(std::move(l_guard));
      // -------------------------------------------------------------------------------------
      if (!l_x_guard->merge(pos - 1, p_x_guard, c_x_guard)) {
         p_guard = std::move(p_x_guard);
         c_guard = std::move(c_x_guard);
         l_guard = std::move(l_x_guard);
         return false;
      }
      l_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      c_guard = std::move(c_x_guard);
      return true;
   };
   auto merge_right = [&]() {
      Swip<BTreeNode>& r_swip = p_guard->getChild(pos + 1);
      if (!swizzle_sibling && !r_swip.isHOT()) {
         return false;
      }
      auto r_guard = HybridPageGuard(p_guard, r_swip);
      if (r_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
         r_guard.unlock();
         return false;
      }
      auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      auto r_x_guard = ExclusivePageGuard(std::move(r_guard));
      // -------------------------------------------------------------------------------------
      assert(p_x_guard->getChild(pos).bfPtr() == c_x_guard.bf());
      if (!c_x_guard->merge(pos, p_x_guard, r_x_guard)) {
         p_guard = std::move(p_x_guard);
         c_guard = std::move(c_x_guard);
         r_guard = std::move(r_x_guard);
         return false;
      }
      c_x_guard.reclaim();
      // -------------------------------------------------------------------------------------
      p_guard = std::move(p_x_guard);
      r_guard = std::move(r_x_guard);
      return true;
   };
   // ATTENTION: don't use c_guard without making sure it was not reclaimed
   // -------------------------------------------------------------------------------------
   volatile bool merged_successfully = false;
   if (p_guard->count > 2) {
      if (pos > 0) {
         merged_successfully |= merge_left();
      }
      if (!merged_successfully && (pos + 1 < p_guard->count)) {
         merged_successfully |= merge_right();
      }
   }
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      HybridPageGuard<BTreeNode> meta_guard(meta_node_bf);
      if (!isMetaNode(p_guard) && p_guard->freeSpaceAfterCompaction() >= BTreeNode::underFullSize) {
         tryMerge(*p_guard.bf, swizzle_sibling);
      }
   }
   jumpmuCatch() {}
   // -------------------------------------------------------------------------------------
   return merged_successfully;
}
// -------------------------------------------------------------------------------------
// ret: 0 did nothing, 1 full, 2 partial
s16 BTreeGeneric::mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                                     s16 left_pos,
                                     ExclusivePageGuard<BTreeNode>& from_left,
                                     ExclusivePageGuard<BTreeNode>& to_right,
                                     bool full_merge_or_nothing)
{
   // TODO: corner cases: new upper fence is larger than the older one.
   u32 space_upper_bound = from_left->mergeSpaceUpperBound(to_right);
   if (space_upper_bound <= EFFECTIVE_PAGE_SIZE) {  // Do a full merge TODO: threshold
      bool succ = from_left->merge(left_pos, parent, to_right);
      static_cast<void>(succ);
      assert(succ);
      from_left.reclaim();
      return 1;
   }
   if (full_merge_or_nothing)
      return 0;
   // -------------------------------------------------------------------------------------
   // Do a partial merge
   // Remove a key at a time from the merge and check if now it fits
   s16 till_slot_id = -1;
   for (s16 s_i = 0; s_i < from_left->count; s_i++) {
      space_upper_bound -= sizeof(BTreeNode::Slot) + from_left->getKeyLen(s_i) + from_left->getPayloadLength(s_i);
      if (space_upper_bound + (from_left->getFullKeyLen(s_i) - to_right->lower_fence.length) < EFFECTIVE_PAGE_SIZE * 1.0) {
         till_slot_id = s_i + 1;
         break;
      }
   }
   if (!(till_slot_id != -1 && till_slot_id < (from_left->count - 1)))
      return 0;  // false

   assert((space_upper_bound + (from_left->getFullKeyLen(till_slot_id - 1) - to_right->lower_fence.length)) < EFFECTIVE_PAGE_SIZE * 1.0);
   assert(till_slot_id > 0);
   // -------------------------------------------------------------------------------------
   u16 copy_from_count = from_left->count - till_slot_id;
   // -------------------------------------------------------------------------------------
   u16 new_left_uf_length = from_left->getFullKeyLen(till_slot_id - 1);
   ensure(new_left_uf_length > 0);
   u8 new_left_uf_key[new_left_uf_length];
   from_left->copyFullKey(till_slot_id - 1, new_left_uf_key);
   // -------------------------------------------------------------------------------------
   if (!parent->prepareInsert(new_left_uf_length, 0))
      return 0;  // false
   // -------------------------------------------------------------------------------------
   // cout << till_slot_id << '\t' << from_left->count << '\t' << to_right->count << endl;
   // -------------------------------------------------------------------------------------
   {
      BTreeNode tmp(true);
      tmp.setFences(new_left_uf_key, new_left_uf_length, to_right->getUpperFenceKey(), to_right->upper_fence.length);
      // -------------------------------------------------------------------------------------
      from_left->copyKeyValueRange(&tmp, 0, till_slot_id, copy_from_count);
      to_right->copyKeyValueRange(&tmp, copy_from_count, 0, to_right->count);
      memcpy(reinterpret_cast<u8*>(to_right.ptr()), &tmp, sizeof(BTreeNode));
      to_right->makeHint();
      // -------------------------------------------------------------------------------------
      // Nothing to do for the right node's separator
      assert(to_right->compareKeyWithBoundaries(new_left_uf_key, new_left_uf_length) == 1);
   }
   {
      BTreeNode tmp(true);
      tmp.setFences(from_left->getLowerFenceKey(), from_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
      // -------------------------------------------------------------------------------------
      from_left->copyKeyValueRange(&tmp, 0, 0, from_left->count - copy_from_count);
      memcpy(reinterpret_cast<u8*>(from_left.ptr()), &tmp, sizeof(BTreeNode));
      from_left->makeHint();
      // -------------------------------------------------------------------------------------
      assert(from_left->compareKeyWithBoundaries(new_left_uf_key, new_left_uf_length) == 0);
      // -------------------------------------------------------------------------------------
      parent->removeSlot(left_pos);
      ensure(parent->prepareInsert(from_left->upper_fence.length, sizeof(SwipType)));
      auto swip = from_left.swip();
      parent->insert(from_left->getUpperFenceKey(), from_left->upper_fence.length, reinterpret_cast<u8*>(&swip), sizeof(SwipType));
   }
   return 2;
}
// -------------------------------------------------------------------------------------
// returns true if it has exclusively locked anything
BTreeGeneric::XMergeReturnCode BTreeGeneric::XMerge(HybridPageGuard<BTreeNode>& p_guard,
                                                    HybridPageGuard<BTreeNode>& c_guard,
                                                    ParentSwipHandler& parent_handler)
{
   WorkerCounters::myCounters().dt_researchy[0][1]++;
   if (c_guard->fillFactorAfterCompaction() >= 0.9) {
      return XMergeReturnCode::NOTHING;
   }
   // -------------------------------------------------------------------------------------
   const u8 MAX_MERGE_PAGES = FLAGS_xmerge_k;
   s16 pos = parent_handler.pos;
   u8 pages_count = 1;
   s16 max_right;
   HybridPageGuard<BTreeNode> guards[MAX_MERGE_PAGES];
   bool fully_merged[MAX_MERGE_PAGES];
   // -------------------------------------------------------------------------------------
   guards[0] = std::move(c_guard);
   fully_merged[0] = false;
   double total_fill_factor = guards[0]->fillFactorAfterCompaction();
   // -------------------------------------------------------------------------------------
   // Handle upper swip instead of avoiding p_guard->count -1 swip
   if (isMetaNode(p_guard) || !guards[0]->is_leaf) {
      c_guard = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
   }
   for (max_right = pos + 1; (max_right - pos) < MAX_MERGE_PAGES && (max_right + 1) < p_guard->count; max_right++) {
      if (!p_guard->getChild(max_right).isHOT()) {
         c_guard = std::move(guards[0]);
         return XMergeReturnCode::NOTHING;
      }
      // -------------------------------------------------------------------------------------
      guards[max_right - pos] = HybridPageGuard<BTreeNode>(p_guard, p_guard->getChild(max_right));
      fully_merged[max_right - pos] = false;
      total_fill_factor += guards[max_right - pos]->fillFactorAfterCompaction();
      pages_count++;
      if ((pages_count - std::ceil(total_fill_factor)) >= (1)) {
         // we can probably save a page by merging all together so there is no need to look furhter
         break;
      }
   }
   if (((pages_count - std::ceil(total_fill_factor))) < (1)) {
      c_guard = std::move(guards[0]);
      return XMergeReturnCode::NOTHING;
   }
   // -------------------------------------------------------------------------------------
   ExclusivePageGuard<BTreeNode> p_x_guard = std::move(p_guard);
   // -------------------------------------------------------------------------------------
   XMergeReturnCode ret_code = XMergeReturnCode::PARTIAL_MERGE;
   s16 left_hand, right_hand, ret;
   while (true) {
      for (right_hand = max_right; right_hand > pos; right_hand--) {
         if (fully_merged[right_hand - pos]) {
            continue;
         } else {
            break;
         }
      }
      if (right_hand == pos)
         break;
      // -------------------------------------------------------------------------------------
      left_hand = right_hand - 1;
      // -------------------------------------------------------------------------------------
      {
         ExclusivePageGuard<BTreeNode> right_x_guard(std::move(guards[right_hand - pos]));
         ExclusivePageGuard<BTreeNode> left_x_guard(std::move(guards[left_hand - pos]));
         max_right = left_hand;
         ret = mergeLeftIntoRight(p_x_guard, left_hand, left_x_guard, right_x_guard, left_hand == pos);
         // we unlock only the left page, the right one should not be touched again
         if (ret == 1) {
            fully_merged[left_hand - pos] = true;
            WorkerCounters::myCounters().xmerge_full_counter[dt_id]++;
            ret_code = XMergeReturnCode::FULL_MERGE;
         } else if (ret == 2) {
            guards[left_hand - pos] = std::move(left_x_guard);
            WorkerCounters::myCounters().xmerge_partial_counter[dt_id]++;
         } else if (ret == 0) {
            break;
         } else {
            ensure(false);
         }
      }
      // -------------------------------------------------------------------------------------
   }
   if (c_guard.guard.state == GUARD_STATE::MOVED)
      c_guard = std::move(guards[0]);
   p_guard = std::move(p_x_guard);
   return ret_code;
}
// -------------------------------------------------------------------------------------
BTreeGeneric::~BTreeGeneric() {}
// -------------------------------------------------------------------------------------
// Called by buffer manager before eviction
// Returns true if the buffer manager has to restart and pick another buffer frame for eviction
// Attention: the guards here down the stack are not synchronized with the ones in the buffer frame manager stack frame
bool BTreeGeneric::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard& o_guard, ParentSwipHandler& parent_handler)
{
   if (FLAGS_xmerge) {
      auto& btree = *reinterpret_cast<BTreeGeneric*>(btree_object);
      HybridPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
      HybridPageGuard<BTreeNode> c_guard(o_guard.guard, &bf);
      XMergeReturnCode return_code = btree.XMerge(p_guard, c_guard, parent_handler);
      o_guard.guard = std::move(c_guard.guard);
      parent_handler.parent_guard = std::move(p_guard.guard);
      p_guard.unlock();
      c_guard.unlock();
      return (return_code != XMergeReturnCode::NOTHING);
   }
   return false;
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::checkpoint(void*, BufferFrame& bf, u8* dest)
{
   std::memcpy(dest, bf.page.dt, EFFECTIVE_PAGE_SIZE);
   auto& dest_node = *reinterpret_cast<BTreeNode*>(dest);
   // root node is handled as inner
   if (dest_node.isInner()) {
      for (u64 t_i = 0; t_i < dest_node.count; t_i++) {
         if (!dest_node.getChild(t_i).isEVICTED()) {
           auto& child_bf = *dest_node.getChild(t_i).bfPtrAsHot();
           dest_node.getChild(t_i).evict(child_bf.header.pid);
         }
      }
      if (!dest_node.upper.isEVICTED()) {
         auto& child_bf = *dest_node.upper.bfPtrAsHot();
         dest_node.upper.evict(child_bf.header.pid);
      }
   }
}
// -------------------------------------------------------------------------------------
// TODO: Refactor
// Jump if any page on the path is already evicted or of the bf could not be found
// to_find is not latched
struct ParentSwipHandler BTreeGeneric::findParent(BTreeGeneric& btree, BufferFrame& to_find)
{
   auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
   // -------------------------------------------------------------------------------------
   HybridPageGuard<BTreeNode> p_guard(btree.meta_node_bf);
   u16 level = 0;
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>* c_swip = &p_guard->upper;
   if (btree.dt_id != to_find.page.dt_id || (!p_guard->upper.isHOT())) {
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   const bool infinity = c_node.upper_fence.offset == 0;
   const u16 key_length = c_node.upper_fence.length;
   u8* key = c_node.getUpperFenceKey();
   // -------------------------------------------------------------------------------------
   // check if bf is the root node
   if (c_swip->bfPtrAsHot() == &to_find) {
      p_guard.recheck();
      return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.guard), .parent_bf = btree.meta_node_bf};
   }
   // -------------------------------------------------------------------------------------
   HybridPageGuard c_guard(p_guard, p_guard->upper);  // the parent of the bf we are looking for (to_find)
   s16 pos = -1;
   auto search_condition = [&]() {
      if (infinity) {
         c_swip = &(c_guard->upper);
         pos = c_guard->count;
      } else {
         pos = c_guard->lowerBound<false>(key, key_length);
         if (pos == c_guard->count) {
            c_swip = &(c_guard->upper);
         } else {
            c_swip = &(c_guard->getChild(pos));
         }
      }
      return (c_swip->bfPtrAsHot() != &to_find);
   };
   while (!c_guard->is_leaf && search_condition()) {
      p_guard = std::move(c_guard);
      if (c_swip->isEVICTED()) {
         jumpmu::jump();
      }
      c_guard = HybridPageGuard(p_guard, c_swip->cast<BTreeNode>());
      level++;
   }
   p_guard.unlock();
   const bool found = c_swip->bfPtrAsHot() == &to_find;
   c_guard.recheck();
   if (!found) {
      jumpmu::jump();
   }
   return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.guard), .parent_bf = c_guard.bf, .pos = pos};
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   // Pre: bf is read locked
   auto& c_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
   if (c_node.is_leaf) {
      return;
   }
   for (u16 i = 0; i < c_node.count; i++) {
      if (!callback(c_node.getChild(i).cast<BufferFrame>())) {
         return;
      }
   }
   callback(c_node.upper.cast<BufferFrame>());
}
// -------------------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPagesRec(HybridPageGuard<BTreeNode>& node_guard,
                                     std::function<s64(BTreeNode&)> inner,
                                     std::function<s64(BTreeNode&)> leaf)
{
   if (node_guard->is_leaf) {
      return leaf(node_guard.ref());
   }
   s64 res = inner(node_guard.ref());
   for (u16 i = 0; i < node_guard->count; i++) {
      Swip<BTreeNode>& c_swip = node_guard->getChild(i);
      auto c_guard = HybridPageGuard(node_guard, c_swip);
      c_guard.recheck();
      res += iterateAllPagesRec(c_guard, inner, leaf);
   }
   // -------------------------------------------------------------------------------------
   Swip<BTreeNode>& c_swip = node_guard->upper;
   auto c_guard = HybridPageGuard(node_guard, c_swip);
   c_guard.recheck();
   res += iterateAllPagesRec(c_guard, inner, leaf);
   // -------------------------------------------------------------------------------------
   return res;
}
// -------------------------------------------------------------------------------------
s64 BTreeGeneric::iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
         HybridPageGuard c_guard(p_guard, p_guard->upper);
         jumpmu_return iterateAllPagesRec(c_guard, inner, leaf);
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::getHeight()
{
   return height.load();
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countEntries()
{
   return iterateAllPages([](BTreeNode&) { return 0; }, [](BTreeNode& node) { return node.count; });
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countPages()
{
   return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 1; });
}
// -------------------------------------------------------------------------------------
u64 BTreeGeneric::countInner()
{
   return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 0; });
}
// -------------------------------------------------------------------------------------
double BTreeGeneric::averageSpaceUsage()
{
   ensure(false);  // TODO
}
// -------------------------------------------------------------------------------------
u32 BTreeGeneric::bytesFree()
{
   return iterateAllPages([](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
                          [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BTreeGeneric::serialize(BTreeGeneric& btree)
{
   assert(btree.meta_node_bf->page.dt_id == btree.dt_id);
   return {{"dt_id", std::to_string(btree.dt_id)},
           {"height", std::to_string(btree.height.load())},
           {"meta_pid", std::to_string(btree.meta_node_bf->header.pid)}};
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::deserialize(BTreeGeneric& btree, std::unordered_map<std::string, std::string> map)
{
   btree.dt_id = std::stol(map["dt_id"]);
   btree.height = std::stol(map["height"]);
   btree.meta_node_bf = reinterpret_cast<BufferFrame*>(std::stol(map["meta_pid"]) | (u64(1) << 63));
   HybridLatch dummy_latch;
   Guard dummy_guard(&dummy_latch);
   dummy_guard.toOptimisticSpin();
   u16 failcounter = 0;
   while (true) {
      jumpmuTry()
      {
         btree.meta_node_bf = &BMC::global_bf->resolveSwip(dummy_guard, *reinterpret_cast<Swip<BufferFrame>*>(&btree.meta_node_bf));
         jumpmu_break;
      }
      jumpmuCatch()
      {
         failcounter++;
         if (failcounter >= 200) {
            cerr << "Failed to allocate MetaNode, Buffer might be to small" << endl;
            assert(false);
         }
      }
   }
   btree.meta_node_bf->header.keep_in_memory = true;
   assert(btree.meta_node_bf->page.dt_id == btree.dt_id);
   // assert(reinterpret_cast<BTreeNode*>(btree.meta_node_bf->page.dt)->count > 0);
}
// -------------------------------------------------------------------------------------
void BTreeGeneric::printInfos(uint64_t totalSize)
{
   HybridPageGuard<BTreeNode> p_guard(meta_node_bf);
   HybridPageGuard r_guard(p_guard, p_guard->upper);
   uint64_t cnt = countPages();
   cout << "nodes:" << cnt << " innerNodes:" << countInner() << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float)totalSize << " height:" << height
        << " rootCnt:" << r_guard->count << " bytesFree:" << bytesFree() << endl;
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
