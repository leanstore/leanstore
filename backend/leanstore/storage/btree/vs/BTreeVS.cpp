#include "BTreeVS.hpp"

// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace btree
{
namespace vs
{
// -------------------------------------------------------------------------------------
BTree::BTree() {}
// -------------------------------------------------------------------------------------
void BTree::init(DTID dtid)
{
  this->dtid = dtid;
  auto root_write_guard = ExclusivePageGuard<BTreeNode>::allocateNewPage(dtid);
  root_write_guard.init(true);
  root_swip = root_write_guard.bf;
}
// -------------------------------------------------------------------------------------
bool BTree::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard<BTreeNode> leaf = findLeafForRead(key, key_length);
      // -------------------------------------------------------------------------------------
      DEBUG_BLOCK()
      {
        bool sanity_check_result = leaf->sanityCheck(key, key_length);
        leaf.recheck_done();
        if (!sanity_check_result) {
          cout << leaf->count << endl;
        }
        ensure(sanity_check_result);
      }
      // -------------------------------------------------------------------------------------
      s32 pos = leaf->lowerBound<true>(key, key_length);
      if (pos != -1) {
        u16 payload_length = leaf->getPayloadLength(pos);
        payload_callback((leaf->isLarge(pos)) ? leaf->getPayloadLarge(pos) : leaf->getPayload(pos), payload_length);
        leaf.recheck_done();
        jumpmu_return true;
      } else {
        leaf.recheck_done();
        jumpmu_return false;
      }
    }
    jumpmuCatch()
    {
      BACKOFF_STRATEGIES()
      WorkerCounters::myCounters().dt_restarts_read[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
void BTree::scan(u8* start_key,
                 u16 key_length,
                 std::function<bool(u8* payload, u16 payload_length, std::function<string()>&)> callback,
                 function<void()> undo)
{
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
  u8* volatile next_key = start_key;
  volatile u16 next_key_length = key_length;
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard<BTreeNode> o_leaf = findLeafForRead(next_key, next_key_length);
      while (true) {
        auto leaf = SharedPageGuard<BTreeNode>(std::move(o_leaf));
        s32 cur = leaf->lowerBound<false>(start_key, key_length);
        while (cur < leaf->count) {
          u16 payload_length = leaf->getPayloadLength(cur);
          u8* payload = leaf->isLarge(cur) ? leaf->getPayloadLarge(cur) : leaf->getPayload(cur);
          std::function<string()> key_extract_fn = [&]() {
            u16 key_length = leaf->getFullKeyLength(cur);
            string key(key_length, '0');
            leaf->copyFullKey(cur, reinterpret_cast<u8*>(key.data()), key_length);
            return key;
          };
          if (!callback(payload, payload_length, key_extract_fn)) {
            jumpmu_return;
          }
          cur++;
        }
        // -------------------------------------------------------------------------------------
        if (next_key != start_key) {
          delete[] next_key;
        }
        if (leaf->isUpperFenceInfinity()) {
          jumpmu_return;
        }
        // -------------------------------------------------------------------------------------
        next_key_length = leaf->upper_fence.length + 1;
        next_key = new u8[next_key_length];
        memcpy(next_key, leaf->getUpperFenceKey(), leaf->upper_fence.length);
        next_key[next_key_length - 1] = 0;
        // -------------------------------------------------------------------------------------
        o_leaf = std::move(leaf);
        o_leaf = findLeafForRead(next_key, next_key_length);
      }
    }
    jumpmuCatch()
    {
      undo();
      for (u32 i = mask; i; --i) {
        _mm_pause();
      }
      mask = mask < max ? mask << 1 : max;
      WorkerCounters::myCounters().dt_restarts_read[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
void BTree::insert(u8* key, u16 key_length, u64 payloadLength, u8* payload)
{
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
  volatile u32 local_restarts_counter = 0;
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
      // -------------------------------------------------------------------------------------
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      p_guard.recheck_done();
      if (c_x_guard->insert(key, key_length, ValueType(reinterpret_cast<BufferFrame*>(payloadLength)), payload)) {
        jumpmu_return;
      }
      // -------------------------------------------------------------------------------------
      // Release lock
      c_guard = std::move(c_x_guard);
      c_guard.kill();
      // -------------------------------------------------------------------------------------
      trySplit(*c_x_guard.bf);
      jumpmu_continue;
    }
    jumpmuCatch()
    {
      BACKOFF_STRATEGIES()
      WorkerCounters::myCounters().dt_restarts_structural_change[dtid]++;
      local_restarts_counter++;
    }
  }
}
// -------------------------------------------------------------------------------------
void BTree::trySplit(BufferFrame& to_split, s32 favored_split_pos)
{
  auto parent_handler = findParent(this, to_split);
  OptimisticPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
  OptimisticPageGuard<BTreeNode> c_guard = OptimisticPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
  if (c_guard->count <= 2)
    return;
  // -------------------------------------------------------------------------------------
  BTreeNode::SeparatorInfo sep_info;
  if (favored_split_pos < 0 || favored_split_pos >= c_guard->count - 1) {
    sep_info = c_guard->findSep();
  } else {
    // Split on a specified position
    sep_info = BTreeNode::SeparatorInfo{c_guard->getFullKeyLength(favored_split_pos), static_cast<u32>(favored_split_pos), false};
  }
  u8 sep_key[sep_info.length];
  if (!p_guard.hasBf()) {
    auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
    auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
    assert(height == 1 || !c_x_guard->is_leaf);
    assert(root_swip.bf == c_x_guard.bf);
    // create new root
    auto new_root = ExclusivePageGuard<BTreeNode>::allocateNewPage(dtid, false);
    auto new_left_node = ExclusivePageGuard<BTreeNode>::allocateNewPage(dtid);
    new_root.keepAlive();
    new_left_node.init(c_x_guard->is_leaf);
    new_root.init(false);
    // -------------------------------------------------------------------------------------
    new_root->upper = c_x_guard.bf;
    root_swip.swizzle(new_root.bf);
    // -------------------------------------------------------------------------------------
    c_x_guard->getSep(sep_key, sep_info);
    // -------------------------------------------------------------------------------------
    c_x_guard->split(new_root, new_left_node, sep_info.slot, sep_key, sep_info.length);
    // -------------------------------------------------------------------------------------
    height++;
    return;
  }
  unsigned spaced_need_for_separator = BTreeNode::spaceNeeded(sep_info.length, p_guard->prefix_length);
  if (p_guard->hasEnoughSpaceFor(spaced_need_for_separator)) {  // Is there enough space in the parent
                                                                // for the separator?
    auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
    auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
    p_x_guard->requestSpaceFor(spaced_need_for_separator);
    assert(p_x_guard.hasBf());
    assert(!p_x_guard->is_leaf);
    // -------------------------------------------------------------------------------------
    auto new_left_node = ExclusivePageGuard<BTreeNode>::allocateNewPage(dtid);
    new_left_node.init(c_x_guard->is_leaf);
    // -------------------------------------------------------------------------------------
    c_x_guard->getSep(sep_key, sep_info);
    // -------------------------------------------------------------------------------------
    c_x_guard->split(p_x_guard, new_left_node, sep_info.slot, sep_key, sep_info.length);
    // -------------------------------------------------------------------------------------
  } else {
    p_guard.kill();
    c_guard.kill();
    trySplit(*p_guard.bf);  // Must split parent head to make space for separator
  }
}
// -------------------------------------------------------------------------------------
void BTree::updateSameSize(u8* key, u16 key_length, function<void(u8* payload, u16 payload_size)> callback)
{
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
  volatile u32 local_restarts_counter = 0;
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard<BTreeNode> c_guard = findLeafForRead<1>(key, key_length);
      s32 pos = c_guard->lowerBound<true>(key, key_length);
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      assert(pos != -1);
      u16 payload_length = c_x_guard->getPayloadLength(pos);
      callback((c_x_guard->isLarge(pos)) ? c_x_guard->getPayloadLarge(pos) : c_x_guard->getPayload(pos), payload_length);
      // -------------------------------------------------------------------------------------
      if (FLAGS_cm_split && local_restarts_counter > 0) {
        if (utils::RandomGenerator::getRandU64(0, 100) < (FLAGS_cm_update_tracker_pct)) {
          s64 last_modified_pos = c_x_guard.bf->header.contention_tracker.last_modified_pos;
          c_x_guard.bf->header.contention_tracker.last_modified_pos = pos;
          // -------------------------------------------------------------------------------------
          c_x_guard.bf->header.contention_tracker.restarts_counter += local_restarts_counter;
          c_x_guard.bf->header.contention_tracker.access_counter++;
          const u64 current_restarts_counter = c_x_guard.bf->header.contention_tracker.restarts_counter;
          const u64 current_access_counter = c_x_guard.bf->header.contention_tracker.access_counter;
          const u64 normalized_restarts = 100.0 * current_restarts_counter / current_access_counter;
          if (utils::RandomGenerator::getRandU64(0, 100) < (FLAGS_cm_update_tracker_pct)) {
            c_x_guard.bf->header.contention_tracker.restarts_counter = 0;
            c_x_guard.bf->header.contention_tracker.access_counter = 0;
            // -------------------------------------------------------------------------------------
            if (last_modified_pos != pos && normalized_restarts >= FLAGS_restarts_threshold && c_x_guard->count > 2) {
              s32 split_pos = std::min<s32>(last_modified_pos, pos);
              c_guard = std::move(c_x_guard);
              c_guard.kill();
              jumpmuTry()
              {
                trySplit(*c_guard.bf, split_pos);
                WorkerCounters::myCounters().dt_researchy_0[dtid]++;
              }
              jumpmuCatch() {}
            }
          }
        }
      }
      jumpmu_return;
    }
    jumpmuCatch()
    {
      BACKOFF_STRATEGIES()
      local_restarts_counter++;
      WorkerCounters::myCounters().dt_restarts_update_same_size[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
// TODO: not used and not well tested
void BTree::update(u8* key, u16 key_length, u64 payloadLength, u8* payload)
{
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
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
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      p_guard.kill();
      if (c_x_guard->update(key, key_length, payloadLength, payload)) {
        jumpmu_return;
      }
      // no more space, need to split
      // -------------------------------------------------------------------------------------
      // Release lock
      c_guard = std::move(c_x_guard);
      c_guard.kill();
      // -------------------------------------------------------------------------------------
      trySplit(*c_x_guard.bf);
      jumpmu_continue;
    }
    jumpmuCatch()
    {
      for (u32 i = mask; i; --i) {
        _mm_pause();
      }
      mask = mask < max ? mask << 1 : max;
      WorkerCounters::myCounters().dt_restarts_structural_change[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
bool BTree::remove(u8* key, u16 key_length)
{
  /*
   * Plan:
   * check the right (only one) node if it is under filled
   * if yes, then lock exclusively
   * if there was not, and after deletion we got an empty
   * */
  volatile u32 mask = 1;
  u32 const max = 512;  // MAX_BACKOFF
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard c_guard = findLeafForRead<2>(key, key_length);
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      if (!c_x_guard->remove(key, key_length)) {
        jumpmu_return false;
      }
      if (c_x_guard->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
        c_guard = std::move(c_x_guard);
        c_guard.kill();
        jumpmuTry() { tryMerge(*c_guard.bf); }
        jumpmuCatch()
        {
          // nothing, it is fine not to merge
        }
      }
      jumpmu_return true;
    }
    jumpmuCatch()
    {
      for (u32 i = mask; i; --i) {
        _mm_pause();
      }
      mask = mask < max ? mask << 1 : max;
      WorkerCounters::myCounters().dt_restarts_structural_change[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
bool BTree::tryMerge(BufferFrame& to_merge, bool swizzle_sibling)
{
  auto parent_handler = findParent(this, to_merge);
  OptimisticPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
  OptimisticPageGuard<BTreeNode> c_guard = OptimisticPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
  int pos = parent_handler.pos;
  if (!p_guard.hasBf() || c_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
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
    Swip<BTreeNode>& l_swip = p_guard->getValue(pos - 1);
    if (!swizzle_sibling && !l_swip.isSwizzled()) {
      return false;
    }
    auto l_guard = OptimisticPageGuard(p_guard, l_swip);
    if (l_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
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
    Swip<BTreeNode>& r_swip = p_guard->getValue(pos + 1);
    if (!swizzle_sibling && !r_swip.isSwizzled()) {
      return false;
    }
    auto r_guard = OptimisticPageGuard(p_guard, r_swip);
    if (r_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize) {
      return false;
    }
    auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
    auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
    auto r_x_guard = ExclusivePageGuard(std::move(r_guard));
    // -------------------------------------------------------------------------------------
    assert(&p_x_guard->getValue(pos).asBufferFrame() == c_x_guard.bf);
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
  bool merged_successfully = false;
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
    if (p_guard.hasBf() && p_guard->freeSpaceAfterCompaction() >= BTreeNode::underFullSize && root_swip.bf != p_guard.bf) {
      tryMerge(*p_guard.bf, swizzle_sibling);
    }
  }
  jumpmuCatch() {}
  // -------------------------------------------------------------------------------------
  return merged_successfully;
}
// -------------------------------------------------------------------------------------
bool BTree::kWayMerge(BufferFrame& to_merge)
{
  auto parent_handler = findParent(this, to_merge);
  OptimisticPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
  OptimisticPageGuard<BTreeNode> c_guard = OptimisticPageGuard(p_guard, parent_handler.swip.cast<BTreeNode>());
  s32 pos = parent_handler.pos;
  // -------------------------------------------------------------------------------------
  assert(parent_handler.swip.bf == &to_merge);
  assert(pos != -1);
  // -------------------------------------------------------------------------------------
  bool can_we_merge = true;
  can_we_merge &= p_guard.hasBf() && c_guard->is_leaf && (c_guard->freeSpaceAfterCompaction() >= EFFECTIVE_PAGE_SIZE * FLAGS_d);
  can_we_merge &= (pos > 0) && (pos + 1) < p_guard->count;
  if (!can_we_merge) {
    p_guard.kill();
    c_guard.kill();
    return false;
  }
  // -------------------------------------------------------------------------------------
  can_we_merge &= p_guard->getValue(pos - 1).isSwizzled();
  if (!can_we_merge) {
    p_guard.kill();
    c_guard.kill();
    return false;
  }
  // -------------------------------------------------------------------------------------
  OptimisticPageGuard<BTreeNode> l_guard = OptimisticPageGuard(p_guard, p_guard->getValue(pos - 1));
  // -------------------------------------------------------------------------------------
  auto merge_left_into_right = [&](ExclusivePageGuard<BTreeNode>& parent, s32 left_pos, ExclusivePageGuard<BTreeNode>& from_left,
                                   ExclusivePageGuard<BTreeNode>& to_right) {
    // -------------------------------------------------------------------------------------
    u32 space_upper_bound = from_left->mergeSpaceUpperBound(to_right);
    if (space_upper_bound <= EFFECTIVE_PAGE_SIZE) {  // Do a full merge
      bool succ = from_left->merge(left_pos, parent, to_right);
      ensure(succ);
      WorkerCounters::myCounters().dt_researchy_2[dtid]++;
      from_left.reclaim();
      return true;
    }
    // return false;
    // -------------------------------------------------------------------------------------
    // Do a partial merge
    // Remove a key at a time from the merge and check if now it fits
    s32 till_slot_id = -1;
    for (s32 s_i = 0; s_i < from_left->count; s_i++) {
      if (from_left->slot[s_i].rest_len) {
        space_upper_bound -=
            sizeof(ValueType) + (from_left->isLarge(s_i) ? (from_left->getRestLenLarge(s_i) + sizeof(u16)) : from_left->getRestLen(s_i));
      }
      space_upper_bound -= from_left->getPayloadLength(s_i);
      if (space_upper_bound < EFFECTIVE_PAGE_SIZE * 1.0) {
        till_slot_id = s_i + 1;
        break;
      }
    }
    if (!(till_slot_id != -1 && till_slot_id < (from_left->count - 1)))
      return false;
    ensure(till_slot_id > 0);
    // -------------------------------------------------------------------------------------
    u16 copy_from_count = from_left->count - till_slot_id;
    // -------------------------------------------------------------------------------------
    u16 new_left_uf_length = from_left->getFullKeyLength(till_slot_id - 1);
    ensure(new_left_uf_length > 0);
    u8 new_left_uf_key[new_left_uf_length];
    from_left->copyFullKey(till_slot_id - 1, new_left_uf_key, new_left_uf_length);
    // -------------------------------------------------------------------------------------
    if (!parent->canInsert(new_left_uf_key, new_left_uf_length, 0))
      return false;
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
      assert(!to_right->sanityCheck(new_left_uf_key, new_left_uf_length));
    }
    {
      BTreeNode tmp(true);
      tmp.setFences(from_left->getLowerFenceKey(), from_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
      // -------------------------------------------------------------------------------------
      from_left->copyKeyValueRange(&tmp, 0, 0, from_left->count - copy_from_count);
      memcpy(reinterpret_cast<u8*>(from_left.ptr()), &tmp, sizeof(BTreeNode));
      from_left->makeHint();
      // -------------------------------------------------------------------------------------
      assert(from_left->sanityCheck(new_left_uf_key, new_left_uf_length));
      // -------------------------------------------------------------------------------------
      parent->removeSlot(left_pos);
      ensure(parent->insert(from_left->getUpperFenceKey(), from_left->upper_fence.length, from_left.swip()));
    }
    WorkerCounters::myCounters().dt_researchy_1[dtid]++;
    return true;
  };
  // -------------------------------------------------------------------------------------
  auto p_x_guard = ExclusivePageGuard(std::move(p_guard));
  auto l_x_guard = ExclusivePageGuard(std::move(l_guard));
  auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
  // -------------------------------------------------------------------------------------
  ensure(pos > 0 && pos < p_x_guard->count);
  return merge_left_into_right(p_x_guard, pos - 1, l_x_guard, c_x_guard);
}  // namespace vs
// -------------------------------------------------------------------------------------
BTree::~BTree() {}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
  DTRegistry::DTMeta btree_meta = {
      .iterate_children = iterateChildrenSwips, .find_parent = findParent, .check_space_utilization = checkSpaceUtilization};
  return btree_meta;
}
// -------------------------------------------------------------------------------------
// Called by buffer manager before eviction
void BTree::checkSpaceUtilization(void* btree_object, BufferFrame& bf)
{
  auto& c_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
  auto& btree = *reinterpret_cast<BTree*>(btree_object);
  if (FLAGS_cm_merge) {
    if (bf.page.dt_id == btree.dtid && c_node.freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
      jumpmuTry()
      {
        if (btree.tryMerge(bf, false)) {
          WorkerCounters::myCounters().dt_researchy_1[btree.dtid]++;
        } else {
          // do nothing
          jumpmu_return;
        }
      }
      jumpmuCatch()
      {
        WorkerCounters::myCounters().dt_researchy_2[btree.dtid]++;
        jumpmu::jump();
      }
    }
    return;
    // -------------------------------------------------------------------------------------
  }
  if (FLAGS_su_merge) {
    if (bf.page.dt_id == btree.dtid) {
      if (c_node.is_leaf && c_node.freeSpaceAfterCompaction() >= EFFECTIVE_PAGE_SIZE * FLAGS_d &&
          utils::RandomGenerator::getRandU64(0, 100) < FLAGS_y) {
        btree.kWayMerge(bf);
      } else {
      }
    }
  }
}
// -------------------------------------------------------------------------------------
// Should not have to swizzle any page
// Throws if the bf could not be found
struct ParentSwipHandler BTree::findParent(void* btree_object, BufferFrame& to_find)
{
  // Pre: bf is write locked TODO: but trySplit does not ex lock !
  auto& c_node = *reinterpret_cast<BTreeNode*>(to_find.page.dt);
  auto& btree = *reinterpret_cast<BTree*>(btree_object);
  // -------------------------------------------------------------------------------------
  if (btree.dtid != to_find.page.dt_id)
    jumpmu::jump();
  // -------------------------------------------------------------------------------------
  Swip<BTreeNode>* c_swip = &btree.root_swip;
  u16 level = 0;
  // -------------------------------------------------------------------------------------
  auto p_guard = OptimisticPageGuard<BTreeNode>::makeRootGuard(btree.root_lock);
  // -------------------------------------------------------------------------------------
  const bool infinity = c_node.upper_fence.offset == 0;
  u16 key_length = c_node.upper_fence.length;
  u8* key = c_node.getUpperFenceKey();
  // -------------------------------------------------------------------------------------
  // check if bf is the root node
  if (c_swip->bf == &to_find) {
    p_guard.kill();
    return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = p_guard.bf_s_lock, .parent = nullptr};
  }
  // -------------------------------------------------------------------------------------
  OptimisticPageGuard c_guard(p_guard, btree.root_swip);  // the parent of the bf we are looking for (to_find)
  s32 pos = -1;
  auto search_condition = [&]() {
    if (infinity) {
      c_swip = &(c_guard->upper);
      pos = c_guard->count;
    } else {
      pos = c_guard->lowerBound<false>(key, key_length);
      if (pos == c_guard->count) {
        c_swip = &(c_guard->upper);
      } else {
        c_swip = &(c_guard->getValue(pos));
      }
    }
    return (c_swip->bf != &to_find);
  };
  while (!c_guard->is_leaf && search_condition()) {
    p_guard = std::move(c_guard);
    c_guard = OptimisticPageGuard(p_guard, c_swip->cast<BTreeNode>());
    level++;
  }
  p_guard.kill();
  const bool found = c_swip->bf == &to_find;
  c_guard.recheck_done();
  if (!found) {
    jumpmu::jump();
  }
  return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = c_guard.bf_s_lock, .parent = c_guard.bf, .pos = pos};
}
// -------------------------------------------------------------------------------------
void BTree::iterateChildrenSwips(void*, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
  // Pre: bf is read locked
  auto& c_node = *reinterpret_cast<BTreeNode*>(bf.page.dt);
  if (c_node.is_leaf) {
    return;
  }
  for (u16 i = 0; i < c_node.count; i++) {
    if (!callback(c_node.getValue(i).cast<BufferFrame>())) {
      return;
    }
  }
  callback(c_node.upper.cast<BufferFrame>());
}
// Helpers
// -------------------------------------------------------------------------------------
s64 BTree::iterateAllPagesRec(OptimisticPageGuard<BTreeNode>& node_guard, std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf)
{
  if (node_guard->is_leaf) {
    return leaf(node_guard.ref());
  }
  s64 res = inner(node_guard.ref());
  for (u16 i = 0; i < node_guard->count; i++) {
    Swip<BTreeNode>& c_swip = node_guard->getValue(i);
    auto c_guard = OptimisticPageGuard(node_guard, c_swip);
    c_guard.recheck_done();
    res += iterateAllPagesRec(c_guard, inner, leaf);
  }
  // -------------------------------------------------------------------------------------
  Swip<BTreeNode>& c_swip = node_guard->upper;
  auto c_guard = OptimisticPageGuard(node_guard, c_swip);
  c_guard.recheck_done();
  res += iterateAllPagesRec(c_guard, inner, leaf);
  // -------------------------------------------------------------------------------------
  return res;
}
// -------------------------------------------------------------------------------------
s64 BTree::iterateAllPages(std::function<s64(BTreeNode&)> inner, std::function<s64(BTreeNode&)> leaf)
{
  while (true) {
    jumpmuTry()
    {
      auto p_guard = OptimisticPageGuard<BTreeNode>::makeRootGuard(root_lock);
      OptimisticPageGuard c_guard(p_guard, root_swip);
      jumpmu_return iterateAllPagesRec(c_guard, inner, leaf);
    }
    jumpmuCatch() {}
  }
}
// -------------------------------------------------------------------------------------
u32 BTree::countEntries()
{
  return iterateAllPages([](BTreeNode&) { return 0; }, [](BTreeNode& node) { return node.count; });
}
// -------------------------------------------------------------------------------------
u32 BTree::countPages()
{
  return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 1; });
}
// -------------------------------------------------------------------------------------
u32 BTree::countInner()
{
  return iterateAllPages([](BTreeNode&) { return 1; }, [](BTreeNode&) { return 0; });
}
// -------------------------------------------------------------------------------------
double BTree::averageSpaceUsage()
{
  ensure(false);  // TODO
}
// -------------------------------------------------------------------------------------
u32 BTree::bytesFree()
{
  return iterateAllPages([](BTreeNode& inner) { return inner.freeSpaceAfterCompaction(); },
                         [](BTreeNode& leaf) { return leaf.freeSpaceAfterCompaction(); });
}
// -------------------------------------------------------------------------------------
void BTree::printInfos(uint64_t totalSize)
{
  auto p_guard = OptimisticPageGuard<BTreeNode>::makeRootGuard(root_lock);
  OptimisticPageGuard r_guard(p_guard, root_swip);
  uint64_t cnt = countPages();
  cout << "nodes:" << cnt << " innerNodes:" << countInner() << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float)totalSize << " height:" << height
       << " rootCnt:" << r_guard->count << " bytesFree:" << bytesFree() << endl;
}
// -------------------------------------------------------------------------------------
}  // namespace vs
}  // namespace btree
}  // namespace leanstore
