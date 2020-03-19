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
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard<BTreeNode> leaf = findLeafForRead(key, key_length);
      // -------------------------------------------------------------------------------------
      DEBUG_BLOCK()
      {
        s32 sanity_check_result = leaf->sanityCheck(key, key_length);
        leaf.recheck_done();
        if (sanity_check_result != 0) {
          cout << leaf->count << endl;
        }
        ensure(sanity_check_result == 0);
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
void BTree::rangeScan(u8* start_key,
                      u16 key_length,
                      std::function<bool(u8* payload, u16 payload_length, std::function<string()>&)> callback,
                      function<void()> undo)
{
  volatile u32 mask = 1;
  u8* volatile next_key = start_key;
  volatile u16 next_key_length = key_length;
  volatile bool is_heap_freed = true;  // because at first we reuse the start_key
  while (true) {
    jumpmuTry()
    {
      OptimisticPageGuard<BTreeNode> o_leaf = findLeafForRead(next_key, next_key_length);
      while (true) {
        auto leaf = ExclusivePageGuard<BTreeNode>(std::move(o_leaf));
        s32 cur = leaf->lowerBound<false>(start_key, key_length);
        while (cur < leaf->count) {
          u16 payload_length = leaf->getPayloadLength(cur);
          u8* payload = leaf->isLarge(cur) ? leaf->getPayloadLarge(cur) : leaf->getPayload(cur);
          std::function<string()> key_extract_fn = [&]() {
            ensure(false);
            u16 key_length = leaf->getFullKeyLength(cur);
            string key(key_length, '0');
            leaf->copyFullKey(cur, reinterpret_cast<u8*>(key.data()), key_length);
            return key;
          };
          if (!callback(payload, payload_length, key_extract_fn)) {
            if (!is_heap_freed) {
              delete[] next_key;
              is_heap_freed = true;
            }
            jumpmu_return;
          }
          cur++;
        }
        // -------------------------------------------------------------------------------------
        if (!is_heap_freed) {
          delete[] next_key;
          is_heap_freed = true;
        }
        if (leaf->isUpperFenceInfinity()) {
          jumpmu_return;
        }
        // -------------------------------------------------------------------------------------
        next_key_length = leaf->upper_fence.length + 1;
        next_key = new u8[next_key_length];
        is_heap_freed = false;
        memcpy(next_key, leaf->getUpperFenceKey(), leaf->upper_fence.length);
        next_key[next_key_length - 1] = 0;
        // -------------------------------------------------------------------------------------
        o_leaf = std::move(leaf);
        o_leaf = findLeafForRead(next_key, next_key_length);
      }
    }
    jumpmuCatch()
    {
      {
        next_key = start_key;
        next_key_length = key_length;
        is_heap_freed = true;  // because at first we reuse the start_key
      }
      undo();
      BACKOFF_STRATEGIES()
      WorkerCounters::myCounters().dt_restarts_read[dtid]++;
    }
  }
}
// -------------------------------------------------------------------------------------
void BTree::insert(u8* key, u16 key_length, u64 payloadLength, u8* payload)
{
  volatile u32 mask = 1;
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
      if (c_x_guard->prepareInsert(key, key_length, ValueType(reinterpret_cast<BufferFrame*>(payloadLength)))) {
        c_x_guard->insert(key, key_length, ValueType(reinterpret_cast<BufferFrame*>(payloadLength)), payload);
        jumpmu_return;
      }
      // -------------------------------------------------------------------------------------
      // Release lock
      c_guard = std::move(c_x_guard);
      c_guard.kill();
      // -------------------------------------------------------------------------------------
      if (FLAGS_bstar)
        tryBStar(*c_guard.bf);
      else
        trySplit(*c_guard.bf);
      // -------------------------------------------------------------------------------------
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
bool BTree::tryBalanceRight(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& left, s32 l_pos)
{
  if (!parent.hasBf() || l_pos + 1 >= parent->count) {
    return false;
  }
  OptimisticPageGuard<BTreeNode> right = OptimisticPageGuard(parent, parent->getValue(l_pos + 1));
  // -------------------------------------------------------------------------------------
  // Rebalance: move key/value from end of left to the beginning of right
  const u32 total_free_space = left->freeSpaceAfterCompaction() + right->freeSpaceAfterCompaction();
  const u32 r_target_free_space = total_free_space / 2;
  BTreeNode tmp(true);
  tmp.setFences(left->getLowerFenceKey(), left->lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
  ensure(tmp.prefix_length <= right->prefix_length);
  const u32 worst_case_amplification_per_key = right->prefix_length - tmp.prefix_length;
  // -------------------------------------------------------------------------------------
  s64 r_free_space = right->freeSpaceAfterCompaction();
  r_free_space -= (worst_case_amplification_per_key * right->count);
  if (r_free_space <= 0)
    return false;
  s32 left_boundary = -1;  // exclusive
  for (s32 s_i = left->count - 1; s_i > 0; s_i--) {
    r_free_space -= left->spaceUsedBySlot(s_i);
    const u16 new_right_lf_key_length = left->getFullKeyLength(s_i);
    if ((r_free_space - ((right->lower_fence.length < left->getFullKeyLength(s_i)) ? (new_right_lf_key_length - right->lower_fence.length) : 0)) >=
        r_target_free_space) {
      left_boundary = s_i - 1;
    } else {
      break;
    }
  }
  // -------------------------------------------------------------------------------------
  if (left_boundary == -1) {
    return false;
  }
  // -------------------------------------------------------------------------------------
  u16 new_left_uf_length = left->getFullKeyLength(left_boundary);
  ensure(new_left_uf_length > 0);
  u8 new_left_uf_key[new_left_uf_length];
  left->copyFullKey(left_boundary, new_left_uf_key, new_left_uf_length);
  // -------------------------------------------------------------------------------------
  const u16 old_left_sep_space = parent->spaceUsedBySlot(l_pos);
  const u16 new_left_sep_space = parent->spaceNeeded(new_left_uf_length, left.swip());
  if (new_left_sep_space > old_left_sep_space) {
    if (!parent->hasEnoughSpaceFor(new_left_sep_space - old_left_sep_space))
      return false;
  }
  // -------------------------------------------------------------------------------------
  ExclusivePageGuard<BTreeNode> x_parent = std::move(parent);
  ExclusivePageGuard<BTreeNode> x_left = std::move(left);
  ExclusivePageGuard<BTreeNode> x_right = std::move(right);
  // -------------------------------------------------------------------------------------
  const u16 copy_from_count = left->count - (left_boundary + 1);
  // -------------------------------------------------------------------------------------
  {
    tmp = BTreeNode(true);
    // Right node
    tmp.setFences(new_left_uf_key, new_left_uf_length, x_right->getUpperFenceKey(), x_right->upper_fence.length);
    // -------------------------------------------------------------------------------------
    x_left->copyKeyValueRange(&tmp, 0, left_boundary + 1, copy_from_count);
    x_right->copyKeyValueRange(&tmp, copy_from_count, 0, x_right->count);
    memcpy(reinterpret_cast<u8*>(x_right.ptr()), &tmp, sizeof(BTreeNode));
    x_right->makeHint();
    // -------------------------------------------------------------------------------------
    // Nothing to do for the right node's separator
  }
  {
    tmp = BTreeNode(true);
    tmp.setFences(x_left->getLowerFenceKey(), x_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
    // -------------------------------------------------------------------------------------
    x_left->copyKeyValueRange(&tmp, 0, 0, x_left->count - copy_from_count);
    ensure(x_left->freeSpaceAfterCompaction() <= tmp.freeSpaceAfterCompaction());
    memcpy(reinterpret_cast<u8*>(left.ptr()), &tmp, sizeof(BTreeNode));
    x_left->makeHint();
    // -------------------------------------------------------------------------------------
  }
  {
    x_parent->removeSlot(l_pos);
    ensure(x_parent->prepareInsert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip()));
    x_parent->insert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip());
  }
  // -------------------------------------------------------------------------------------
  return true;
}
// -------------------------------------------------------------------------------------
bool BTree::tryBalanceLeft(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& right, s32 c_pos)
{
  if (!parent.hasBf() || c_pos - 1 < 0) {
    return false;
  }
  OptimisticPageGuard<BTreeNode> left = OptimisticPageGuard(parent, parent->getValue(c_pos - 1));
  // -------------------------------------------------------------------------------------
  // Rebalance: move key/value from end of left to the beginning of right
  const u32 total_free_space = left->freeSpaceAfterCompaction() + right->freeSpaceAfterCompaction();
  const u32 l_target_free_space = total_free_space / 2;
  BTreeNode tmp(true);
  tmp.setFences(left->getLowerFenceKey(), left->lower_fence.length, right->getUpperFenceKey(), right->upper_fence.length);
  ensure(tmp.prefix_length <= left->prefix_length);
  const u32 worst_case_amplification_per_key = left->prefix_length - tmp.prefix_length;
  // -------------------------------------------------------------------------------------
  s64 l_free_space = left->freeSpaceAfterCompaction();
  l_free_space -= (worst_case_amplification_per_key * left->count);
  if (l_free_space <= 0)
    return false;
  s32 right_boundary = -1;  // exclusive
  for (s32 s_i = 0; s_i < right->count - 1; s_i++) {
    l_free_space -= right->spaceUsedBySlot(s_i);
    const u16 new_left_uf_key_length = left->getFullKeyLength(s_i);
    if (l_free_space - ((new_left_uf_key_length > left->upper_fence.length) ? (new_left_uf_key_length - left->upper_fence.length) : 0) >=
        l_target_free_space) {
      right_boundary = s_i + 1;
    } else {
      break;
    }
  }
  // -------------------------------------------------------------------------------------
  if (right_boundary == -1) {
    return false;
  }
  // -------------------------------------------------------------------------------------
  u16 new_left_uf_length = right->getFullKeyLength(right_boundary - 1);
  ensure(new_left_uf_length > 0);
  u8 new_left_uf_key[new_left_uf_length];
  right->copyFullKey(right_boundary - 1, new_left_uf_key, new_left_uf_length);
  // -------------------------------------------------------------------------------------
  const u16 old_left_sep_space = parent->spaceUsedBySlot(c_pos - 1);
  const u16 new_left_sep_space = parent->spaceNeeded(new_left_uf_length, left.swip());
  if (new_left_sep_space > old_left_sep_space) {
    if (!parent->hasEnoughSpaceFor(new_left_sep_space - old_left_sep_space))
      return false;
  }
  // -------------------------------------------------------------------------------------
  ExclusivePageGuard<BTreeNode> x_parent = std::move(parent);
  ExclusivePageGuard<BTreeNode> x_left = std::move(left);
  ExclusivePageGuard<BTreeNode> x_right = std::move(right);
  // -------------------------------------------------------------------------------------
  const u16 copy_from_count = right_boundary;
  // -------------------------------------------------------------------------------------
  {
    tmp = BTreeNode(true);
    tmp.setFences(x_left->getLowerFenceKey(), x_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
    // -------------------------------------------------------------------------------------
    x_left->copyKeyValueRange(&tmp, 0, 0, x_left->count);
    right->copyKeyValueRange(&tmp, x_left->count, 0, copy_from_count);  //
    memcpy(reinterpret_cast<u8*>(left.ptr()), &tmp, sizeof(BTreeNode));
    x_left->makeHint();
    // -------------------------------------------------------------------------------------
  }
  {
    tmp = BTreeNode(true);
    // Right node
    tmp.setFences(new_left_uf_key, new_left_uf_length, x_right->getUpperFenceKey(), x_right->upper_fence.length);
    // -------------------------------------------------------------------------------------
    right->copyKeyValueRange(&tmp, 0, right_boundary, right->count - right_boundary);  //
    ensure(right->freeSpaceAfterCompaction() <= tmp.freeSpaceAfterCompaction());
    memcpy(reinterpret_cast<u8*>(x_right.ptr()), &tmp, sizeof(BTreeNode));
    x_right->makeHint();
    // -------------------------------------------------------------------------------------
    // Nothing to do for the right node's separator
  }
  {
    x_parent->removeSlot(c_pos - 1);
    ensure(x_parent->prepareInsert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip()));
    x_parent->insert(x_left->getUpperFenceKey(), x_left->upper_fence.length, left.swip());
  }
  // -------------------------------------------------------------------------------------
  return true;
}
// -------------------------------------------------------------------------------------
bool BTree::trySplitRight(OptimisticPageGuard<BTreeNode>& parent, OptimisticPageGuard<BTreeNode>& left, s32 l_pos)
{
  if (!parent.hasBf() || l_pos + 1 >= parent->count) {
    return false;
  }
  OptimisticPageGuard<BTreeNode> right = OptimisticPageGuard(parent, parent->getValue(l_pos + 1));
  // -------------------------------------------------------------------------------------
  u32 debugging_total_elements = left->count + right->count;
  // -------------------------------------------------------------------------------------
  // Split two into three
  // Choose a separator in the left node (LF_right, sep_left] fill 2/3 of a page
  // Choose another separator from the right node (sep_right, UF_right]
  // allocate a new node first with the fences (sep_left, sep_right]
  auto find_separator = [&](OptimisticPageGuard<BTreeNode>& node, u32 target_free_space, bool right_to_left) {
    s32 s_i;
    s64 free_space = node->freeSpaceAfterCompaction();  // we want to increase the free space in node
    for (s_i = right_to_left ? (node->count - 1) : 0; free_space < target_free_space && s_i >= 0 && s_i <= node->count - 1;
         (right_to_left) ? s_i-- : s_i++) {
      free_space += node->spaceUsedBySlot(s_i);
    }
    return s_i;  // exclusive
  };
  // -------------------------------------------------------------------------------------
  // lf_pos and uf_pos refer to the middle node
  s32 lf_pos = find_separator(left, 1.0 * EFFECTIVE_PAGE_SIZE / 3.0, true);
  if (lf_pos == left->count - 1)
    return false;
  u16 lf_length = left->getFullKeyLength(lf_pos);
  ensure(lf_length > 0);
  u8 lf_key[lf_length];
  left->copyFullKey(lf_pos, lf_key, lf_length);
  // -------------------------------------------------------------------------------------
  s32 uf_pos = find_separator(right, 1.0 * EFFECTIVE_PAGE_SIZE / 3.0, false) - 1;
  if (uf_pos + 1 == 0)
    return false;
  u16 uf_length = right->getFullKeyLength(uf_pos);
  ensure(lf_length > 0);
  u8 uf_key[uf_length];
  right->copyFullKey(uf_pos, uf_key, uf_length);
  // -------------------------------------------------------------------------------------
  const u16 old_left_sep_space = parent->spaceUsedBySlot(l_pos);
  const u16 new_left_sep_space = parent->spaceNeeded(lf_length, left.swip());
  const u16 new_mid_sep_space = parent->spaceNeeded(uf_length, left.swip());
  if (new_left_sep_space + new_mid_sep_space > old_left_sep_space) {
    if (!parent->hasEnoughSpaceFor(new_left_sep_space + new_mid_sep_space - old_left_sep_space))
      return false;
  }
  // -------------------------------------------------------------------------------------
  const u32 new_left_count = lf_pos + 1;
  const u32 new_right_count = right->count - (uf_pos + 1);
  const u32 new_middle_count = left->count - new_left_count + right->count - new_right_count;
  // -------------------------------------------------------------------------------------
  ExclusivePageGuard<BTreeNode> x_parent = std::move(parent);
  ExclusivePageGuard<BTreeNode> x_left = std::move(left);
  ExclusivePageGuard<BTreeNode> x_right = std::move(right);
  auto middle = ExclusivePageGuard<BTreeNode>::allocateNewPage(dtid);
  middle.init(true);
  // -------------------------------------------------------------------------------------
  middle->setFences(lf_key, lf_length, uf_key, uf_length);
  left->copyKeyValueRange(middle.ptr(), 0, new_left_count, left->count - new_left_count);
  right->copyKeyValueRange(middle.ptr(), left->count - new_left_count, 0, right->count - new_right_count);
  middle->makeHint();
  ensure(middle->count == new_middle_count);
  // -------------------------------------------------------------------------------------
  {
    BTreeNode tmp(true);
    tmp.setFences(left->getLowerFenceKey(), left->lower_fence.length, lf_key, lf_length);
    left->copyKeyValueRange(&tmp, 0, 0, new_left_count);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
    memcpy(left.ptr(), &tmp, sizeof(BTreeNode));
#pragma GCC diagnostic pop
    left->makeHint();
    ensure(left->count == new_left_count);
  }
  // -------------------------------------------------------------------------------------
  {
    BTreeNode tmp(true);
    tmp.setFences(uf_key, uf_length, right->getUpperFenceKey(), right->upper_fence.length);
    right->copyKeyValueRange(&tmp, 0, uf_pos + 1, new_right_count);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
    memcpy(right.ptr(), &tmp, sizeof(BTreeNode));
#pragma GCC diagnostic pop
    right->makeHint();
    ensure(right->count == new_right_count);
  }
  // -------------------------------------------------------------------------------------
  {
    parent->removeSlot(l_pos);
    // Right swip stay as it is
    ensure(parent->prepareInsert(middle->getUpperFenceKey(), middle->upper_fence.length, middle.swip()));
    parent->insert(middle->getUpperFenceKey(), middle->upper_fence.length, middle.swip());
    ensure(parent->prepareInsert(left->getUpperFenceKey(), left->upper_fence.length, left.swip()));
    parent->insert(left->getUpperFenceKey(), left->upper_fence.length, left.swip());
  }
  // -------------------------------------------------------------------------------------
  static_cast<void>(debugging_total_elements);
  assert(debugging_total_elements == (left->count + middle->count + right->count));
  return true;
}
// -------------------------------------------------------------------------------------
void BTree::tryBStar(BufferFrame& bf)
{
  auto parent_handler = findParent(this, bf);
  OptimisticPageGuard<BTreeNode> parent = parent_handler.getParentReadPageGuard<BTreeNode>();
  OptimisticPageGuard<BTreeNode> center = OptimisticPageGuard(parent, parent_handler.swip.cast<BTreeNode>());
  // -------------------------------------------------------------------------------------
  if (tryBalanceRight(parent, center, parent_handler.pos))
    return;
  if (tryBalanceLeft(parent, center, parent_handler.pos))
    return;
  if (trySplitRight(parent, center, parent_handler.pos))
    return;
  if (parent_handler.pos - 1 >= 0) {
    // to reuse trySplitRight
    center = OptimisticPageGuard(parent, parent->getValue(parent_handler.pos - 1));
    if (trySplitRight(parent, center, parent_handler.pos - 1))
      return;
  }
  // -------------------------------------------------------------------------------------
  parent.kill();
  center.kill();
  trySplit(bf);
  return;
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
    // Split on a specified position, used by contention management
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
  volatile u32 local_restarts_counter = 0;
  while (true) {
    jumpmuTry()
    {
      // -------------------------------------------------------------------------------------
      OptimisticPageGuard<BTreeNode> c_guard = findLeafForRead<10>(key, key_length);
      auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
      s32 pos = c_x_guard->lowerBound<true>(key, key_length);
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
                WorkerCounters::myCounters().cm_split_succ_counter[dtid]++;
              }
              jumpmuCatch() { WorkerCounters::myCounters().cm_split_fail_counter[dtid]++; }
            }
          }
        }
      } else {
        c_guard = std::move(c_x_guard);
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
// TODO:
void BTree::update(u8*, u16, u64, u8*)
{
  ensure(false);
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
      BACKOFF_STRATEGIES()
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
    p_guard.kill();
    c_guard.kill();
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
      l_guard.kill();
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
      r_guard.kill();
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
    if (p_guard.hasBf() && p_guard->freeSpaceAfterCompaction() >= BTreeNode::underFullSize && root_swip.bf != p_guard.bf) {
      tryMerge(*p_guard.bf, swizzle_sibling);
    }
  }
  jumpmuCatch() {}
  // -------------------------------------------------------------------------------------
  return merged_successfully;
}
// -------------------------------------------------------------------------------------
// ret: 0 did nothing, 1 full, 2 partial
s32 BTree::mergeLeftIntoRight(ExclusivePageGuard<BTreeNode>& parent,
                              s32 left_pos,
                              ExclusivePageGuard<BTreeNode>& from_left,
                              ExclusivePageGuard<BTreeNode>& to_right,
                              bool full_merge_or_nothing)
{
  // TODO: corner cases: new upper fence is larger than the older one.
  u32 space_upper_bound = from_left->mergeSpaceUpperBound(to_right);
  if (space_upper_bound <= EFFECTIVE_PAGE_SIZE) {  // Do a full merge TODO: threshold
    bool succ = from_left->merge(left_pos, parent, to_right);
    ensure(succ);
    from_left.reclaim();
    return 1;
  }
  if (full_merge_or_nothing)
    return 0;
  // -------------------------------------------------------------------------------------
  // Do a partial merge
  // Remove a key at a time from the merge and check if now it fits
  s32 till_slot_id = -1;
  for (s32 s_i = 0; s_i < from_left->count; s_i++) {
    if (from_left->slot[s_i].rest_len) {
      space_upper_bound -= (from_left->isLarge(s_i) ? (from_left->getRestLenLarge(s_i) + sizeof(u16)) : from_left->getRestLen(s_i));
    }
    space_upper_bound -= sizeof(ValueType) + sizeof(BTreeNode::Slot) + from_left->getPayloadLength(s_i);
    if (space_upper_bound < EFFECTIVE_PAGE_SIZE * 1.0) {
      till_slot_id = s_i + 1;
      break;
    }
  }
  if (!(till_slot_id != -1 && till_slot_id < (from_left->count - 1)))
    return 0;  // false
  ensure(till_slot_id > 0);
  // -------------------------------------------------------------------------------------
  u16 copy_from_count = from_left->count - till_slot_id;
  // -------------------------------------------------------------------------------------
  u16 new_left_uf_length = from_left->getFullKeyLength(till_slot_id - 1);
  ensure(new_left_uf_length > 0);
  u8 new_left_uf_key[new_left_uf_length];
  from_left->copyFullKey(till_slot_id - 1, new_left_uf_key, new_left_uf_length);
  // -------------------------------------------------------------------------------------
  if (!parent->prepareInsert(new_left_uf_key, new_left_uf_length, 0))
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
    assert(to_right->sanityCheck(new_left_uf_key, new_left_uf_length) == 1);
  }
  {
    BTreeNode tmp(true);
    tmp.setFences(from_left->getLowerFenceKey(), from_left->lower_fence.length, new_left_uf_key, new_left_uf_length);
    // -------------------------------------------------------------------------------------
    from_left->copyKeyValueRange(&tmp, 0, 0, from_left->count - copy_from_count);
    memcpy(reinterpret_cast<u8*>(from_left.ptr()), &tmp, sizeof(BTreeNode));
    from_left->makeHint();
    // -------------------------------------------------------------------------------------
    assert(from_left->sanityCheck(new_left_uf_key, new_left_uf_length) == 0);
    // -------------------------------------------------------------------------------------
    parent->removeSlot(left_pos);
    ensure(parent->prepareInsert(from_left->getUpperFenceKey(), from_left->upper_fence.length, from_left.swip()));
    parent->insert(from_left->getUpperFenceKey(), from_left->upper_fence.length, from_left.swip());
  }
  return 2;
}
// -------------------------------------------------------------------------------------
// returns true if it has exclusively locked anything
bool BTree::kWayMerge(OptimisticPageGuard<BTreeNode>& p_guard, OptimisticPageGuard<BTreeNode>& c_guard, ParentSwipHandler& parent_handler)
{
  if (c_guard->fillFactorAfterCompaction() >= 0.9) {  // || utils::RandomGenerator::getRandU64(0, 100) >= FLAGS_y
    return false;
  }
  // -------------------------------------------------------------------------------------
  constexpr u8 MAX_MERGE_PAGES = 5;
  s32 pos = parent_handler.pos;
  u8 pages_count = 1;
  s32 max_right;
  OptimisticPageGuard<BTreeNode> guards[MAX_MERGE_PAGES];
  bool fully_merged[MAX_MERGE_PAGES];
  // -------------------------------------------------------------------------------------
  guards[0] = std::move(c_guard);
  fully_merged[0] = false;
  double total_fill_factor = guards[0]->fillFactorAfterCompaction();
  // -------------------------------------------------------------------------------------
  // Handle upper swip instead of avoiding p_guard->count -1 swip
  if (!p_guard.hasBf() || !guards[0]->is_leaf)
    return false;
  for (max_right = pos + 1; (max_right - pos) < MAX_MERGE_PAGES && (max_right + 1) < p_guard->count; max_right++) {
    if (!p_guard->getValue(max_right).isSwizzled())
      return false;
    // -------------------------------------------------------------------------------------
    guards[max_right - pos] = OptimisticPageGuard<BTreeNode>(p_guard, p_guard->getValue(max_right));
    fully_merged[max_right - pos] = false;
    total_fill_factor += guards[max_right - pos]->fillFactorAfterCompaction();
    pages_count++;
    if ((pages_count - std::ceil(total_fill_factor)) >= (1)) {
      break;
    }
  }
  if (((pages_count - std::ceil(total_fill_factor))) < (1)) {
    return false;
  }
  // -------------------------------------------------------------------------------------
  ExclusivePageGuard<BTreeNode> p_x_guard = std::move(p_guard);
  // -------------------------------------------------------------------------------------
  s32 left_hand, right_hand, ret;
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
        WorkerCounters::myCounters().su_merge_full_counter[dtid]++;
      } else if (ret == 2) {
        guards[left_hand - pos] = std::move(left_x_guard);
        WorkerCounters::myCounters().su_merge_partial_counter[dtid]++;
      } else if (ret == 0) {
        break;
      } else {
        ensure(false);
      }
    }
    // -------------------------------------------------------------------------------------
  }
  return true;
}
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
// Returns true if the buffer manager has to restart and pick another buffer frame for eviction
// Attention: the guards here down the stack are not synchronized with the ones in the buffer frame manager stack frame
bool BTree::checkSpaceUtilization(void* btree_object, BufferFrame& bf, OptimisticGuard guard, ParentSwipHandler parent_handler)
{
  auto& btree = *reinterpret_cast<BTree*>(btree_object);
  OptimisticPageGuard<BTreeNode> p_guard = parent_handler.getParentReadPageGuard<BTreeNode>();
  OptimisticPageGuard<BTreeNode> c_guard = OptimisticPageGuard<BTreeNode>::manuallyAssembleGuard(std::move(guard), &bf);
  // -------------------------------------------------------------------------------------
  if (FLAGS_su_merge) {
    bool merged = btree.kWayMerge(p_guard, c_guard, parent_handler);
    p_guard.kill();
    c_guard.kill();
    return merged;
  }
  p_guard.kill();
  c_guard.kill();
  return false;
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
    p_guard.recheck_done();
    return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(p_guard.bf_s_lock), .parent_bf = nullptr};
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
  return {.swip = c_swip->cast<BufferFrame>(), .parent_guard = std::move(c_guard.bf_s_lock), .parent_bf = c_guard.bf, .pos = pos};
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
