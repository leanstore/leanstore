#include "storage/btree/node.h"
#include "common/exceptions.h"
#include "common/utils.h"

#include "fmt/ranges.h"
#include "share_headers/logger.h"

#include <algorithm>
#include <cstring>

namespace leanstore::storage {

// -------------------------------------------------------------------------------------
BTreeNodeHeader::BTreeNodeHeader(bool is_leaf) : is_leaf(is_leaf) {}

auto BTreeNodeHeader::HasRightNeighbor() -> bool { return next_leaf_node != BTreeNodeHeader::EMPTY_NEIGHBOR; }

auto BTreeNodeHeader::IsLowerFenceInfinity() -> bool { return lower_fence.len == 0; };

// -------------------------------------------------------------------------------------
template <class NodeHeader>
BTreeNodeImpl<NodeHeader>::BTreeNodeImpl(bool is_leaf) : header(is_leaf) {}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::Ptr() -> u8 * {
  return reinterpret_cast<u8 *>(this);
};

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::IsInner() -> bool {
  return !header.is_leaf;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetPrefix() -> u8 * {
  return Ptr() + header.lower_fence.offset;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetHead(u8 *key, leng_t key_len) -> std::array<u8, 4> {
  std::array<u8, 4> ret = {0};
  std::memcpy(ret.data(), key, std::min(key_len, static_cast<leng_t>(4)));
  return ret;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FindChild(std::span<u8> search_key, leng_t &pos,
                                          const ComparisonLambda &cmp) -> pageid_t {
  pos = LowerBound(search_key, cmp);
  return (pos == header.count) ? header.right_most_child : GetChild(pos);
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FindChild(std::span<u8> search_key, const ComparisonLambda &cmp) -> pageid_t {
  leng_t unused;
  return FindChild(search_key, unused, cmp);
}

/**
 * @brief Find length of the common prefix of two slots: lhs_slot & rhs_slot
 */
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::CommonPrefix(leng_t lhs_slot, leng_t rhs_slot) -> leng_t {
  assert(lhs_slot < header.count && rhs_slot < header.count);
  leng_t limit = std::min(slots[lhs_slot].key_length, slots[rhs_slot].key_length);
  u8 *lhs_key  = GetKey(lhs_slot);
  u8 *rhs_key  = GetKey(rhs_slot);
  leng_t idx;
  for (idx = 0; idx < limit; idx++) {
    if (lhs_key[idx] != rhs_key[idx]) { break; }
  }
  return idx;
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::GetSeparatorKey(u8 *out_separator_key, const SeparatorInfo &info) {
  std::memcpy(out_separator_key, GetPrefix(), header.prefix_len);
  std::memcpy(out_separator_key + header.prefix_len, GetKey(info.slot + static_cast<u8>(info.is_truncated)),
              info.len - header.prefix_len);
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::KeyToString(leng_t slot_id) -> std::string {
  auto key_info = std::span<u8>{GetKey(slot_id), slots[slot_id].key_length};
  return fmt::format("{}", key_info);
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::ToString() -> std::string {
  auto prefix = std::span<u8>{GetPrefix(), header.prefix_len};
  std::string extra_metadata;
  if (header.is_leaf) {
    extra_metadata = "NextSibling(" + std::to_string(header.next_leaf_node) + ")";
  } else {
    extra_metadata = "RightMostChild(" + std::to_string(header.right_most_child) + ")";
  }
  std::string all_keys;
  for (size_t idx = 0; idx < header.count; idx++) {
    if (idx > 0) { all_keys += (idx % 5 == 0) ? ",\n\t\t" : ","; }
    all_keys += KeyToString(idx);
  }
  std::string payload;
  for (size_t idx = 0; idx < header.count; idx++) {
    if (header.is_leaf) {
      if (idx > 0) { payload += (idx % 5 == 0) ? ",\n\t\t" : ","; }
      payload += fmt::format("{}", GetPayload(idx));
    } else {
      if (idx > 0) { payload += (idx % 10 == 0) ? ",\n\t\t" : ","; }
      payload += std::to_string(GetChild(idx));
    }
  }

  return fmt::format(
    "<\n\tHeader: p_gsn({:d})\n\tMetadata: IsLeaf({}) - Count({:d}) - Free space({:d}) - {}"
    "\n\tPrefix({}) - LowerFences({}) - UpperFences({})\n\tKeys: [{}]\n\tPayloads: [{}]\n>",
    p_gsn, header.is_leaf, header.count, FreeSpaceAfterCompaction(), extra_metadata, prefix, GetLowerFence(),
    GetUpperFence(), all_keys, payload);
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetLowerFence() -> std::span<u8> {
  return {Ptr() + header.lower_fence.offset, header.lower_fence.len};
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetUpperFence() -> std::span<u8> {
  return {Ptr() + header.upper_fence.offset, header.upper_fence.len};
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::InsertFence(FenceKeySlot &fk, std::span<u8> key) {
  assert(FreeSpace() >= key.size());
  header.data_offset -= key.size();
  header.space_used += key.size();
  fk.offset = header.data_offset;
  fk.len    = key.size();
  std::memcpy(Ptr() + header.data_offset, key.data(), key.size());
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::SetFences(std::span<u8> lower, std::span<u8> upper, const ComparisonLambda &cmp) {
  InsertFence(header.lower_fence, lower);
  InsertFence(header.upper_fence, upper);
  if (cmp.op == ComparisonOperator::MEMCMP) {
    // Prefix optimization is only for MEMCMP
    for (header.prefix_len = 0; (header.prefix_len < std::min(lower.size(), upper.size())) &&
                                (lower[header.prefix_len] == upper[header.prefix_len]);
         header.prefix_len++) {}
  }
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::Compactify(const ComparisonLambda &cmp) {
  [[maybe_unused]] auto space_after_compacted = FreeSpaceAfterCompaction();
  // Clone this -> tmp without trash
  BTreeNodeImpl<NodeHeader> tmp(header.is_leaf);
  tmp.SetFences(GetLowerFence(), GetUpperFence(), cmp);
  CopyKeyValueRange(&tmp, 0, 0, header.count);
  tmp.header.right_most_child = header.right_most_child;
  // Clone tmp back into this, and then generate hint again
  CopyNodeContent(this, &tmp);
  MakeHint();
  assert(FreeSpace() == space_after_compacted);
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FreeSpace() -> leng_t {
  return header.data_offset - (reinterpret_cast<u8 *>(slots + header.count) - Ptr());
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FreeSpaceAfterCompaction() -> leng_t {
  return PAGE_SIZE - (reinterpret_cast<u8 *>(slots + header.count) - Ptr()) - header.space_used;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::SpaceRequiredForKV(leng_t key_len, leng_t payload_len) -> leng_t {
  return sizeof(PageSlot) + (key_len - header.prefix_len) + payload_len;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::HasSpaceForKV(leng_t key_len, leng_t payload_len) -> bool {
  return SpaceRequiredForKV(key_len, payload_len) <= FreeSpaceAfterCompaction();
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetKey(leng_t slot_id) -> u8 * {
  return Ptr() + slots[slot_id].offset;
}

/* Return memory addr of the payload */
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetPayload(leng_t slot_id) -> std::span<u8> {
  return {Ptr() + slots[slot_id].offset + slots[slot_id].key_length, slots[slot_id].payload_length};
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::GetChild(leng_t slot_id) -> pageid_t {
  return LoadUnaligned<pageid_t>(GetPayload(slot_id).data());
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::MakeHint() {
  auto dist = header.count / (NodeHeader::HINT_COUNT + 1);
  for (size_t idx = 0; idx < NodeHeader::HINT_COUNT; idx++) { header.hints[idx] = slots[dist * (idx + 1)].head; }
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::UpdateHint(leng_t slot_id) {
  auto dist  = header.count / (NodeHeader::HINT_COUNT + 1);
  auto begin = 0;
  if ((header.count > NodeHeader::HINT_COUNT * 2 + 1) &&
      (((header.count - 1) / (NodeHeader::HINT_COUNT + 1)) == dist) && ((slot_id / dist) > 1)) {
    begin = (slot_id / dist) - 1;
  }

  for (size_t idx = begin; idx < NodeHeader::HINT_COUNT; idx++) { header.hints[idx] = slots[dist * (idx + 1)].head; }
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::SearchHint(const std::array<u8, 4> &key_head, leng_t &lower_bound,
                                           leng_t &upper_bound) {
  if (header.count > NodeHeader::HINT_COUNT * 2) {
    leng_t dist = upper_bound / (NodeHeader::HINT_COUNT + 1);
    leng_t lower_hint_p;
    leng_t upper_hint_p;
    for (lower_hint_p = 0; lower_hint_p < NodeHeader::HINT_COUNT; lower_hint_p++) {
      if (std::memcmp(header.hints[lower_hint_p].data(), key_head.data(), 4) >= 0) { break; }
    }

    for (upper_hint_p = lower_hint_p; upper_hint_p < NodeHeader::HINT_COUNT; upper_hint_p++) {
      if (std::memcmp(header.hints[upper_hint_p].data(), key_head.data(), 4) != 0) { break; }
    }

    lower_bound = lower_hint_p * dist;
    if (upper_hint_p < NodeHeader::HINT_COUNT) { upper_bound = (upper_hint_p + 1) * dist; }
  }
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::LowerBound(std::span<u8> search_key, bool &exact_found,
                                           const ComparisonLambda &cmp) -> leng_t {
  exact_found = false;

  int ret = 0;
  if (cmp.op == ComparisonOperator::MEMCMP) {
    // evaluate if the search key is within the prefix of the current node (i.e. prefix of all records inside this node)
    ret = cmp.func(search_key.data(), GetPrefix(), std::min(search_key.size(), static_cast<size_t>(header.prefix_len)));
    if (ret < 0) { return 0; }
    if (ret > 0) { return header.count; }
  }

  // key is a substring of the prefix
  if (search_key.size() < header.prefix_len) { return 0; }

  u8 *key         = search_key.data() + header.prefix_len;
  auto key_length = search_key.size() - header.prefix_len;

  // check hint
  leng_t lower  = 0;             // inclusive
  leng_t upper  = header.count;  // exclusive
  auto key_head = GetHead(key, key_length);
  SearchHint(key_head, lower, upper);

  // binary search on remaining range
  while (lower < upper) {
    auto mid = ((upper - lower) / 2) + lower;
    if (std::memcmp(key_head.data(), slots[mid].head.data(), 4) < 0) {
      upper = mid;
    } else if (std::memcmp(key_head.data(), slots[mid].head.data(), 4) > 0) {
      lower = mid + 1;
    } else {
      ret = cmp.func(key, GetKey(mid), std::min(key_length, static_cast<size_t>(slots[mid].key_length)));
      if (ret < 0) {
        upper = mid;
      } else if (ret > 0) {
        lower = mid + 1;
      } else {
        // full key is equal to GetKey(mid), now compare their length
        assert(ret == 0);
        if (key_length < slots[mid].key_length) {  // key is shorter
          upper = mid;
        } else if (key_length > slots[mid].key_length) {  // key is longer
          lower = mid + 1;
        } else {
          exact_found = true;
          return mid;
        }
      }
    }
  }
  return lower;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::LowerBound(std::span<u8> search_key, const ComparisonLambda &cmp) -> leng_t {
  bool ignore;
  return LowerBound(search_key, ignore, cmp);
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::StoreRecordData(leng_t slot_id, std::span<u8> full_key, std::span<const u8> payload) {
  u8 *key         = full_key.data() + header.prefix_len;
  auto key_length = full_key.size() - header.prefix_len;
  StoreRecordDataWithoutPrefix(slot_id, {key, key_length}, payload);
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::StoreRecordDataWithoutPrefix(leng_t slot_id, std::span<u8> key_no_prefix,
                                                             std::span<const u8> payload) {
  u8 *key             = key_no_prefix.data();
  auto required_space = key_no_prefix.size() + payload.size();
  // update page metadata
  header.data_offset -= required_space;
  header.space_used += required_space;
  // update slots info of this key
  slots[slot_id] = (PageSlot){header.data_offset, static_cast<leng_t>(key_no_prefix.size()),
                              static_cast<leng_t>(payload.size()), BTreeNode::GetHead(key, key_no_prefix.size())};
  assert(GetKey(slot_id) >= reinterpret_cast<u8 *>(&slots[slot_id]));
  // copy record content into the page
  std::memcpy(GetKey(slot_id), key, key_no_prefix.size());
  std::memcpy(GetPayload(slot_id).data(), payload.data(), payload.size());
}

/**
 * @brief Insert new record into the Btree Page
 *        We should have the full key here (i.e. with prefix)
 */
template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::InsertKeyValue(std::span<u8> key, std::span<const u8> payload,
                                               const ComparisonLambda &cmp) {
  auto space_needed = SpaceRequiredForKV(key.size(), payload.size());
  if (space_needed > FreeSpace()) {
    assert(space_needed <= FreeSpaceAfterCompaction());
    Compactify(cmp);
  }
  bool found;
  auto slot_id = LowerBound(key, found, cmp);
  if (found) { return; }
  std::move_backward(&slots[slot_id], &slots[header.count], &slots[header.count + 1]);
  StoreRecordData(slot_id, key, payload);
  header.count++;
  UpdateHint(slot_id);
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::RemoveSlot(leng_t slot_id) -> bool {
  header.space_used -= slots[slot_id].key_length;
  header.space_used -= slots[slot_id].payload_length;
  std::move(&slots[slot_id + 1], &slots[header.count], &slots[slot_id]);
  header.count--;
  MakeHint();
  return true;
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::RemoveKey(std::span<u8> key, const ComparisonLambda &cmp) -> bool {
  bool found;
  auto slot_id = LowerBound(key, found, cmp);
  return (found) ? RemoveSlot(slot_id) : false;
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::CopyKeyValue(BTreeNodeImpl<NodeHeader> *dst, leng_t src_slot, leng_t dst_slot) {
  size_t full_key_length = slots[src_slot].key_length + header.prefix_len;
  u8 full_key[full_key_length];
  std::memcpy(full_key, GetPrefix(), header.prefix_len);
  std::memcpy(full_key + header.prefix_len, GetKey(src_slot), slots[src_slot].key_length);
  dst->StoreRecordData(dst_slot, {full_key, full_key_length}, GetPayload(src_slot));
  dst->header.count++;
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::CopyNodeContent(BTreeNodeImpl<NodeHeader> *dst, BTreeNodeImpl<NodeHeader> *src) {
  std::memcpy(reinterpret_cast<u8 *>(dst), reinterpret_cast<u8 *>(src), sizeof(BTreeNode));
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::CopyKeyValueRange(BTreeNodeImpl<NodeHeader> *dst, leng_t dst_slot, leng_t src_slot,
                                                  leng_t src_count) {
  // prefix grows
  assert(std::memcmp(GetPrefix(), dst->GetPrefix(), std::min(header.prefix_len, dst->header.prefix_len)) == 0);
  if (header.prefix_len <= dst->header.prefix_len) {
    auto diff = dst->header.prefix_len - header.prefix_len;
    for (auto idx = 0; idx < src_count; idx++) {
      auto new_key_length = slots[src_slot + idx].key_length - diff;
      dst->StoreRecordDataWithoutPrefix(dst_slot + idx,
                                        {GetKey(src_slot + idx) + diff, static_cast<size_t>(new_key_length)},
                                        GetPayload(src_slot + idx));
    }
    dst->header.count += src_count;
  } else {
    for (auto idx = 0; idx < src_count; idx++) { CopyKeyValue(dst, src_slot + idx, dst_slot + idx); }
  }
  assert((dst->Ptr() + dst->header.data_offset) >= reinterpret_cast<u8 *>(dst->slots + dst->header.count));
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FindSeparator(bool append_bias, const ComparisonLambda &cmp) -> SeparatorInfo {
  assert(header.count > 1);
  if (IsInner()) {
    // Always split inner nodes in the middle
    leng_t slot_id = header.count / 2;
    return SeparatorInfo{static_cast<leng_t>(header.prefix_len + slots[slot_id].key_length), slot_id, false};
  }

  // Now find good (i.e. shortest) separator for leaf node
  assert(header.is_leaf);
  leng_t best_slot;
  leng_t best_prefix_len;

  if (append_bias) {
    best_slot = header.count - 2;
  } else if (header.count > 16 && cmp.op == ComparisonOperator::MEMCMP) {
    // Prefix optimization is only for MEMCMP
    leng_t lower = (header.count / 2) - (header.count / 16);
    leng_t upper = (header.count / 2);

    // Find the shortest separator, thus after split,
    //  the prefix_len of two split nodes are as long as possible
    best_prefix_len = CommonPrefix(lower, 0);
    best_slot       = lower;

    if (best_prefix_len != CommonPrefix(upper - 1, 0)) {
      for (best_slot = lower + 1; (best_slot < upper) && (CommonPrefix(best_slot, 0) == best_prefix_len); best_slot++) {
      }
    }
  } else {
    best_slot = (header.count - 1) / 2;
  }

  /**
   * @brief try to truncate separator, so that
   * - the separator is as short as possible (no need to store the whole key)
   * - the smallest key of right node is larger than the separator
   * - the largest key of left node is smaller than the separator
   *
   * e.g. if the keys at two best slots are "aaaz" and "abaa",
   *  then a separator of "ab" suits all the above criteria
   */
  if (cmp.op == ComparisonOperator::MEMCMP) {
    auto common = CommonPrefix(best_slot, best_slot + 1);
    if ((best_slot + 1 < header.count) && (slots[best_slot].key_length > common) &&
        (slots[best_slot + 1].key_length > (common + 1))) {
      return SeparatorInfo{static_cast<leng_t>(header.prefix_len + common + 1), best_slot, true};
    }
  }

  return SeparatorInfo{static_cast<leng_t>(header.prefix_len + slots[best_slot].key_length), best_slot, false};
}

template <class NodeHeader>
void BTreeNodeImpl<NodeHeader>::SplitNode(BTreeNodeImpl<NodeHeader> *parent, BTreeNodeImpl<NodeHeader> *node_right,
                                          pageid_t left_pid, pageid_t right_pid, leng_t separator_slot,
                                          std::span<u8> sep_key, const ComparisonLambda &cmp) {
  assert(separator_slot > 0);
  assert(separator_slot < (PAGE_SIZE / sizeof(pageid_t)));

  BTreeNodeImpl<NodeHeader> tmp(header.is_leaf);
  BTreeNodeImpl<NodeHeader> *node_left = &tmp;

  // update_fence of two child nodes
  node_left->SetFences(GetLowerFence(), sep_key, cmp);
  node_right->SetFences(sep_key, GetUpperFence(), cmp);

  // update the separator in parent node
  leng_t old_parent_slot = parent->LowerBound(sep_key, cmp);
  if (old_parent_slot == parent->header.count) {
    assert(parent->header.right_most_child == left_pid);
    parent->header.right_most_child = right_pid;
  } else {
    assert(parent->GetChild(old_parent_slot) == left_pid);
    std::memcpy(parent->GetPayload(old_parent_slot).data(), &right_pid, sizeof(pageid_t));
  }
  parent->InsertKeyValue(sep_key, {reinterpret_cast<const u8 *>(&left_pid), sizeof(pageid_t)}, cmp);

  // update content of two child nodes
  if (header.is_leaf) {
    CopyKeyValueRange(node_left, 0, 0, separator_slot + 1);
    CopyKeyValueRange(node_right, 0, node_left->header.count, header.count - node_left->header.count);
    node_left->header.next_leaf_node  = right_pid;
    node_right->header.next_leaf_node = header.next_leaf_node;
  } else {
    // in inner node split, moves the separator key move to parent node
    // i.e., count == 1 + nodeLeft->count + nodeRight->count
    CopyKeyValueRange(node_left, 0, 0, separator_slot);
    CopyKeyValueRange(node_right, 0, node_left->header.count + 1, header.count - node_left->header.count - 1);
    node_left->header.right_most_child  = GetChild(node_left->header.count);
    node_right->header.right_most_child = header.right_most_child;
  }
  node_left->MakeHint();
  node_right->MakeHint();

  // move tmp data back into this node
  CopyNodeContent(this, node_left);
}

/**
 * @brief Merge current node (i.e. left node) into right node
 *        If this return true, left node should be deallocated
 */
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::MergeNodes(leng_t left_slot_id, BTreeNodeImpl<NodeHeader> *parent,
                                           BTreeNodeImpl<NodeHeader> *right, const ComparisonLambda &cmp) -> bool {
  BTreeNodeImpl<NodeHeader> tmp(header.is_leaf);
  tmp.SetFences(GetLowerFence(), right->GetUpperFence(), cmp);

  if (header.is_leaf) {
    assert((right->header.is_leaf) && (parent->IsInner()));
    // calculate the upper bound on space used of the new node
    auto space_upper_bound =
      sizeof(BTreeNodeHeader) +                                     // size of BTreeNodeHeader (i.e. metadata)
      header.space_used + right->header.space_used +                // size of all keys + their payload
      (header.prefix_len - tmp.header.prefix_len) * header.count +  //  grow from prefix compression for left node
      (right->header.prefix_len - tmp.header.prefix_len) * right->header.count +  // same as above for right node
      sizeof(PageSlot) * (header.count + right->header.count);                    // size of all slots
    if (space_upper_bound > PAGE_SIZE) { return false; }
    // start moving entries to new node
    CopyKeyValueRange(&tmp, 0, 0, header.count);
    right->CopyKeyValueRange(&tmp, header.count, 0, right->header.count);
    // update metadata for new node
    tmp.header.next_leaf_node = right->header.next_leaf_node;
  } else {
    assert((right->IsInner()) && (parent->IsInner()));
    // calculate the upper bound on space used of the new node
    auto extra_key_len = parent->header.prefix_len + parent->slots[left_slot_id].key_length;
    auto space_upper_bound =
      sizeof(BTreeNodeHeader) +                                     // size of BTreeNodeHeader (i.e. metadata)
      header.space_used + right->header.space_used +                // size of all keys + their payload
      (header.prefix_len - tmp.header.prefix_len) * header.count +  //  grow from prefix compression for left node
      (right->header.prefix_len - tmp.header.prefix_len) * right->header.count +  // same as above for right node
      sizeof(PageSlot) * (header.count + right->header.count) +                   // size of all slots

      // separator retrieved from parent
      tmp.SpaceRequiredForKV(extra_key_len, sizeof(pageid_t));
    if (space_upper_bound > PAGE_SIZE) { return false; }
    // start moving entries to new node
    CopyKeyValueRange(&tmp, 0, 0, header.count);
    // extra key are retrieved from
    // {parent->full_key[left_slot_id], left->right_most_child}
    std::memcpy(parent->GetPayload(left_slot_id).data(), &(header.right_most_child), sizeof(pageid_t));
    parent->CopyKeyValue(&tmp, left_slot_id, header.count);
    // now copy the rest from original right child
    right->CopyKeyValueRange(&tmp, tmp.header.count, 0, right->header.count);
    // update right_most_child of new node
    tmp.header.right_most_child = right->header.right_most_child;
  }
  // update metadata of new node
  tmp.MakeHint();
  CopyNodeContent(right, &tmp);
  // update parent's entries
  parent->RemoveSlot(left_slot_id);
  return true;
}

// -------------------------------------------------------------------------------------
template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::FindChildWithBlobKey(const blob::BlobLookupKey &key, leng_t &pos,
                                                     const ComparisonLambda &cmp) -> pageid_t {
  bool unused;
  pos = LowerBoundWithBlobKey(key, unused, cmp);
  return (pos == header.count) ? header.right_most_child : GetChild(pos);
}

template <class NodeHeader>
auto BTreeNodeImpl<NodeHeader>::LowerBoundWithBlobKey(const blob::BlobLookupKey &key, bool &exact_found,
                                                      const ComparisonLambda &cmp) -> leng_t {
  exact_found = false;

  /**
   * @brief This should only be called in Blob Handler indexes,
   * In such indexes, the prefix_len is 0
   */
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  Ensure(header.prefix_len == 0);

  // check hint
  leng_t lower = 0;             // inclusive
  leng_t upper = header.count;  // exclusive
  u8 key_head[4];
  std::memcpy(key_head, key.blob.data(), std::min(key.blob.size(), 4UL));
  SearchHint(std::to_array(key_head), lower, upper);

  // binary search on remaining range
  while (lower < upper) {
    auto mid = ((upper - lower) / 2) + lower;
    if (std::memcmp(key_head, slots[mid].head.data(), 4) < 0) {
      upper = mid;
    } else if (std::memcmp(key_head, slots[mid].head.data(), 4) > 0) {
      lower = mid + 1;
    } else {
      // head is equal, check full key
      int ret = -cmp.func(GetKey(mid), &key, key.blob.size());
      if (ret < 0) {
        upper = mid;
      } else if (ret > 0) {
        lower = mid + 1;
      } else {
        // exact BLOB is found, return
        assert(ret == 0);
        exact_found = true;
        return mid;
      }
    }
  }
  return lower;
}

template class BTreeNodeImpl<BTreeNodeHeader>;
template class BTreeNodeImpl<BTreeNodeHeaderWithLatch>;

}  // namespace leanstore::storage