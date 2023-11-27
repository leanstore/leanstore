#include "storage/btree/node.h"
#include "common/exceptions.h"
#include "common/utils.h"

#include "fmt/ranges.h"
#include "share_headers/logger.h"

#include <algorithm>
#include <cstring>

namespace leanstore::storage {

// -------------------------------------------------------------------------------------
BTreeNodeHeader::BTreeNodeHeader(bool is_leaf, indexid_t index) : is_leaf(is_leaf) { PageHeader::idx_id = index; }

auto BTreeNodeHeader::HasRightNeighbor() -> bool { return next_leaf_node != BTreeNodeHeader::EMPTY_NEIGHBOR; }

auto BTreeNodeHeader::IsLowerFenceInfinity() -> bool { return lower_fence.len == 0; };

auto BTreeNodeHeader::IsUpperFenceInfinity() -> bool { return upper_fence.len == 0; };

// -------------------------------------------------------------------------------------
BTreeNode::BTreeNode(bool is_leaf, indexid_t index) : BTreeNodeHeader(is_leaf, index) { dirty = true; }

auto BTreeNode::Ptr() -> u8 * { return reinterpret_cast<u8 *>(this); };

auto BTreeNode::IsInner() -> bool { return !is_leaf; }

auto BTreeNode::GetPrefix() -> u8 * { return Ptr() + lower_fence.offset; }

auto BTreeNode::GetHead(u8 *key, leng_t key_len) -> std::array<u8, 4> {
  std::array<u8, 4> ret = {0};
  std::memcpy(ret.data(), key, std::min(key_len, static_cast<leng_t>(4)));
  return ret;
}

auto BTreeNode::FindChild(std::span<u8> search_key, leng_t &pos, const ComparisonLambda &cmp) -> pageid_t {
  pos = LowerBound(search_key, cmp);
  return (pos == count) ? right_most_child : GetChild(pos);
}

auto BTreeNode::FindChild(std::span<u8> search_key, const ComparisonLambda &cmp) -> pageid_t {
  leng_t unused;
  return FindChild(search_key, unused, cmp);
}

/**
 * @brief Find length of the common prefix of two slots: lhs_slot & rhs_slot
 */
auto BTreeNode::CommonPrefix(leng_t lhs_slot, leng_t rhs_slot) -> leng_t {
  assert(lhs_slot < count && rhs_slot < count);
  leng_t limit = std::min(slots[lhs_slot].key_length, slots[rhs_slot].key_length);
  u8 *lhs_key  = GetKey(lhs_slot);
  u8 *rhs_key  = GetKey(rhs_slot);
  leng_t idx;
  for (idx = 0; idx < limit; idx++) {
    if (lhs_key[idx] != rhs_key[idx]) { break; }
  }
  return idx;
}

void BTreeNode::GetSeparatorKey(u8 *out_separator_key, const SeparatorInfo &info) {
  std::memcpy(out_separator_key, GetPrefix(), prefix_len);
  std::memcpy(out_separator_key + prefix_len, GetKey(info.slot + static_cast<u8>(info.is_truncated)),
              info.len - prefix_len);
}

// -------------------------------------------------------------------------------------
auto BTreeNode::KeyToString(leng_t slot_id) -> std::string {
  auto key_info = std::span<u8>{GetKey(slot_id), slots[slot_id].key_length};
  return fmt::format("{}", key_info);
}

auto BTreeNode::ToString() -> std::string {
  auto prefix = std::span<u8>{GetPrefix(), prefix_len};
  std::string extra_metadata;
  if (is_leaf) {
    extra_metadata = "NextSibling(" + std::to_string(next_leaf_node) + ")";
  } else {
    extra_metadata = "RightMostChild(" + std::to_string(right_most_child) + ")";
  }
  std::string all_keys;
  for (size_t idx = 0; idx < count; idx++) {
    if (idx > 0) { all_keys += (idx % 5 == 0) ? ",\n\t\t" : ","; }
    all_keys += KeyToString(idx);
  }
  std::string payload;
  for (size_t idx = 0; idx < count; idx++) {
    if (is_leaf) {
      if (idx > 0) { payload += (idx % 5 == 0) ? ",\n\t\t" : ","; }
      payload += fmt::format("{}", GetPayload(idx));
    } else {
      if (idx > 0) { payload += (idx % 10 == 0) ? ",\n\t\t" : ","; }
      payload += std::to_string(GetChild(idx));
    }
  }

  return fmt::format(
    "<\n\tHeader: idx_id({:d}) - p_gsn({:d})\n\tMetadata: IsLeaf({}) - Count({:d}) - Free space({:d}) - {}"
    "\n\tPrefix({}) - LowerFences({}) - UpperFences({})\n\tKeys: [{}]\n\tPayloads: [{}]\n>",
    idx_id, p_gsn, is_leaf, count, FreeSpaceAfterCompaction(), extra_metadata, prefix, GetLowerFence(), GetUpperFence(),
    all_keys, payload);
}

// -------------------------------------------------------------------------------------
auto BTreeNode::GetLowerFence() -> std::span<u8> { return {Ptr() + lower_fence.offset, lower_fence.len}; }

auto BTreeNode::GetUpperFence() -> std::span<u8> { return {Ptr() + upper_fence.offset, upper_fence.len}; }

void BTreeNode::InsertFence(FenceKeySlot &fk, std::span<u8> key) {
  assert(FreeSpace() >= key.size());
  data_offset -= key.size();
  space_used += key.size();
  fk.offset = data_offset;
  fk.len    = key.size();
  std::memcpy(Ptr() + data_offset, key.data(), key.size());
}

void BTreeNode::SetFences(std::span<u8> lower, std::span<u8> upper, const ComparisonLambda &cmp) {
  InsertFence(lower_fence, lower);
  InsertFence(upper_fence, upper);
  if (cmp.op == ComparisonOperator::MEMCMP) {
    // Prefix optimization is only for MEMCMP
    for (prefix_len = 0;
         (prefix_len < std::min(lower.size(), upper.size())) && (lower[prefix_len] == upper[prefix_len]);
         prefix_len++) {}
  }
}

// -------------------------------------------------------------------------------------
void BTreeNode::Compactify(const ComparisonLambda &cmp) {
  [[maybe_unused]] auto space_after_compacted = FreeSpaceAfterCompaction();
  // Clone this -> tmp without trash
  BTreeNode tmp(is_leaf, idx_id);
  tmp.SetFences(GetLowerFence(), GetUpperFence(), cmp);
  CopyKeyValueRange(&tmp, 0, 0, count);
  tmp.right_most_child = right_most_child;
  // Clone tmp back into this, and then generate hint again
  CopyNodeContent(this, &tmp);
  MakeHint();
  assert(FreeSpace() == space_after_compacted);
}

auto BTreeNode::FreeSpace() -> leng_t { return data_offset - (reinterpret_cast<u8 *>(slots + count) - Ptr()); }

auto BTreeNode::FreeSpaceAfterCompaction() -> leng_t {
  return PAGE_SIZE - (reinterpret_cast<u8 *>(slots + count) - Ptr()) - space_used;
}

auto BTreeNode::SpaceRequiredForKV(leng_t key_len, leng_t payload_len) -> leng_t {
  return sizeof(PageSlot) + (key_len - prefix_len) + payload_len;
}

auto BTreeNode::HasSpaceForKV(leng_t key_len, leng_t payload_len) -> bool {
  return SpaceRequiredForKV(key_len, payload_len) <= FreeSpaceAfterCompaction();
}

// -------------------------------------------------------------------------------------
auto BTreeNode::GetKey(leng_t slot_id) -> u8 * { return Ptr() + slots[slot_id].offset; }

auto BTreeNode::GetPayload(leng_t slot_id) -> std::span<u8> {
  return {Ptr() + slots[slot_id].offset + slots[slot_id].key_length, slots[slot_id].payload_length};
}

auto BTreeNode::GetChild(leng_t slot_id) -> pageid_t { return LoadUnaligned<pageid_t>(GetPayload(slot_id).data()); }

// -------------------------------------------------------------------------------------
void BTreeNode::MakeHint() {
  auto dist = count / (BTreeNode::HINT_COUNT + 1);
  for (size_t idx = 0; idx < BTreeNode::HINT_COUNT; idx++) { hints[idx] = slots[dist * (idx + 1)].head; }
}

void BTreeNode::UpdateHint(leng_t slot_id) {
  auto dist  = count / (BTreeNode::HINT_COUNT + 1);
  auto begin = 0;
  if ((count > BTreeNode::HINT_COUNT * 2 + 1) && (((count - 1) / (BTreeNode::HINT_COUNT + 1)) == dist) &&
      ((slot_id / dist) > 1)) {
    begin = (slot_id / dist) - 1;
  }

  for (size_t idx = begin; idx < BTreeNode::HINT_COUNT; idx++) { hints[idx] = slots[dist * (idx + 1)].head; }
}

void BTreeNode::SearchHint(const std::array<u8, 4> &key_head, leng_t &lower_bound, leng_t &upper_bound) {
  if (count > BTreeNode::HINT_COUNT * 2) {
    leng_t dist = upper_bound / (BTreeNode::HINT_COUNT + 1);
    leng_t lower_hint_p;
    leng_t upper_hint_p;
    for (lower_hint_p = 0; lower_hint_p < BTreeNode::HINT_COUNT; lower_hint_p++) {
      if (std::memcmp(hints[lower_hint_p].data(), key_head.data(), 4) >= 0) { break; }
    }

    for (upper_hint_p = lower_hint_p; upper_hint_p < BTreeNode::HINT_COUNT; upper_hint_p++) {
      if (std::memcmp(hints[upper_hint_p].data(), key_head.data(), 4) != 0) { break; }
    }

    lower_bound = lower_hint_p * dist;
    if (upper_hint_p < BTreeNode::HINT_COUNT) { upper_bound = (upper_hint_p + 1) * dist; }
  }
}

// -------------------------------------------------------------------------------------
auto BTreeNode::LowerBound(std::span<u8> search_key, bool &exact_found, const ComparisonLambda &cmp) -> leng_t {
  exact_found = false;

  int ret = 0;
  if (cmp.op == ComparisonOperator::MEMCMP) {
    // evaluate if the search key is within the prefix of the current node (i.e. prefix of all records inside this node)
    ret = cmp.func(search_key.data(), GetPrefix(), std::min(search_key.size(), static_cast<size_t>(prefix_len)));
    if (ret < 0) { return 0; }
    if (ret > 0) { return count; }
  }

  // key is a substring of the prefix
  if (search_key.size() < prefix_len) { return 0; }

  u8 *key         = search_key.data() + prefix_len;
  auto key_length = search_key.size() - prefix_len;

  // check hint
  leng_t lower  = 0;      // inclusive
  leng_t upper  = count;  // exclusive
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

auto BTreeNode::LowerBound(std::span<u8> search_key, const ComparisonLambda &cmp) -> leng_t {
  bool ignore;
  return LowerBound(search_key, ignore, cmp);
}

// -------------------------------------------------------------------------------------
void BTreeNode::StoreRecordData(leng_t slot_id, std::span<u8> full_key, std::span<const u8> payload) {
  u8 *key         = full_key.data() + prefix_len;
  auto key_length = full_key.size() - prefix_len;
  StoreRecordDataWithoutPrefix(slot_id, {key, key_length}, payload);
}

void BTreeNode::StoreRecordDataWithoutPrefix(leng_t slot_id, std::span<u8> key_no_prefix, std::span<const u8> payload) {
  u8 *key             = key_no_prefix.data();
  auto required_space = key_no_prefix.size() + payload.size();
  // update page metadata
  data_offset -= required_space;
  space_used += required_space;
  // update slots info of this key
  slots[slot_id] = (PageSlot){data_offset, static_cast<leng_t>(key_no_prefix.size()),
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
void BTreeNode::InsertKeyValue(std::span<u8> key, std::span<const u8> payload, const ComparisonLambda &cmp) {
  auto space_needed = SpaceRequiredForKV(key.size(), payload.size());
  if (space_needed > FreeSpace()) {
    assert(space_needed <= FreeSpaceAfterCompaction());
    Compactify(cmp);
  }
  bool found;
  auto slot_id = LowerBound(key, found, cmp);
  if (found) { return; }
  std::move_backward(&slots[slot_id], &slots[count], &slots[count + 1]);
  StoreRecordData(slot_id, key, payload);
  count++;
  UpdateHint(slot_id);
}

auto BTreeNode::RemoveSlot(leng_t slot_id) -> bool {
  space_used -= slots[slot_id].key_length;
  space_used -= slots[slot_id].payload_length;
  std::move(&slots[slot_id + 1], &slots[count], &slots[slot_id]);
  count--;
  MakeHint();
  return true;
}

auto BTreeNode::RemoveKey(std::span<u8> key, const ComparisonLambda &cmp) -> bool {
  bool found;
  auto slot_id = LowerBound(key, found, cmp);
  return (found) ? RemoveSlot(slot_id) : false;
}

// -------------------------------------------------------------------------------------
void BTreeNode::CopyKeyValue(BTreeNode *dst, leng_t src_slot, leng_t dst_slot) {
  size_t full_key_length = slots[src_slot].key_length + prefix_len;
  u8 full_key[full_key_length];
  std::memcpy(full_key, GetPrefix(), prefix_len);
  std::memcpy(full_key + prefix_len, GetKey(src_slot), slots[src_slot].key_length);
  dst->StoreRecordData(dst_slot, {full_key, full_key_length}, GetPayload(src_slot));
  dst->count++;
}

void BTreeNode::CopyNodeContent(BTreeNode *dst, BTreeNode *src) {
  std::memcpy(reinterpret_cast<u8 *>(dst), reinterpret_cast<u8 *>(src), sizeof(BTreeNode));
}

void BTreeNode::CopyKeyValueRange(BTreeNode *dst, leng_t dst_slot, leng_t src_slot, leng_t src_count) {
  // prefix grows
  assert(std::memcmp(GetPrefix(), dst->GetPrefix(), std::min(prefix_len, dst->prefix_len)) == 0);
  if (prefix_len <= dst->prefix_len) {
    auto diff = dst->prefix_len - prefix_len;
    for (auto idx = 0; idx < src_count; idx++) {
      auto new_key_length = slots[src_slot + idx].key_length - diff;
      dst->StoreRecordDataWithoutPrefix(dst_slot + idx,
                                        {GetKey(src_slot + idx) + diff, static_cast<size_t>(new_key_length)},
                                        GetPayload(src_slot + idx));
    }
    dst->count += src_count;
  } else {
    for (auto idx = 0; idx < src_count; idx++) { CopyKeyValue(dst, src_slot + idx, dst_slot + idx); }
  }
  assert((dst->Ptr() + dst->data_offset) >= reinterpret_cast<u8 *>(dst->slots + dst->count));
}

// -------------------------------------------------------------------------------------
auto BTreeNode::FindSeparator(bool append_bias, const ComparisonLambda &cmp) -> SeparatorInfo {
  assert(count > 1);
  if (IsInner()) {
    // Always split inner nodes in the middle
    leng_t slot_id = count / 2;
    return SeparatorInfo{static_cast<leng_t>(prefix_len + slots[slot_id].key_length), slot_id, false};
  }

  // Now find good (i.e. shortest) separator for leaf node
  assert(is_leaf);
  leng_t best_slot;
  leng_t best_prefix_len;

  if (append_bias) {
    best_slot = count - 2;
  } else if (count > 16 && cmp.op == ComparisonOperator::MEMCMP) {
    // Prefix optimization is only for MEMCMP
    leng_t lower = (count / 2) - (count / 16);
    leng_t upper = (count / 2);

    // Find the shortest separator, thus after split,
    //  the prefix_len of two split nodes are as long as possible
    best_prefix_len = CommonPrefix(lower, 0);
    best_slot       = lower;

    if (best_prefix_len != CommonPrefix(upper - 1, 0)) {
      for (best_slot = lower + 1; (best_slot < upper) && (CommonPrefix(best_slot, 0) == best_prefix_len); best_slot++) {
      }
    }
  } else {
    best_slot = (count - 1) / 2;
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
    if ((best_slot + 1 < count) && (slots[best_slot].key_length > common) &&
        (slots[best_slot + 1].key_length > (common + 1))) {
      return SeparatorInfo{static_cast<leng_t>(prefix_len + common + 1), best_slot, true};
    }
  }

  return SeparatorInfo{static_cast<leng_t>(prefix_len + slots[best_slot].key_length), best_slot, false};
}

void BTreeNode::SplitNode(BTreeNode *parent, BTreeNode *node_right, pageid_t left_pid, pageid_t right_pid,
                          leng_t separator_slot, std::span<u8> sep_key, const ComparisonLambda &cmp) {
  assert(separator_slot > 0);
  assert(separator_slot < (PAGE_SIZE / sizeof(pageid_t)));

  BTreeNode tmp(is_leaf, idx_id);
  BTreeNode *node_left = &tmp;

  // update_fence of two child nodes
  node_left->SetFences(GetLowerFence(), sep_key, cmp);
  node_right->SetFences(sep_key, GetUpperFence(), cmp);

  // update the separator in parent node
  leng_t old_parent_slot = parent->LowerBound(sep_key, cmp);
  if (old_parent_slot == parent->count) {
    assert(parent->right_most_child == left_pid);
    parent->right_most_child = right_pid;
  } else {
    assert(parent->GetChild(old_parent_slot) == left_pid);
    std::memcpy(parent->GetPayload(old_parent_slot).data(), &right_pid, sizeof(pageid_t));
  }
  parent->InsertKeyValue(sep_key, {reinterpret_cast<const u8 *>(&left_pid), sizeof(pageid_t)}, cmp);

  // update content of two child nodes
  if (is_leaf) {
    CopyKeyValueRange(node_left, 0, 0, separator_slot + 1);
    CopyKeyValueRange(node_right, 0, node_left->count, count - node_left->count);
    node_left->next_leaf_node  = right_pid;
    node_right->next_leaf_node = this->next_leaf_node;
  } else {
    // in inner node split, moves the separator key move to parent node
    // i.e., count == 1 + nodeLeft->count + nodeRight->count
    CopyKeyValueRange(node_left, 0, 0, separator_slot);
    CopyKeyValueRange(node_right, 0, node_left->count + 1, count - node_left->count - 1);
    node_left->right_most_child  = GetChild(node_left->count);
    node_right->right_most_child = this->right_most_child;
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
auto BTreeNode::MergeNodes(leng_t left_slot_id, BTreeNode *parent, BTreeNode *right, const ComparisonLambda &cmp)
  -> bool {
  BTreeNode tmp(is_leaf, idx_id);
  tmp.SetFences(GetLowerFence(), right->GetUpperFence(), cmp);

  if (is_leaf) {
    assert((right->is_leaf) && (parent->IsInner()));
    // calculate the upper bound on space used of the new node
    auto space_upper_bound = sizeof(BTreeNodeHeader) +                // size of BTreeNodeHeader (i.e. metadata)
                             space_used + right->space_used +         // size of all keys + their payload
                             (prefix_len - tmp.prefix_len) * count +  //  grow from prefix compression for left node
                             (right->prefix_len - tmp.prefix_len) * right->count +  // same as above for right node
                             sizeof(PageSlot) * (count + right->count);             // size of all slots
    if (space_upper_bound > PAGE_SIZE) { return false; }
    // start moving entries to new node
    CopyKeyValueRange(&tmp, 0, 0, count);
    right->CopyKeyValueRange(&tmp, count, 0, right->count);
    // update metadata for new node
    tmp.next_leaf_node = right->next_leaf_node;
  } else {
    assert((right->IsInner()) && (parent->IsInner()));
    // calculate the upper bound on space used of the new node
    auto extra_key_len     = parent->prefix_len + parent->slots[left_slot_id].key_length;
    auto space_upper_bound = sizeof(BTreeNodeHeader) +                // size of BTreeNodeHeader (i.e. metadata)
                             space_used + right->space_used +         // size of all keys + their payload
                             (prefix_len - tmp.prefix_len) * count +  //  grow from prefix compression for left node
                             (right->prefix_len - tmp.prefix_len) * right->count +  // same as above for right node
                             sizeof(PageSlot) * (count + right->count) +            // size of all slots
                             tmp.SpaceRequiredForKV(extra_key_len,
                                                    sizeof(pageid_t));  // separator retrieved from parent
    if (space_upper_bound > PAGE_SIZE) { return false; }
    // start moving entries to new node
    CopyKeyValueRange(&tmp, 0, 0, count);
    // extra key are retrieved from
    // {parent->full_key[left_slot_id], left->right_most_child}
    std::memcpy(parent->GetPayload(left_slot_id).data(), &right_most_child, sizeof(pageid_t));
    parent->CopyKeyValue(&tmp, left_slot_id, count);
    // now copy the rest from original right child
    right->CopyKeyValueRange(&tmp, tmp.count, 0, right->count);
    // update right_most_child of new node
    tmp.right_most_child = right->right_most_child;
  }
  // update metadata of new node
  tmp.MakeHint();
  CopyNodeContent(right, &tmp);
  // update parent's entries
  parent->RemoveSlot(left_slot_id);
  return true;
}

// -------------------------------------------------------------------------------------
auto BTreeNode::FindChildWithBlobKey(const blob::BlobLookupKey &key, leng_t &pos, const ComparisonLambda &cmp)
  -> pageid_t {
  bool unused;
  pos = LowerBoundWithBlobKey(key, unused, cmp);
  return (pos == count) ? right_most_child : GetChild(pos);
}

auto BTreeNode::LowerBoundWithBlobKey(const blob::BlobLookupKey &key, bool &exact_found, const ComparisonLambda &cmp)
  -> leng_t {
  exact_found = false;

  /**
   * @brief This should only be called in Blob Handler indexes,
   * In such indexes, the prefix_len is 0
   */
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  Ensure(prefix_len == 0);

  // check hint
  leng_t lower = 0;      // inclusive
  leng_t upper = count;  // exclusive
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

}  // namespace leanstore::storage