#include "storage/btree/tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"

#include <cstring>
#include <memory>
#include <tuple>

#define ACCESS_RECORD_MACRO(node, pos)                                                              \
  ({                                                                                                \
    auto node_ptr = *((node).Ptr());                                                                \
    std::memcpy((key), node_ptr.GetPrefix(), node_ptr.prefix_len);                                  \
    std::memcpy((key) + node_ptr.prefix_len, node_ptr.GetKey(pos), node_ptr.slots[pos].key_length); \
    fn({(key), (key_len)}, node_ptr.GetPayload(pos));                                               \
  })

namespace leanstore::storage {

leng_t BTree::btree_slot_counter = 0;

BTree::BTree(buffer::BufferManager *buffer_pool, bool append_bias) : buffer_(buffer_pool), append_bias_(append_bias) {
  GuardX<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
  GuardX<BTreeNode> root_page(buffer_, buffer_->AllocPage());
  new (root_page.Ptr()) storage::BTreeNode(true, metadata_slotid_);
  metadata_slotid_                   = btree_slot_counter++;
  meta_page->roots[metadata_slotid_] = root_page.PageID();
  // -------------------------------------------------------------------------------------
  meta_page.AdvanceGSN();
  root_page.AdvanceGSN();
}

void BTree::ToggleAppendBiasMode(bool append_bias) { append_bias_ = append_bias; }

void BTree::SetComparisonOperator(ComparisonLambda cmp_op) { cmp_lambda_ = cmp_op; }

auto BTree::IterateAllNodes(GuardO<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                            const std::function<u64(BTreeNode &)> &leaf_fn) -> u64 {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->count; idx++) {
    GuardO<BTreeNode> child(buffer_, node->GetChild(idx));
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  GuardO<BTreeNode> child(buffer_, node->right_most_child);
  res += IterateAllNodes(child, inner_fn, leaf_fn);
  return res;
}

auto BTree::IterateUntils(GuardO<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                          const std::function<bool(BTreeNode &)> &leaf_fn) -> bool {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  auto res = inner_fn(*(node.Ptr()));
  if (!res) {
    for (auto idx = 0; idx < node->count; idx++) {
      GuardO<BTreeNode> child(buffer_, node->GetChild(idx));
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
      if (res) { break; }
    }
    if (!res) {
      GuardO<BTreeNode> child(buffer_, node->right_most_child);
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
    }
  }

  return res;
}

// -------------------------------------------------------------------------------------
auto BTree::FindLeafOptimistic(std::span<u8> key) -> GuardO<BTreeNode> {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  while (node->IsInner()) { node = GuardO<BTreeNode>(buffer_, node->FindChild(key, cmp_lambda_), node); }
  return node;
}

auto BTree::FindLeafShared(std::span<u8> key) -> GuardS<BTreeNode> {
  while (true) {
    try {
      GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

      while (node->IsInner()) { node = GuardO<BTreeNode>(buffer_, node->FindChild(key, cmp_lambda_), node); }

      return GuardS<BTreeNode>(std::move(node));
    } catch (const sync::RestartException &) {}
  }
}

void BTree::TrySplit(GuardX<BTreeNode> &&parent, GuardX<BTreeNode> &&node) {
  // create new root if necessary
  if (parent.PageID() == METADATA_PAGE_ID) {
    auto meta_p = reinterpret_cast<MetadataPage *>(parent.Ptr());
    // Root node is full, alloc a new root
    GuardX<BTreeNode> new_root(buffer_, buffer_->AllocPage());
    new (new_root.Ptr()) storage::BTreeNode(false, metadata_slotid_);
    new_root->right_most_child = node.PageID();
    if (FLAGS_wal_enable) {
      new_root.ReserveWalEntry<WALNewRoot>(0);
      new_root.SubmitActiveWalEntry();
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent                          = std::move(new_root);
    parent.AdvanceGSN();
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(append_bias_.load(), cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    GuardX<BTreeNode> new_child(buffer_, buffer_->AllocPage());
    new (new_child.Ptr()) storage::BTreeNode(!node->IsInner(), metadata_slotid_);
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(), new_child.PageID(), sep_info.slot,
                    {sep_key, sep_info.len}, cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL new node
      new_child.ReserveWalEntry<WALInitPage>(0);
      new_child.SubmitActiveWalEntry();
      // WAL logical split
      //  all parent, node, and new_child share the same local log buffer,
      //  hence we don't need to push this wal entry using parent's and new_child's
      auto &entry = node.ReserveWalEntry<WALLogicalSplit>(0);
      std::tie(entry.parent_pid, entry.left_pid, entry.right_pid, entry.sep_slot) =
        std::make_tuple(parent.PageID(), node.PageID(), new_child.PageID(), sep_info.slot);
      node.SubmitActiveWalEntry();
    }
    return;
  }

  // must split parent to make space for separator, restart from root to do this
  node.Unlock();
  EnsureSpaceForSplit(parent.UnlockAndGetPtr(), {sep_key, sep_info.len});
}

void BTree::EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key) {
  assert(to_split->IsInner());
  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner() && (node.Ptr() != to_split)) {
        parent = std::move(node);
        node   = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      if (node.Ptr() == to_split) {
        if (node->HasSpaceForKV(key.size(), sizeof(pageid_t))) {
          // someone else did split concurrently
          return;
        }

        GuardX<BTreeNode> parent_locked(std::move(parent));
        GuardX<BTreeNode> node_locked(std::move(node));
        TrySplit(std::move(parent_locked), std::move(node_locked));
      }

      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

void BTree::TryMerge(GuardX<BTreeNode> &&parent, GuardX<BTreeNode> &&left, GuardX<BTreeNode> &&right, leng_t left_pos) {
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    buffer_->GetFreePageManager()->PrepareFreeTier(left.PageID(), 0);
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL merge left into right
      auto &entry = left.ReserveWalEntry<WALMergeNodes>(0);
      std::tie(entry.parent_pid, entry.left_pid, entry.right_pid, entry.left_pos) =
        std::make_tuple(parent.PageID(), left.PageID(), right.PageID(), left_pos);
      left.SubmitActiveWalEntry();
    }
    if (parent->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
      left.Unlock();
      right.Unlock();
      // Parent node is underfull, try merge this inner node
      EnsureUnderfullInnersForMerge(parent.UnlockAndGetPtr());
    }
  }
}

void BTree::EnsureUnderfullInnersForMerge(BTreeNode *to_merge) {
  assert(to_merge->IsInner());
  auto rep_key = to_merge->GetUpperFence();
  while (true) {
    try {
      // TODO(Duy): Implement tree-level compression
      //  i.e. parent of parent is page 0 (i.e. metadata page)
      //  and we can compress the inner nodes
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                             parent);

      leng_t node_pos = 0;
      while (node->IsInner() && (node.Ptr() != to_merge)) {
        parent = std::move(node);
        node   = GuardO<BTreeNode>(buffer_, parent->FindChild(rep_key, node_pos, cmp_lambda_), parent);
      }

      if (parent.PageID() != METADATA_PAGE_ID &&  // Root node can't be merged
          node.Ptr() == to_merge &&               // Found the correct node to be merged
          node_pos < parent->count &&             // Current node is not the right most child
          parent->count >= 1 &&                   // Parent has more than one children
          (node->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL)  // Current node is underfull
      ) {
        // underfull
        auto right_pid = (node_pos < parent->count - 1) ? parent->GetChild(node_pos + 1) : parent->right_most_child;
        GuardO<BTreeNode> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
          GuardX<BTreeNode> parent_locked(std::move(parent));
          GuardX<BTreeNode> node_locked(std::move(node));
          GuardX<BTreeNode> right_locked(std::move(right));
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      }
      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::LookUp(std::span<u8> key, const PayloadFunc &read_cb) -> bool {
  while (true) {
    try {
      GuardO<BTreeNode> node = FindLeafOptimistic(key);
      bool found;
      leng_t pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {}
  }
}

void BTree::Insert(std::span<u8> key, std::span<const u8> payload) {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      // Found the leaf node to insert new data
      if (node->HasSpaceForKV(key.size(), payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        // --------------------------------------------------------------------------
        // WAL Insert
        if (FLAGS_wal_enable) { WalNewTuple(node_locked, WALInsert, key, payload); }
        // --------------------------------------------------------------------------
        return;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      GuardX<BTreeNode> parent_locked(std::move(parent));
      GuardX<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Remove(std::span<u8> key) -> bool {
  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                             parent);

      leng_t node_pos = 0;
      while (node->IsInner()) {
        parent = std::move(node);
        node   = GuardO<BTreeNode>(buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      auto payload      = node->GetPayload(slot_id);
      leng_t entry_size = node->slots[slot_id].key_length + payload.size();
      if ((node->FreeSpaceAfterCompaction() + entry_size >=
           BTreeNodeHeader::SIZE_UNDER_FULL) &&     // new node is under full
          (parent.PageID() != METADATA_PAGE_ID) &&  // current node is not the root node
          (parent->count >= 2) &&                   // parent has more than one children
          ((node_pos + 1) < parent->count)          // current node has a right sibling
      ) {
        // underfull
        GuardX<BTreeNode> parent_locked(std::move(parent));
        GuardX<BTreeNode> node_locked(std::move(node));
        GuardX<BTreeNode> right_locked(buffer_, parent_locked->GetChild(node_pos + 1));
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) { WalNewTuple(node_locked, WALRemove, key, payload); }
        // --------------------------------------------------------------------------
        // right child is also under full
        if (right_locked->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL) {
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      } else {
        GuardX<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) { WalNewTuple(node_locked, WALRemove, key, payload); }
        // --------------------------------------------------------------------------
      }
      return true;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Update(std::span<u8> key, std::span<const u8> payload) -> bool {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      GuardO<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_),
                             parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = GuardO<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }
      auto curr_payload = node->GetPayload(slot_id);

      // Found the leaf node to insert new data
      if (payload.size() <= curr_payload.size() ||
          node->HasSpaceForKV(key.size(), payload.size() - curr_payload.size())) {
        // only lock leaf
        GuardX<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        // --------------------------------------------------------------------------
        // WAL Insert
        if (FLAGS_wal_enable) {
          WalNewTuple(node_locked, WALRemove, key, curr_payload);
          WalNewTuple(node_locked, WALInsert, key, payload);
        }
        // --------------------------------------------------------------------------
        return true;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      GuardX<BTreeNode> parent_locked(std::move(parent));
      GuardX<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::UpdateInPlace(std::span<u8> key, const PayloadFunc &func) -> bool {
  while (true) {
    try {
      auto node = FindLeafOptimistic(key);
      bool found;
      auto pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return false; }

      {
        GuardX<BTreeNode> node_locked(std::move(node));
        func(node_locked->GetPayload(pos));
        // --------------------------------------------------------------------------
        if (FLAGS_wal_enable) {
          // WAL Update
          auto payload = node_locked->GetPayload(pos);
          WalNewTuple(node_locked, WALAfterImage, key, payload);
        }
        return true;
      }
    } catch (const sync::RestartException &) {}
  }
}

void BTree::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) {
  auto node = FindLeafShared(key);
  bool unused;
  auto pos = node->LowerBound(key, unused, cmp_lambda_);
  while (true) {
    if (pos < node->count) {
      size_t key_len = node->prefix_len + node->slots[pos].key_length;
      u8 key[key_len];
      if (!ACCESS_RECORD_MACRO(node, pos)) { return; }
      pos++;
    } else {
      if (!node->HasRightNeighbor()) { return; }
      pos  = 0;
      node = GuardS<BTreeNode>(buffer_, node->next_leaf_node);
    }
  }
}

void BTree::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) {
  auto node = FindLeafShared(key);
  bool found;
  int pos = static_cast<int>(node->LowerBound(key, found, cmp_lambda_));
  // LowerBound search always return the first position whose key >= the search key
  // hence, if LowerBound doesn't give an exact match,
  //    then the found key will > search key as we scan desc,
  // any key > search key should be overlooked, i.e. start from pos - 1
  if (!found) { pos--; }
  while (true) {
    while (pos >= 0) {
      if (pos < node->count) {
        size_t key_len = node->prefix_len + node->slots[pos].key_length;
        u8 key[key_len];
        if (!ACCESS_RECORD_MACRO(node, pos)) { return; }
      }
      pos--;
    }
    if (node->IsLowerFenceInfinity()) { return; }
    node = FindLeafShared(node->GetLowerFence());
    pos  = node->count - 1;
  }
}

auto BTree::CountEntries() -> u64 {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(
    node, [](BTreeNode &) { return 0; }, [](BTreeNode &node) { return node.count; });
}

// -------------------------------------------------------------------------------------

auto BTree::IsNotEmpty() -> bool {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateUntils(
    node, [](BTreeNode &) { return false; }, [](BTreeNode &node) { return (node.count > 0); });
}

auto BTree::CountPages() -> u64 {
  GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(
    node, [](BTreeNode &) { return 1; }, [](BTreeNode &) { return 1; });
}

auto BTree::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

/**
 * @brief Only used for Blob Handler indexes.
 * Similar to LookUp operator, but for Byte String as key
 */
auto BTree::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const PayloadFunc &read_cb) -> bool {
  Ensure(cmp_lambda_.op == ComparisonOperator::BLOB_HANDLER);
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  leng_t unused;
  auto search_key = blob::BlobLookupKey(blob_key);

  while (true) {
    try {
      GuardO<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      GuardO<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);
      while (node->IsInner()) {
        node = GuardO<BTreeNode>(buffer_, node->FindChildWithBlobKey(search_key, unused, cmp), node);
      }

      bool found;
      leng_t pos = node->LowerBoundWithBlobKey(search_key, found, cmp);
      if (!found) { return false; }

      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return true;
    } catch (const sync::RestartException &) {}
  }
}

}  // namespace leanstore::storage