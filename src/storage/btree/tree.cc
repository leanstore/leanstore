#include "storage/btree/tree.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/env.h"
#include "storage/blob/blob_manager.h"

#include <cstring>
#include <memory>
#include <tuple>

using leanstore::sync::DeferLog;
using leanstore::sync::ExclusiveGuard;
using leanstore::sync::OptimisticGuard;
using leanstore::sync::SharedGuard;

namespace leanstore::storage {

BTree::BTree(buffer::BufferManager *buffer_pool, recovery::RecoveryManager *recovery, u32 tree_slot)
    : buffer_(buffer_pool), recovery_(recovery), metadata_slotid_(tree_slot) {
  if (!FLAGS_wal_enable_recovery) {
    ExclusiveGuard<MetadataPage> meta_page(buffer_, METADATA_PAGE_ID);
    ExclusiveGuard<BTreeNode> root_page(buffer_, buffer_->AllocPage());
    new (root_page.Ptr()) storage::BTreeNode(true);
    meta_page->roots[metadata_slotid_] = root_page.PageID();
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      GenerateWALNewRoot(meta_page, metadata_slotid_, root_page.PageID());
      GenerateWALFreshPage(root_page, true);
    }
  }
}

void BTree::SetComparisonOperator(ComparisonLambda cmp_op) { cmp_lambda_ = cmp_op; }

auto BTree::IterateAllNodes(OptimisticGuard<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                            const std::function<u64(BTreeNode &)> &leaf_fn) -> u64 {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  u64 res = inner_fn(*(node.Ptr()));
  for (auto idx = 0; idx < node->header.count; idx++) {
    auto child_pid = node->GetChild(idx);
    InstantRecovery(child_pid);
    OptimisticGuard<BTreeNode> child(buffer_, child_pid);
    res += IterateAllNodes(child, inner_fn, leaf_fn);
  }
  InstantRecovery(node->header.right_most_child);
  OptimisticGuard<BTreeNode> child(buffer_, node->header.right_most_child);
  res += IterateAllNodes(child, inner_fn, leaf_fn);
  return res;
}

auto BTree::IterateUntils(OptimisticGuard<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                          const std::function<bool(BTreeNode &)> &leaf_fn) -> bool {
  if (!node->IsInner()) { return leaf_fn(*(node.Ptr())); }

  auto res = inner_fn(*(node.Ptr()));
  if (!res) {
    for (auto idx = 0; idx < node->header.count; idx++) {
      OptimisticGuard<BTreeNode> child(buffer_, node->GetChild(idx));
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
      if (res) { break; }
    }
    if (!res) {
      OptimisticGuard<BTreeNode> child(buffer_, node->header.right_most_child);
      res |= IterateAllNodes(child, inner_fn, leaf_fn);
    }
  }

  return res;
}

// -------------------------------------------------------------------------------------
auto BTree::FindLeafOptimistic(std::span<u8> key) -> OptimisticGuard<BTreeNode> {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  auto root_pid = meta->GetRoot(metadata_slotid_);
  InstantRecovery(root_pid);
  OptimisticGuard<BTreeNode> node(buffer_, root_pid, meta);

  while (node->IsInner()) {
    auto next_pid = node->FindChild(key, cmp_lambda_);
    InstantRecovery(next_pid);
    node = OptimisticGuard<BTreeNode>(buffer_, next_pid, node);
  }
  return node;
}

auto BTree::FindLeafShared(std::span<u8> key) -> SharedGuard<BTreeNode> {
  while (true) {
    try {
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      auto root_pid = meta->GetRoot(metadata_slotid_);
      InstantRecovery(root_pid);
      OptimisticGuard<BTreeNode> node(buffer_, root_pid, meta);

      while (node->IsInner()) {
        auto next_pid = node->FindChild(key, cmp_lambda_);
        InstantRecovery(next_pid);
        node = OptimisticGuard<BTreeNode>(buffer_, next_pid, node);
      }
      return SharedGuard<BTreeNode>(std::move(node));
    } catch (const sync::RestartException &) {}
  }
}

void BTree::TrySplit(ExclusiveGuard<BTreeNode> &&parent, ExclusiveGuard<BTreeNode> &&node) {
  // create new root if necessary
  if (parent.PageID() == METADATA_PAGE_ID) {
    auto meta_p = reinterpret_cast<MetadataPage *>(parent.Ptr());
    // Root node is full, alloc a new root
    ExclusiveGuard<BTreeNode> new_root(buffer_, buffer_->AllocPage());
    new (new_root.Ptr()) storage::BTreeNode(false);
    new_root->header.right_most_child = node.PageID();
    if (FLAGS_wal_enable) {
      GenerateWALNewRoot(parent, metadata_slotid_, new_root.PageID());
      GenerateWALFreshPage(new_root, false, node.PageID());
    }
    // Update root pid
    meta_p->roots[metadata_slotid_] = new_root.PageID();
    parent                          = std::move(new_root);
  }

  // split & retrieve new separator
  assert(parent->IsInner());
  auto sep_info = node->FindSeparator(cmp_lambda_);
  u8 sep_key[sep_info.len];
  node->GetSeparatorKey(sep_key, sep_info);

  if (parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t))) {
    // alloc a new child page
    ExclusiveGuard<BTreeNode> new_child(buffer_, buffer_->AllocPage());
    new (new_child.Ptr()) storage::BTreeNode(!node->IsInner());
    // now split the node
    node->SplitNode(parent.Ptr(), new_child.Ptr(), node.PageID(), new_child.PageID(), sep_info.slot,
                    {sep_key, sep_info.len}, cmp_lambda_);
    assert(node->IsInner() == new_child->IsInner());
    // -------------------------------------------------------------------------------------
    if (FLAGS_wal_enable) {
      // WAL new node
      GenerateWALNewPage(new_child);
      // WAL separator to parent
      {
        auto &entry      = parent.PrepareWalEntry<WALInsertSep>(sep_info.len);
        entry.left_pid   = node.PageID();
        entry.right_pid  = new_child.PageID();
        entry.sep_length = sep_info.len;
        std::memcpy(entry.sep_key, sep_key, sep_info.len);
        parent.SubmitActiveWalEntry();
      }
      // WAL logical split
      {
        auto &entry = node.PrepareWalEntry<WALLogicalSplit>(0);
        entry.sep   = sep_info;
        if (!node->IsInner()) { entry.next_leaf_node = node->header.next_leaf_node; }
        node.SubmitActiveWalEntry();
      }
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
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner() && (node.Ptr() != to_split)) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      if (node.Ptr() == to_split) {
        if (node->HasSpaceForKV(key.size(), sizeof(pageid_t))) {
          // someone else did split concurrently
          return;
        }

        ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        TrySplit(std::move(parent_locked), std::move(node_locked));
      }

      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

/** Try merging `left` into `right` node */
void BTree::TryMerge(ExclusiveGuard<BTreeNode> &&parent, ExclusiveGuard<BTreeNode> &&left,
                     ExclusiveGuard<BTreeNode> &&right, leng_t left_pos) {
  if (left->MergeNodes(left_pos, parent.Ptr(), right.Ptr(), cmp_lambda_)) {
    // TODO(XXX): Free page left.PageID()
    if (FLAGS_wal_enable) {
      // WAL parent remove slot
      {
        auto &entry   = parent.PrepareWalEntry<WALRemove>(0);
        entry.slot_id = left_pos;
        parent.SubmitActiveWalEntry();
      }
      // WAL new page -- it is simpler to implement, but probably slightly inefficient
      GenerateWALNewPage(right);
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
      // TODO(XXX): Implement tree-level compression
      //  i.e. parent of parent is page 0 (i.e. metadata page) and we can compress the inner nodes
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner() && (node.Ptr() != to_merge)) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(rep_key, node_pos, cmp_lambda_), parent);
      }

      if (parent.PageID() != METADATA_PAGE_ID &&  // Root node can't be merged
          node.Ptr() == to_merge &&               // Found the correct node to be merged
          node_pos < parent->header.count &&      // Current node is not the right most child
          parent->header.count >= 1 &&            // Parent has more than one children
          (node->FreeSpaceAfterCompaction() >= BTreeNodeHeader::SIZE_UNDER_FULL)  // Current node is underfull
      ) {
        // underfull
        auto right_pid =
          (node_pos < parent->header.count - 1) ? parent->GetChild(node_pos + 1) : parent->header.right_most_child;
        OptimisticGuard<BTreeNode> right(buffer_, right_pid, parent);
        if (right->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
          ExclusiveGuard<BTreeNode> node_locked(std::move(node));
          ExclusiveGuard<BTreeNode> right_locked(std::move(right));
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      }
      // complete, get out of the optimistic loop
      return;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> OpResult {
  while (true) {
    try {
      OptimisticGuard<BTreeNode> node = FindLeafOptimistic(key);
      bool found;
      leng_t pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      if (!node.TryLockShared(metadata_slotid_, key)) { return OpResult::ABORT_TX; }
      auto payload = node->GetPayload(pos);
      read_cb(payload);
      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Insert(std::span<u8> key, std::span<const u8> payload) -> OpResult {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      // Found the leaf node to insert new data
      if (node->HasSpaceForKV(key.size(), payload.size())) {
        // Concurrency control
        if (!node.TryLock(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

        /* Alternative WAL cycle - automatically append WAL entry when the scope ends */
        auto defer_log = DeferLog<BTreeNode>();

        /* Leaf node has enough space -> only latch leaf node */
        {
          ExclusiveGuard<BTreeNode> node_locked(std::move(node));
          parent.ValidateOrRestart();
          node_locked->InsertKeyValue(key, payload, cmp_lambda_);

          /* Generate the log entry */
          if (FLAGS_wal_enable) { defer_log.Construct<WALInsert>(node_locked, key, payload); }
        }

        return OpResult::OK;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::Remove(std::span<u8> key) -> OpResult {
  while (true) {
    try {
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      leng_t node_pos = 0;
      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, node_pos, cmp_lambda_), parent);
      }

      // Lookup key
      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      // Concurrency control
      if (!node.TryLock(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

      auto payload      = node->GetPayload(slot_id);
      leng_t entry_size = node->slots[slot_id].key_length + payload.size();
      if ((node->FreeSpaceAfterCompaction() + entry_size >=
           BTreeNodeHeader::SIZE_UNDER_FULL) &&     // new node is under full
          (parent.PageID() != METADATA_PAGE_ID) &&  // current node is not the root node
          (parent->header.count >= 2) &&            // parent has more than one children
          ((node_pos + 1) < parent->header.count)   // current node has a right sibling
      ) {
        // underfull
        ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        ExclusiveGuard<BTreeNode> right_locked(buffer_, parent_locked->GetChild(node_pos + 1));
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
        // --------------------------------------------------------------------------
        // right child is also under full
        if (right_locked->FreeSpaceAfterCompaction() >= (PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL)) {
          TryMerge(std::move(parent_locked), std::move(node_locked), std::move(right_locked), node_pos);
        }
      } else {
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();
        node_locked->RemoveSlot(slot_id);
        // --------------------------------------------------------------------------
        // WAL Remove
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
        // --------------------------------------------------------------------------
      }
      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

/**
 * @brief Atomically update the key-payload pair
 * Return true/false whether the key exists and is updated successfully
 *
 * If `func` is provided, then func(previous payload) is triggered
 */
auto BTree::Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult {
  assert((key.size() + payload.size()) <= BTreeNode::MAX_RECORD_SIZE);

  while (true) {
    try {
      OptimisticGuard<BTreeNode> parent(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(
        buffer_, reinterpret_cast<MetadataPage *>(parent.Ptr())->GetRoot(metadata_slotid_), parent);

      while (node->IsInner()) {
        parent = std::move(node);
        node   = OptimisticGuard<BTreeNode>(buffer_, parent->FindChild(key, cmp_lambda_), parent);
      }

      // Lookup key
      bool found;
      auto slot_id = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      // Concurrency control
      if (!node.TryLock(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

      auto curr_payload = node->GetPayload(slot_id);
      // Found the leaf node to insert new data
      if (payload.size() <= curr_payload.size() ||
          node->HasSpaceForKV(key.size(), payload.size() - curr_payload.size())) {
        // only lock leaf
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));
        parent.ValidateOrRestart();

        // Log previous payload, trigger func utility if provided, and remove the entry
        if (FLAGS_wal_enable) {
          auto &entry   = node_locked.PrepareWalEntry<WALRemove>(0);
          entry.slot_id = slot_id;
          node_locked.SubmitActiveWalEntry();
        }
        if (func) { func(curr_payload); }
        node_locked->RemoveSlot(slot_id);

        // Insert new payload and add log entry
        node_locked->InsertKeyValue(key, payload, cmp_lambda_);
        if (FLAGS_wal_enable) { GenerateWAL<ExclusiveGuard<BTreeNode>, WALInsert>(node_locked, key, payload); }
        // --------------------------------------------------------------------------
        return OpResult::OK;  // success
      }

      // The leaf node doesn't have enough space, we have to split it
      ExclusiveGuard<BTreeNode> parent_locked(std::move(parent));
      ExclusiveGuard<BTreeNode> node_locked(std::move(node));
      TrySplit(std::move(parent_locked), std::move(node_locked));

      // We haven't run the insertion yet, so we run the loop again to insert the record
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> OpResult {
  while (true) {
    try {
      auto node = FindLeafOptimistic(key);
      bool found;
      auto pos = node->LowerBound(key, found, cmp_lambda_);
      if (!found) { return OpResult::NOT_FOUND; }

      // Concurrency control
      if (!node.TryLock(metadata_slotid_, key)) { return OpResult::ABORT_TX; }

      /* Alternative WAL cycle - automatically append WAL entry when the scope ends */
      auto defer_log = DeferLog<BTreeNode>();

      {
        ExclusiveGuard<BTreeNode> node_locked(std::move(node));

        /* Modify the record, and store the after-value */
        auto record = node_locked->GetPayload(pos);
        func(record);
        if (FLAGS_wal_enable && delta != nullptr) { delta->UpdateDeltaPayload(record); }

        /* Generate the delta record */
        if (FLAGS_wal_enable) {
          if (delta != nullptr) {
            defer_log.Construct<WALDeltaImage>(node_locked, key,
                                               std::span<u8>(reinterpret_cast<u8 *>(delta), delta->size));
          } else {
            defer_log.Construct<WALAfterImage>(node_locked, key, record);
          }
        }
      }

      return OpResult::OK;
    } catch (const sync::RestartException &) {}
  }
}

auto BTree::ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) -> OpResult {
  auto node = FindLeafShared(key);
  bool unused;
  auto pos = node->LowerBound(key, unused, cmp_lambda_);
  while (true) {
    if (pos < node->header.count) {
      auto op_ret = AccessRecord(node, pos, fn);
      if (op_ret == OpResult::ABORT_TX) { return op_ret; }
      if (op_ret == OpResult::STOP_SCAN) { return OpResult::OK; }
      pos++;
    } else {
      if (!node->header.HasRightNeighbor()) { return OpResult::OK; }
      pos  = 0;
      node = SharedGuard<BTreeNode>(buffer_, node->header.next_leaf_node);
    }
  }
}

auto BTree::ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) -> OpResult {
  auto node = FindLeafShared(key);
  bool found;
  int pos = static_cast<int>(node->LowerBound(key, found, cmp_lambda_));
  // LowerBound search always return the first position whose key >= the search key
  // hence, if LowerBound doesn't give an exact match, the found key will > search key as we scan desc,
  // any key > search key should be overlooked, i.e. start from pos - 1
  if (!found) { pos--; }
  while (true) {
    while (pos >= 0) {
      if (pos < node->header.count) {
        auto op_ret = AccessRecord(node, pos, fn);
        if (op_ret == OpResult::ABORT_TX) { return op_ret; }
        if (op_ret == OpResult::STOP_SCAN) { return OpResult::OK; }
      }
      pos--;
    }
    if (node->header.IsLowerFenceInfinity()) { return OpResult::OK; }
    node = FindLeafShared(node->GetLowerFence());
    pos  = node->header.count - 1;
  }
}

auto BTree::CountEntries() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  auto root_pid = meta->GetRoot(metadata_slotid_);
  InstantRecovery(root_pid);
  OptimisticGuard<BTreeNode> node(buffer_, root_pid, meta);

  return IterateAllNodes(node, [](BTreeNode &) { return 0; }, [](BTreeNode &node) { return node.header.count; });
}

// -------------------------------------------------------------------------------------

auto BTree::IsNotEmpty() -> bool {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateUntils(
    node, [](BTreeNode &) { return false; }, [](BTreeNode &node) { return (node.header.count > 0); });
}

auto BTree::CountPages() -> u64 {
  OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
  OptimisticGuard<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);

  return IterateAllNodes(node, [](BTreeNode &) { return 1; }, [](BTreeNode &) { return 1; });
}

auto BTree::SizeInMB() -> float { return CountPages() * static_cast<float>(PAGE_SIZE) / MB; }

/**
 * @brief Only used for Blob Handler indexes.
 * Similar to LookUp operator, but for Byte String as key
 */
auto BTree::LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const AccessPayloadFunc &read_cb)
  -> bool {
  Ensure(cmp_lambda_.op == ComparisonOperator::BLOB_HANDLER);
  Ensure(cmp.op == ComparisonOperator::BLOB_LOOKUP);
  leng_t unused;
  auto search_key = blob::BlobLookupKey(blob_key);

  while (true) {
    try {
      OptimisticGuard<MetadataPage> meta(buffer_, METADATA_PAGE_ID);
      OptimisticGuard<BTreeNode> node(buffer_, meta->GetRoot(metadata_slotid_), meta);
      while (node->IsInner()) {
        node = OptimisticGuard<BTreeNode>(buffer_, node->FindChildWithBlobKey(search_key, unused, cmp), node);
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