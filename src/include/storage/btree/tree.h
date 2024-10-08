#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

#include <cstring>
#include <functional>
#include <span>
#include <utility>

namespace leanstore::storage {

class BTree : public KVInterface {
 public:
  static leng_t btree_slot_counter;  // Counter to initialize B-Tree in metadata page

  explicit BTree(buffer::BufferManager *buffer_pool, bool append_bias = false);
  ~BTree() override = default;

  /* BTree config*/
  void ToggleAppendBiasMode(bool append_bias) override;
  void SetComparisonOperator(ComparisonLambda cmp) override;

  /* All BTree operators */
  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const PayloadFunc &read_cb) -> bool override;
  void Insert(std::span<u8> key, std::span<const u8> payload) override;
  auto Remove(std::span<u8> key) -> bool override;
  auto Update(std::span<u8> key, std::span<const u8> payload, const PayloadFunc &func) -> bool override;
  auto UpdateInPlace(std::span<u8> key, const PayloadFunc &func, FixedSizeDelta *delta) -> bool override;
  void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;
  auto LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp,
                  const PayloadFunc &read_cb) -> bool override;

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;

 private:
  /* Iterate all pages utilities */
  auto IterateAllNodes(sync::OptimisticGuard<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                       const std::function<u64(BTreeNode &)> &leaf_fn) -> u64;
  auto IterateUntils(sync::OptimisticGuard<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                     const std::function<bool(BTreeNode &)> &leaf_fn) -> bool;

  /* Find Leaf Node storing the key */
  auto FindLeafOptimistic(std::span<u8> key) -> sync::OptimisticGuard<BTreeNode>;
  auto FindLeafShared(std::span<u8> key) -> sync::SharedGuard<BTreeNode>;

  /* Split/Merge utilities */
  void TrySplit(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&node);
  void EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key);
  void TryMerge(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&left,
                sync::ExclusiveGuard<BTreeNode> &&right, leng_t left_pos);
  void EnsureUnderfullInnersForMerge(BTreeNode *to_merge);

  /* Core properties */
  buffer::BufferManager *buffer_;
  leng_t metadata_slotid_;
  std::atomic<bool> append_bias_;

  /* Comparison properties */
  ComparisonLambda cmp_lambda_{ComparisonOperator::MEMCMP, std::memcmp};
};

}  // namespace leanstore::storage