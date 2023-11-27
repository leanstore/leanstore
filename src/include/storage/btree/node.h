#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "storage/blob/blob_state.h"
#include "storage/page.h"

#include <span>
#include <string>

namespace leanstore::storage {

/*
**    BTreeNode format
**
**      |---------------------|
**      | BTreeNodeHeader     |
**      |---------------------|
**      | PageSlot slots[]    |   |  10 bytes per slot.  Sorted order.
**      |                     |   |  Grows downward
**      |---------------------|   v
**      | unallocated space   |
**      |                     |
**      |---------------------|   ^
**      | Payload area        |   |  Grows upwards
**      | Ptr() + data_offset |   |  Size: space_used
**      |---------------------|
**
**  The payload area of each record contains both key + value (i.e. payload) of the record
**  While the PageSlot only maintain the sorted order & the offset to the record
**
**  The relationship between parent's separators and children are depicted as below
**  Note that both children are leaf nodes.
**  For inner nodes, both left's keys vs X vs right's keys are exclusive, i.e. left's keys < X < right's keys
**
**                    |------------------|
**                    |      parent      |
**                    | ptr |  X   | ptr |
**                    |------------------|
**                   /                    \
**                  /                      \
**         |-------------|          |--------------|
**         | left child  |          | right child  |
**         |   keys < X  |          |  keys >= X   |
**         |-------------|          |--------------|
**
**  Inside a node, LowerFences <= all keys < UpperFence
*/

// -------------------------------------------------------------------------------------
struct FenceKeySlot {
  leng_t offset;
  leng_t len;
};

struct SeparatorInfo {
  u32 len;            // len of new separator
  u32 slot;           // slot at which we split
  bool is_truncated;  // if true, we truncate the separator taking len bytes from slot+1
};

struct PageSlot {
  leng_t offset;
  leng_t key_length;
  leng_t payload_length;

  // HEAD stores the first 4 bytes of the key
  std::array<u8, 4> head;
} __attribute__((packed));

// -------------------------------------------------------------------------------------
class BTreeNodeHeader : public PageHeader {
 public:
  static constexpr u64 SIZE_UNDER_FULL = PAGE_SIZE / 4;  // merge nodes below this size
  static constexpr u64 EMPTY_NEIGHBOR  = ~0ULL;
  static constexpr u32 HINT_COUNT      = 16;

  union {
    pageid_t right_most_child;                 // for inner nodes: pageid_t of right-most child
    pageid_t next_leaf_node = EMPTY_NEIGHBOR;  // for leaf nodes: pageid_t of the right sibling
  };

  // Fences are only used to determine key ranges of a specific B+tree node
  FenceKeySlot lower_fence = {0, 0};  // exclusive
  FenceKeySlot upper_fence = {0, 0};  // inclusive

  bool is_leaf;
  leng_t count       = 0;
  leng_t space_used  = 0;                               // space used to store all key+payload & fences
  leng_t data_offset = static_cast<leng_t>(PAGE_SIZE);  // where we start moving payload to
  leng_t prefix_len  = 0;
  std::array<u8, 4> hints[HINT_COUNT];

  explicit BTreeNodeHeader(bool is_leaf, indexid_t index);
  ~BTreeNodeHeader() = default;

  auto HasRightNeighbor() -> bool;
  auto IsLowerFenceInfinity() -> bool;
  auto IsUpperFenceInfinity() -> bool;
};

// -------------------------------------------------------------------------------------
class alignas(PAGE_SIZE) BTreeNode : public BTreeNodeHeader {
 public:
  static constexpr u32 MAX_RECORD_SIZE = ((PAGE_SIZE - sizeof(BTreeNodeHeader) - (3 * sizeof(PageSlot)))) / 3;

  PageSlot slots[(PAGE_SIZE - sizeof(BTreeNodeHeader)) / sizeof(PageSlot)];

  explicit BTreeNode(bool is_leaf, indexid_t index);
  ~BTreeNode() = default;

  // Misc
  auto Ptr() -> u8 *;
  auto IsInner() -> bool;
  auto GetPrefix() -> u8 *;
  static auto GetHead(u8 *key, leng_t key_len) -> std::array<u8, 4>;
  auto FindChild(std::span<u8> search_key, const ComparisonLambda &cmp) -> pageid_t;
  auto FindChild(std::span<u8> search_key, leng_t &pos, const ComparisonLambda &cmp) -> pageid_t;

  // Debugging purposes
  auto ToString() -> std::string;
  auto KeyToString(leng_t slot_id) -> std::string;

  // Fence utilities
  auto GetLowerFence() -> std::span<u8>;
  auto GetUpperFence() -> std::span<u8>;
  void InsertFence(FenceKeySlot &fk, std::span<u8> key);
  void SetFences(std::span<u8> lower, std::span<u8> upper, const ComparisonLambda &cmp);

  // Space utilities
  void Compactify(const ComparisonLambda &cmp);
  auto FreeSpace() -> leng_t;
  auto FreeSpaceAfterCompaction() -> leng_t;
  auto SpaceRequiredForKV(leng_t key_len, leng_t payload_len) -> leng_t;
  auto HasSpaceForKV(leng_t key_len, leng_t payload_len) -> bool;

  // Get data utilities
  auto GetKey(leng_t slot_id) -> u8 *;  // NOTE: this won't return the prefix
  auto GetPayload(leng_t slot_id) -> std::span<u8>;
  auto GetChild(leng_t slot_id) -> pageid_t;
  void GetSeparatorKey(u8 *out_separator_key, const SeparatorInfo &info);

  // Hint utilities
  void MakeHint();
  void UpdateHint(leng_t slot_id);
  void SearchHint(const std::array<u8, 4> &key_head, leng_t &out_lower_bound, leng_t &out_upper_bound);

  // Search utilities
  auto LowerBound(std::span<u8> search_key, bool &exact_found, const ComparisonLambda &cmp) -> leng_t;
  auto LowerBound(std::span<u8> search_key, const ComparisonLambda &cmp) -> leng_t;

  // Store/Insert/Remove utilities
  void StoreRecordData(leng_t slot_id, std::span<u8> key, std::span<const u8> payload);
  void StoreRecordDataWithoutPrefix(leng_t slot_id, std::span<u8> key, std::span<const u8> payload);
  void InsertKeyValue(std::span<u8> key, std::span<const u8> payload, const ComparisonLambda &cmp);
  auto RemoveSlot(leng_t slot_id) -> bool;
  auto RemoveKey(std::span<u8> key, const ComparisonLambda &cmp) -> bool;

  // Clone utilities
  static void CopyNodeContent(BTreeNode *dst, BTreeNode *src);
  void CopyKeyValue(BTreeNode *dst, leng_t src_slot, leng_t dst_slot);
  void CopyKeyValueRange(BTreeNode *dst, leng_t dst_slot_start, leng_t src_slot_start, leng_t src_count);

  // Split/Merge utilities
  auto FindSeparator(bool append_bias, const ComparisonLambda &cmp) -> SeparatorInfo;
  void SplitNode(BTreeNode *parent, BTreeNode *node_right, pageid_t left_pid, pageid_t right_pid, leng_t separator_slot,
                 std::span<u8> sep_key, const ComparisonLambda &cmp);
  auto MergeNodes(leng_t left_slot_id, BTreeNode *parent, BTreeNode *right, const ComparisonLambda &cmp) -> bool;

  // BLOB custom LookUp operators
  auto FindChildWithBlobKey(const blob::BlobLookupKey &key, leng_t &pos, const ComparisonLambda &cmp) -> pageid_t;
  auto LowerBoundWithBlobKey(const blob::BlobLookupKey &key, bool &exact_found, const ComparisonLambda &cmp) -> leng_t;

 private:
  auto CommonPrefix(leng_t lhs_slot, leng_t rhs_slot) -> leng_t;
};

static_assert(sizeof(BTreeNode) == PAGE_SIZE);
static_assert(BTreeNode::MAX_RECORD_SIZE > 1.2 * KB);
static_assert(blob::BlobState::MAX_MALLOC_SIZE <= BTreeNode::MAX_RECORD_SIZE);

}  // namespace leanstore::storage
