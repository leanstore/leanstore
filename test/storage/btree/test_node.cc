#include "common/typedefs.h"
#include "common/utils.h"
#include "storage/btree/node.h"
#include "storage/page.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"
#include "share_headers/logger.h"

#include <cassert>
#include <cstring>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

using u8string = std::basic_string<u8>;

namespace leanstore {

static constexpr u32 NO_RECORDS = 100;

class TestBTreeNode : public ::testing::Test {
 protected:
  storage::Page mem_;
  u8 separator_key_[64];
  ComparisonLambda cmp_{ComparisonOperator::MEMCMP, std::memcmp};

  void SetUp() override {
    std::memset(reinterpret_cast<u8 *>(&mem_), 0, sizeof(storage::Page));
    mem_.p_gsn = 1;
  }

  template <typename value_t>
  void PrepareData(storage::BTreeNode *node, std::vector<std::pair<int, value_t>> &data, bool get_permutation = false) {
    for (size_t idx = 0; idx < NO_RECORDS; idx++) {
      auto value = (node->IsInner()) ? idx : idx * 100;
      data.emplace_back(idx + 1, static_cast<value_t>(value));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % NO_RECORDS; idx++) { std::next_permutation(data.begin(), data.end()); }
    }
    for (auto &pair : data) {
      std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
      std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
      node->InsertKeyValue(key, payload, cmp_);
    }
  }

  template <typename value_t>
  auto PrepareSmallTree(storage::BTreeNode *parent, storage::BTreeNode *left, storage::BTreeNode *right,
                        std::vector<std::pair<int, value_t>> &data,
                        std::unordered_map<storage::BTreeNode *, pageid_t> &pid_map) -> storage::SeparatorInfo {
    pid_map[parent] = 1;
    pid_map[left]   = 2;
    pid_map[right]  = 3;

    // initial state of parent
    parent->header.right_most_child = pid_map[left];
    parent->header.count            = 0;  // right_most_child doesn't count as a kv in inner nodes

    // Prepare node data
    PrepareData<value_t>(left, data, true);
    LOG_DEBUG("Original data: '%s'", left->ToString().c_str());

    // Now split node to new_leaf
    auto sep_info = left->FindSeparator(false, cmp_);
    assert(parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t)));
    left->GetSeparatorKey(separator_key_, sep_info);
    left->SplitNode(parent, right, pid_map[left], pid_map[right], sep_info.slot, {separator_key_, sep_info.len}, cmp_);
    return sep_info;
  }
};

TEST_F(TestBTreeNode, Constructor) {
  auto node = new (&mem_) storage::BTreeNode(true);
  EXPECT_EQ(node->Ptr(), reinterpret_cast<u8 *>(&mem_));
  EXPECT_EQ(node->header.count, 0);
  EXPECT_EQ(node->header.space_used, 0);
  EXPECT_EQ(node->header.data_offset, PAGE_SIZE);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(mem_.p_gsn, 1);
  EXPECT_FALSE(node->IsInner());
  EXPECT_TRUE(node->header.IsLowerFenceInfinity());
  EXPECT_TRUE(node->header.upper_fence.len == 0);
}

TEST_F(TestBTreeNode, NormalOperation) {
  auto node = new (&mem_) storage::BTreeNode(true);
  std::vector<std::pair<int, int>> data;
  PrepareData<int>(node, data, true);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.count, NO_RECORDS);
  EXPECT_EQ(node->header.space_used, sizeof(int) * 2 * NO_RECORDS);
  EXPECT_EQ(node->header.data_offset, PAGE_SIZE - sizeof(int) * 2 * NO_RECORDS);

  // Insert & Search
  for (auto &pair : data) {
    bool exact_found = false;
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(int)};

    // Search Hint test
    ASSERT_EQ(node->header.prefix_len, 0);
    auto key_head = storage::BTreeNode::GetHead(key.data(), sizeof(int));
    leng_t lower  = 0;
    leng_t upper  = node->header.count;
    node->SearchHint(key_head, lower, upper);

    // Search record
    auto slot_id = node->LowerBound(key, exact_found, cmp_);
    EXPECT_TRUE(exact_found);
    EXPECT_TRUE((lower <= slot_id) && (slot_id <= upper));
    EXPECT_EQ(LoadUnaligned<int>(node->GetKey(slot_id)), pair.first);
    EXPECT_EQ(LoadUnaligned<int>(node->GetPayload(slot_id).data()), pair.second);
  }

  // Try clone and check hints
  auto clone = std::make_unique<storage::BTreeNode>(true);
  storage::BTreeNode::CopyNodeContent(clone.get(), node);
  for (size_t idx = 0; idx < storage::BTreeNodeHeader::HINT_COUNT; idx++) {
    LOG_DEBUG("Clone's Hint[%ld]: %s - Original Hint[%ld]: %s", idx,
              fmt::format("{}", clone->header.hints[idx]).c_str(), idx,
              fmt::format("{}", node->header.hints[idx]).c_str());
    EXPECT_EQ(std::memcmp(clone->header.hints[idx].data(), node->header.hints[idx].data(), 4), 0);
  }

  // Remove
  for (auto &pair : data) {
    bool exact_found = false;
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(int)};

    auto slot_id = node->LowerBound(key, exact_found, cmp_);
    EXPECT_TRUE(exact_found);
    EXPECT_TRUE(node->RemoveSlot(slot_id));
    EXPECT_FALSE(node->RemoveKey(key, cmp_));
  }
}

TEST_F(TestBTreeNode, NormalOperationWithPrefix) {
  u8string prefix      = {5, 7, 9, 2, 4, 10};
  u8string lower_fence = prefix + u8string{0, 0, 0, 0};
  u8string upper_fence = prefix + u8string{255, 255, 255, 255};
  std::vector<std::pair<u8string, leng_t>> data;

  auto node = new (&mem_) storage::BTreeNode(true);
  node->SetFences(std::span{lower_fence.data(), lower_fence.size()}, std::span{upper_fence.data(), upper_fence.size()},
                  cmp_);
  for (u8 idx = 0; idx < NO_RECORDS; idx++) {
    data.emplace_back(prefix + u8string{idx, idx, idx, idx}, static_cast<leng_t>(idx * 100));
  }
  for (size_t idx = 0; idx < rand() % NO_RECORDS; idx++) { std::next_permutation(data.begin(), data.end()); }

  EXPECT_EQ(node->header.prefix_len, 6);
  auto expected_space_used = lower_fence.size() + upper_fence.size();
  EXPECT_EQ(node->header.space_used, expected_space_used);

  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(pair.first.data()), pair.first.size()};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(leng_t)};
    node->InsertKeyValue(key, payload, cmp_);
    expected_space_used += sizeof(leng_t) + key.size() - node->header.prefix_len;
    EXPECT_EQ(node->header.space_used, expected_space_used);
  }
  EXPECT_EQ(node->header.count, NO_RECORDS);
  EXPECT_EQ(node->header.data_offset, PAGE_SIZE - expected_space_used);

  // Insert & Search
  for (auto &pair : data) {
    bool exact_found = false;
    std::span key{reinterpret_cast<u8 *>(pair.first.data()), pair.first.size()};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(leng_t)};

    // Search Hint test
    ASSERT_GT(node->header.prefix_len, 0);
    auto key_head =
      storage::BTreeNode::GetHead(pair.first.data() + node->header.prefix_len, key.size() - node->header.prefix_len);
    leng_t lower = 0;
    leng_t upper = node->header.count;
    node->SearchHint(key_head, lower, upper);

    // Search record
    auto slot_id = node->LowerBound(key, exact_found, cmp_);
    LOG_DEBUG("Key head %s", fmt::format("{}", key_head).c_str());
    LOG_DEBUG("Lower %u - upper %u - slot_id %d - key %s", lower, upper, slot_id, node->KeyToString(slot_id).c_str());
    EXPECT_TRUE(exact_found);
    ASSERT_TRUE((lower <= slot_id) && (slot_id <= upper));
    u8string queried_key(key.size(), 0);
    std::memcpy(queried_key.data(), node->GetPrefix(), node->header.prefix_len);
    std::memcpy(queried_key.data() + node->header.prefix_len, node->GetKey(slot_id),
                key.size() - node->header.prefix_len);
    EXPECT_EQ(queried_key, pair.first);
    EXPECT_EQ(LoadUnaligned<leng_t>(node->GetPayload(slot_id).data()), pair.second);
  }

  // Try clone and check hints
  LOG_DEBUG("Node: %s", node->ToString().c_str());
  auto clone = std::make_unique<storage::BTreeNode>(true);
  storage::BTreeNode::CopyNodeContent(clone.get(), node);
  for (size_t idx = 0; idx < storage::BTreeNodeHeader::HINT_COUNT; idx++) {
    LOG_DEBUG("Clone's Hint[%ld]: %s - Original Hint[%ld]: %s", idx,
              fmt::format("{}", clone->header.hints[idx]).c_str(), idx,
              fmt::format("{}", node->header.hints[idx]).c_str());
    EXPECT_EQ(std::memcmp(clone->header.hints[idx].data(), node->header.hints[idx].data(), 4), 0);
  }

  // Remove
  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(pair.first.data()), pair.first.size()};
    EXPECT_TRUE(node->RemoveKey(key, cmp_));
    EXPECT_FALSE(node->RemoveKey(key, cmp_));
    LOG_DEBUG("Node: %s", node->ToString().c_str());
  }
}

TEST_F(TestBTreeNode, TestCopyRange) {
  auto node  = new (&mem_) storage::BTreeNode(true);
  auto other = std::make_unique<storage::BTreeNode>(true);

  // Prepare node data
  std::vector<std::pair<int, int>> data;
  PrepareData<int>(node, data, false);

  // Clone ops
  node->CopyKeyValueRange(other.get(), 0, 0, node->header.count);
  EXPECT_EQ(node->header.count, other->header.count);
  LOG_DEBUG("node count %u - other count %u", node->header.count, other->header.count);
  for (size_t idx = 0; idx < node->header.count; idx++) {
    EXPECT_EQ(node->slots[idx].offset, other->slots[idx].offset);
    EXPECT_EQ(node->slots[idx].key_length, other->slots[idx].key_length);
    EXPECT_EQ(node->slots[idx].payload_length, other->slots[idx].payload_length);

    for (size_t jdx = 0; jdx < node->slots[idx].key_length; jdx++) {
      EXPECT_EQ(node->GetKey(idx)[jdx], other->GetKey(idx)[jdx]);
    }
    auto node_payload  = node->GetPayload(idx);
    auto other_payload = other->GetPayload(idx);
    EXPECT_EQ(node_payload.size(), other_payload.size());
    for (size_t jdx = 0; jdx < node_payload.size(); jdx++) { EXPECT_EQ(node_payload[jdx], other_payload[jdx]); }
  }
}

TEST_F(TestBTreeNode, TestSplit) {
  auto node     = new (&mem_) storage::BTreeNode(true);
  auto new_leaf = std::make_unique<storage::BTreeNode>(true);
  auto parent   = std::make_unique<storage::BTreeNode>(false);
  std::vector<std::pair<int, int>> data;
  std::unordered_map<storage::BTreeNode *, pageid_t> pid_map;

  // Prepare initial data
  auto sep_info = PrepareSmallTree<int>(parent.get(), node, new_leaf.get(), data, pid_map);
  EXPECT_EQ(parent->header.right_most_child, pid_map[new_leaf.get()]);
  EXPECT_EQ(parent->GetChild(0), pid_map[node]);

  LOG_DEBUG("Parent: %s", parent->ToString().c_str());
  LOG_DEBUG("Left: %s", node->ToString().c_str());
  LOG_DEBUG("Right: %s", new_leaf->ToString().c_str());

  EXPECT_LE(node->header.count, new_leaf->header.count);
  LOG_DEBUG("Node count %u, new leaf count %u", node->header.count, new_leaf->header.count);
  EXPECT_LE(sep_info.len, node->header.prefix_len + node->slots[node->header.count - 1].key_length);

  u8 last_key[node->header.prefix_len + node->slots[node->header.count - 1].key_length];
  std::memcpy(&last_key, new_leaf->GetPrefix(), new_leaf->header.prefix_len);
  std::memcpy(&last_key + new_leaf->header.prefix_len, new_leaf->GetKey(0), new_leaf->slots[0].key_length);
  for (size_t idx = 0; idx < sep_info.len; idx++) { EXPECT_EQ(separator_key_[idx], last_key[idx]); }
}

TEST_F(TestBTreeNode, TestSplitInnerNodes) {
  auto left   = new (&mem_) storage::BTreeNode(false);
  auto right  = std::make_unique<storage::BTreeNode>(false);
  auto parent = std::make_unique<storage::BTreeNode>(false);

  std::vector<std::pair<int, pageid_t>> data;
  std::unordered_map<storage::BTreeNode *, pageid_t> pid_map;

  // Prepare initial data
  auto sep_info = PrepareSmallTree<pageid_t>(parent.get(), left, right.get(), data, pid_map);
  EXPECT_EQ(parent->header.count, 1);
  EXPECT_EQ(parent->header.right_most_child, pid_map[right.get()]);
  EXPECT_EQ(parent->GetChild(0), pid_map[left]);
  EXPECT_EQ(left->header.right_most_child, NO_RECORDS / 2);

  LOG_DEBUG("Parent: %s", parent->ToString().c_str());
  LOG_DEBUG("Left: %s", left->ToString().c_str());
  LOG_DEBUG("Right: %s", right->ToString().c_str());
  EXPECT_LE(sep_info.len, left->header.prefix_len + left->slots[left->header.count - 1].key_length);

  u8 parent_key[parent->header.prefix_len + parent->slots[parent->header.count - 1].key_length];
  std::memcpy(&parent_key, parent->GetPrefix(), parent->header.prefix_len);
  std::memcpy(&parent_key + parent->header.prefix_len, parent->GetKey(0), parent->slots[0].key_length);
  for (size_t idx = 0; idx < sep_info.len; idx++) { EXPECT_EQ(separator_key_[idx], parent_key[idx]); }
}

TEST_F(TestBTreeNode, TestMergeLeafNodes) {
  auto left   = new (&mem_) storage::BTreeNode(true);
  auto right  = std::make_unique<storage::BTreeNode>(true);
  auto parent = std::make_unique<storage::BTreeNode>(false);
  std::vector<std::pair<int, int>> data;
  std::unordered_map<storage::BTreeNode *, pageid_t> pid_map;

  // Prepare initial data
  PrepareSmallTree<int>(parent.get(), left, right.get(), data, pid_map);
  EXPECT_EQ(parent->header.count, 1);
  EXPECT_EQ(parent->header.right_most_child, pid_map[right.get()]);
  EXPECT_EQ(parent->GetChild(0), pid_map[left]);

  // Now merge left & right into one
  auto success = left->MergeNodes(0, parent.get(), right.get(), cmp_);
  EXPECT_TRUE(success);
  EXPECT_EQ(parent->header.count, 0);
  EXPECT_EQ(parent->header.right_most_child, pid_map[right.get()]);
  EXPECT_EQ(right->header.count, NO_RECORDS);

  // Validate records in Right node
  for (auto &pair : data) {
    bool exact_found = false;
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(int)};

    // Search Hint test
    ASSERT_EQ(left->header.prefix_len, 0);
    auto key_head = storage::BTreeNode::GetHead(key.data(), sizeof(int));
    leng_t lower  = 0;
    leng_t upper  = right->header.count;
    right->SearchHint(key_head, lower, upper);

    // Search record
    auto slot_id = right->LowerBound(key, exact_found, cmp_);
    EXPECT_TRUE(exact_found);
    EXPECT_TRUE((lower <= slot_id) && (slot_id <= upper));
    EXPECT_EQ(LoadUnaligned<int>(right->GetKey(slot_id)), pair.first);
    EXPECT_EQ(LoadUnaligned<int>(right->GetPayload(slot_id).data()), pair.second);
  }
}

TEST_F(TestBTreeNode, TestMergeInnerNodes) {
  auto left   = new (&mem_) storage::BTreeNode(false);
  auto right  = std::make_unique<storage::BTreeNode>(false);
  auto parent = std::make_unique<storage::BTreeNode>(false);
  std::vector<std::pair<int, pageid_t>> data;
  std::unordered_map<storage::BTreeNode *, pageid_t> pid_map;

  // Prepare initial data
  PrepareSmallTree<pageid_t>(parent.get(), left, right.get(), data, pid_map);
  EXPECT_EQ(parent->header.count, 1);
  EXPECT_EQ(parent->header.right_most_child, pid_map[right.get()]);
  EXPECT_EQ(parent->GetChild(0), pid_map[left]);
  EXPECT_EQ(left->header.right_most_child, NO_RECORDS / 2);

  LOG_DEBUG("Before merging");
  LOG_DEBUG("Parent: %s", parent->ToString().c_str());
  LOG_DEBUG("Left: %s", left->ToString().c_str());
  LOG_DEBUG("Right: %s", right->ToString().c_str());

  // Now merge left & right into one
  auto success = left->MergeNodes(0, parent.get(), right.get(), cmp_);
  EXPECT_TRUE(success);
  EXPECT_EQ(parent->header.count, 0);
  EXPECT_EQ(parent->header.right_most_child, pid_map[right.get()]);
  EXPECT_EQ(right->header.count, NO_RECORDS);

  LOG_DEBUG("After merging");
  LOG_DEBUG("Parent: %s", parent->ToString().c_str());
  LOG_DEBUG("Left: %s", left->ToString().c_str());
  LOG_DEBUG("Right: %s", right->ToString().c_str());

  // Validate records in Right node
  for (auto &pair : data) {
    bool exact_found = false;
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(pageid_t)};

    // Search Hint test
    ASSERT_EQ(left->header.prefix_len, 0);
    auto key_head = storage::BTreeNode::GetHead(key.data(), sizeof(int));
    leng_t lower  = 0;
    leng_t upper  = right->header.count;
    right->SearchHint(key_head, lower, upper);

    // Search record
    auto slot_id = right->LowerBound(key, exact_found, cmp_);
    EXPECT_TRUE(exact_found);
    EXPECT_TRUE((lower <= slot_id) && (slot_id <= upper));
    EXPECT_EQ(LoadUnaligned<int>(right->GetKey(slot_id)), pair.first);
    EXPECT_EQ(LoadUnaligned<pageid_t>(right->GetPayload(slot_id).data()), pair.second);
  }
}

}  // namespace leanstore