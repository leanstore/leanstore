#include "common/typedefs.h"
#include "common/utils.h"
#include "storage/btree/tree.h"
#include "storage/page.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"
#include "share_headers/logger.h"

#include <cassert>
#include <cstring>
#include <forward_list>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

using u8string  = std::basic_string<u8>;
using BlobState = leanstore::storage::blob::BlobState;

namespace leanstore::storage {

class TestBTreeNodeWithBlob : public BaseTest, public ::testing::WithParamInterface<bool> {
 protected:
  Page mem_;
  ComparisonLambda cmp_;
  ComparisonLambda lookup_cmp_;
  std::unique_ptr<BTree> delta_idx_;
  std::unique_ptr<blob::BlobManager> blob_manager_;
  u8 tmp_storage_[BlobState::MAX_MALLOC_SIZE];

  void SetUp() override {
    BaseTest::SetupTestFile(true);
    InitRandTransaction();
    blob_manager_ = std::make_unique<blob::BlobManager>(buffer_.get());

    std::memset(reinterpret_cast<u8 *>(&mem_), 0, sizeof(Page));
    lookup_cmp_ = {ComparisonOperator::BLOB_LOOKUP, [&](const void *a, const void *b, size_t) {
                     return blob_manager_->BlobStateCompareWithString(a, b);
                   }};
    cmp_        = {ComparisonOperator::BLOB_HANDLER,
                   [&](const void *a, const void *b, size_t) { return blob_manager_->BlobStateComparison(a, b); }};
  }

  void TearDown() override {
    buffer_->AliasArea()->ReleaseAliasingArea();
    blob_manager_.reset();
    BaseTest::TearDown();
  }
};

TEST_P(TestBTreeNodeWithBlob, NormalOperation) {
  auto &txn                     = transaction::TransactionManager::active_txn;
  FLAGS_blob_normal_buffer_pool = GetParam();

  // Initialization
  auto node     = new (&mem_) BTreeNode(true);
  u64 blob_size = PAGE_SIZE * 3;
  /**
   * @brief Two Blobs:
   * - random_blob[0]: {11,22,33}
   * - random_blob[1]: {11,33,55}
   * - random_blob[2]: {11,44,77} - Not inserted, only used for testing
   */
  std::array<u8, PAGE_SIZE * 3> random_blob[3];
  for (auto idx = 0; idx < 3; idx++) {
    for (size_t i = 0; i < blob_size; i++) { random_blob[idx][i] = 11 + 11 * (idx + 1) * (i / PAGE_SIZE); }
  }

  // Allocate two Blob
  auto btmp        = blob_manager_->AllocateBlob(std::span<u8>{random_blob[0].data(), blob_size}, nullptr);
  auto blob        = BlobState::MoveToTempStorage(tmp_storage_, btmp);
  auto bigger_blob = blob_manager_->AllocateBlob(std::span<u8>{random_blob[1].data(), blob_size}, nullptr);
  EXPECT_EQ(blob->extents.NumberOfExtents(), 2);
  EXPECT_EQ(bigger_blob->extents.NumberOfExtents(), 2);
  EXPECT_EQ(txn.ToEvictedExtents().size(), 4);

  // Insert two Blob Tuples as Indexed keys
  EXPECT_LT(blob_manager_->BlobStateComparison(blob, bigger_blob), 0);
  node->InsertKeyValue({reinterpret_cast<u8 *>(bigger_blob), bigger_blob->MallocSize()}, {}, cmp_);
  node->InsertKeyValue({reinterpret_cast<u8 *>(blob), blob->MallocSize()}, {}, cmp_);

  // Insert again should not success
  node->InsertKeyValue({reinterpret_cast<u8 *>(blob), blob->MallocSize()}, {}, cmp_);

  // Check node properties
  EXPECT_EQ(node->header.count, 2);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.space_used, blob->MallocSize() * 2);

  // Check Hints & Slots
  for (auto &hint : node->header.hints) { EXPECT_EQ(std::memcmp(random_blob[0].data(), hint.data(), 4), 0); }
  for (size_t idx = 0; idx < node->header.count; idx++) {
    EXPECT_EQ(blob->MallocSize(), node->slots[idx].key_length);
    EXPECT_EQ(std::memcmp(random_blob[idx].data(), node->slots[idx].head.data(), 4), 0);
    LOG_DEBUG("Slot[%ld]: offset %d - key_length %d - payload_length %d - head %s", idx, node->slots[idx].offset,
              node->slots[idx].key_length, node->slots[idx].payload_length,
              fmt::format("{}", node->slots[idx].head).c_str());
  }
  LOG_DEBUG("Node: %s", node->ToString().c_str());

  // Blob LookUp
  auto exact_found = false;
  for (auto idx = 0; idx < 2; idx++) {
    exact_found = false;
    auto pos =
      node->LowerBoundWithBlobKey(blob::BlobLookupKey({random_blob[idx].data(), blob_size}), exact_found, lookup_cmp_);
    EXPECT_TRUE(exact_found);
    EXPECT_EQ(pos, idx);
  }
  auto pos =
    node->LowerBoundWithBlobKey(blob::BlobLookupKey({random_blob[2].data(), blob_size}), exact_found, lookup_cmp_);
  EXPECT_FALSE(exact_found);
  EXPECT_EQ(pos, 2);

  // Delete 1 Blob
  EXPECT_TRUE(node->RemoveKey({reinterpret_cast<u8 *>(blob), blob->MallocSize()}, cmp_));
  EXPECT_FALSE(node->RemoveKey({reinterpret_cast<u8 *>(blob), blob->MallocSize()}, cmp_));

  // Check node properties again
  EXPECT_EQ(node->header.count, 1);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.space_used, blob->MallocSize());
  EXPECT_EQ(node->slots[0].key_length, blob->MallocSize());
  EXPECT_EQ(blob_manager_->BlobStateComparison(bigger_blob, node->GetKey(0)), 0);

  // Delete last Blob
  EXPECT_TRUE(node->RemoveKey({reinterpret_cast<u8 *>(bigger_blob), bigger_blob->MallocSize()}, cmp_));
  EXPECT_FALSE(node->RemoveKey({reinterpret_cast<u8 *>(bigger_blob), bigger_blob->MallocSize()}, cmp_));
  EXPECT_EQ(node->header.count, 0);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.space_used, 0);
}

TEST_P(TestBTreeNodeWithBlob, TestSplitAndMerge) {
  auto &txn                     = transaction::TransactionManager::active_txn;
  FLAGS_blob_normal_buffer_pool = GetParam();

  // Initialization
  u64 blob_size   = PAGE_SIZE;
  u64 blob_hdl_sz = blob::BlobState::MallocSize(1);
  auto node       = new (&mem_) BTreeNode(true);
  u64 no_records =
    std::ceil(static_cast<double>(PAGE_SIZE - sizeof(BTreeNodeHeader)) / (blob_hdl_sz + sizeof(PageSlot)));

  // initialize small tree
  std::unordered_map<storage::BTreeNode *, pageid_t> pid_map;
  auto new_leaf                   = std::make_unique<storage::BTreeNode>(true);
  auto parent                     = std::make_unique<storage::BTreeNode>(false);
  pid_map[parent.get()]           = 1;
  pid_map[node]                   = 2;
  pid_map[new_leaf.get()]         = 3;
  parent->header.right_most_child = pid_map[node];
  parent->header.count            = 0;  // right_most_child doesn't count as a kv in inner nodes

  // Prepare Blob data and allocate them
  std::vector<std::array<u8, PAGE_SIZE>> data;
  std::vector<blob::BlobState *> blobs;
  std::forward_list<std::array<u8, BlobState::MAX_MALLOC_SIZE>> btup_storage;
  for (size_t idx = 0; idx < no_records; idx++) {
    // Blob content
    data.emplace_back();
    data.back().fill(idx);
    // Btup initialize
    btup_storage.emplace_front();
    auto btmp = blob_manager_->AllocateBlob(std::span<u8>{data.back().data(), blob_size}, nullptr);
    blobs.emplace_back(BlobState::MoveToTempStorage(btup_storage.front().data(), btmp));
    ASSERT_EQ(blob_manager_->BlobStateComparison(blobs.back(), btmp), 0);
    EXPECT_EQ(txn.ToFlushedLargePages().size(), idx + 1);
    EXPECT_EQ(txn.ToEvictedExtents().size(), idx + 1);
  }

  // Test comparison
  for (size_t idx = 0; idx < no_records - 1; idx++) {
    EXPECT_LT(blob_manager_->BlobStateComparison(blobs[idx], blobs[idx + 1]), 0);
    node->InsertKeyValue({reinterpret_cast<u8 *>(blobs[idx]), blob_hdl_sz}, {}, cmp_);
  }
  EXPECT_DEATH(node->InsertKeyValue({reinterpret_cast<u8 *>(blobs[no_records - 1]), blob_hdl_sz}, {}, cmp_),
               "Assertion");

  // Check node properties
  EXPECT_EQ(node->header.count, no_records - 1);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.space_used, blob_hdl_sz * (no_records - 1));

  // Try Split
  auto sep_info = node->FindSeparator(false, cmp_);
  EXPECT_TRUE(parent->HasSpaceForKV(sep_info.len, sizeof(pageid_t)));
  EXPECT_EQ(sep_info.len, blob_hdl_sz);
  EXPECT_EQ(sep_info.slot, (node->header.count - 1) / 2);
  EXPECT_EQ(sep_info.is_truncated, false);
  u8 separator_key[blob_hdl_sz];
  node->GetSeparatorKey(separator_key, sep_info);
  node->SplitNode(parent.get(), new_leaf.get(), pid_map[node], pid_map[new_leaf.get()], sep_info.slot,
                  {separator_key, sep_info.len}, cmp_);

  // Check nodes' info
  EXPECT_EQ(node->header.count, sep_info.slot + 1);
  EXPECT_EQ(node->header.prefix_len, 0);
  EXPECT_EQ(node->header.space_used,
            blob_hdl_sz * (sep_info.slot + 1) + blob_hdl_sz);  // all keys + right fence
  EXPECT_EQ(node->header.next_leaf_node, pid_map[new_leaf.get()]);
  EXPECT_EQ(parent->header.count, 1);
  EXPECT_EQ(parent->header.prefix_len, 0);
  EXPECT_EQ(parent->header.space_used, blob_hdl_sz + sizeof(pageid_t));
  EXPECT_EQ(parent->header.right_most_child, pid_map[new_leaf.get()]);
  EXPECT_EQ(parent->GetChild(0), pid_map[node]);

  // Insert last record and check right's info
  new_leaf->InsertKeyValue({reinterpret_cast<u8 *>(blobs[no_records - 1]), blob_hdl_sz}, {}, cmp_);
  EXPECT_EQ(new_leaf->header.count, no_records - node->header.count);
  EXPECT_EQ(new_leaf->header.prefix_len, 0);
  EXPECT_EQ(new_leaf->header.space_used,
            blob_hdl_sz * (no_records - node->header.count) + blob_hdl_sz);  // all keys + left fence

  // Try Blob Lookup on parent
  for (size_t idx = 0; idx < no_records; idx++) {
    leng_t pos;
    auto pid = parent->FindChildWithBlobKey(blob::BlobLookupKey({data[idx].data(), PAGE_SIZE}), pos, lookup_cmp_);
    if (idx < node->header.count) {
      EXPECT_EQ(pos, 0);
      EXPECT_EQ(pid, pid_map[node]);
    } else {
      EXPECT_EQ(pos, 1);
      EXPECT_EQ(pid, pid_map[new_leaf.get()]);
    }
  }

  // Remove half of the keys of both nodes before merging
  for (u32 idx = 0; idx < sep_info.slot / 2; idx++) {
    EXPECT_TRUE(node->RemoveKey({reinterpret_cast<u8 *>(blobs[idx]), blob_hdl_sz}, cmp_));
    EXPECT_FALSE(node->RemoveKey({reinterpret_cast<u8 *>(blobs[idx]), blob_hdl_sz}, cmp_));
  }
  for (u32 idx = 0; idx < sep_info.slot / 2; idx++) {
    EXPECT_TRUE(new_leaf->RemoveKey({reinterpret_cast<u8 *>(blobs[no_records - idx - 1]), blob_hdl_sz}, cmp_));
    EXPECT_FALSE(new_leaf->RemoveKey({reinterpret_cast<u8 *>(blobs[no_records - idx - 1]), blob_hdl_sz}, cmp_));
  }
  EXPECT_GE(node->FreeSpaceAfterCompaction(), PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL);
  EXPECT_GE(new_leaf->FreeSpaceAfterCompaction(), PAGE_SIZE - BTreeNodeHeader::SIZE_UNDER_FULL);

  // Merge two nodes
  EXPECT_TRUE(node->MergeNodes(0, parent.get(), new_leaf.get(), cmp_));

  // Check parent's info
  EXPECT_EQ(parent->header.count, 0);
  EXPECT_EQ(parent->header.prefix_len, 0);
  EXPECT_EQ(parent->header.space_used, 0);
  EXPECT_EQ(parent->header.right_most_child, pid_map[new_leaf.get()]);
}

INSTANTIATE_TEST_CASE_P(TestBTreeNodeWithBlob, TestBTreeNodeWithBlob, ::testing::Values(false, true));

}  // namespace leanstore::storage