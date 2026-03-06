#include "common/typedefs.h"
#include "test/base_test.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace leanstore {

using ::testing::ElementsAreArray;
using transaction::IsolationLevel;

class TestSVCCTransactionManager : public BaseTest {
 protected:
  std::unique_ptr<storage::BTree> tree_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), 0);
    catalog.push_back(tree_.get());
    txn_man_->CommitTransaction();
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }
};

TEST_F(TestSVCCTransactionManager, CommitPersistsMultipleInserts) {
  std::vector<std::vector<u8>> keys   = {{1}, {2}, {3}};
  std::vector<std::vector<u8>> values = {{10}, {20}, {30}};

  // Load txn
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { ASSERT_EQ(tree_->Insert(keys[i], values[i]), OpResult::OK); }
  txn_man_->CommitTransaction();

  // Assert txn
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    auto ret =
      tree_->LookUp(keys[i], [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(values[i])); });
    ASSERT_EQ(ret, OpResult::OK);
  }
  ASSERT_EQ(tree_->CountEntries(), 3);
  txn_man_->CommitTransaction();
}

TEST_F(TestSVCCTransactionManager, AbortUndoMultipleInserts) {
  std::vector<std::vector<u8>> keys   = {{5}, {6}, {7}};
  std::vector<std::vector<u8>> values = {{50}, {60}, {70}};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    auto ret = tree_->Insert(keys[i], values[i]);
    ASSERT_EQ(ret, OpResult::OK);
  }
  txn_man_->AbortTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (auto i = 0U; i < keys.size(); i++) {
    ASSERT_EQ(tree_->LookUp(keys[i], [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  }
  ASSERT_EQ(tree_->CountEntries(), 0);
  txn_man_->CommitTransaction();
}

TEST_F(TestSVCCTransactionManager, CommitPersistsMultipleUpdates) {
  std::vector<std::vector<u8>> keys    = {{1}, {2}, {3}};
  std::vector<std::vector<u8>> initial = {{10}, {20}, {30}};
  std::vector<std::vector<u8>> updated = {{11}, {21}, {31}};

  // Preload
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Insert(keys[i], initial[i]); }
  txn_man_->CommitTransaction();

  // Commit
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Update(keys[i], updated[i], {}); }
  txn_man_->CommitTransaction();

  // Assert
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(
      tree_->LookUp(keys[i], [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(updated[i])); }),
      OpResult::OK);
  }
  txn_man_->CommitTransaction();
}

TEST_F(TestSVCCTransactionManager, AbortMultipleUpdates) {
  std::vector<std::vector<u8>> keys    = {{7}, {8}, {9}};
  std::vector<std::vector<u8>> initial = {{1}, {2}, {3}};
  std::vector<std::vector<u8>> updated = {{10}, {20}, {30}};

  // Preload
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Insert(keys[i], initial[i]); }
  txn_man_->CommitTransaction();

  // Abort
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Update(keys[i], updated[i], {}); }
  txn_man_->AbortTransaction();

  // Verify full rollback
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(
      tree_->LookUp(keys[i], [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(initial[i])); }),
      OpResult::OK);
  }
  txn_man_->CommitTransaction();
}

TEST_F(TestSVCCTransactionManager, AbortMixedOperationsMultipleTuples) {
  std::vector<u8> k1 = {1};
  std::vector<u8> k2 = {2};
  std::vector<u8> k3 = {3};

  std::vector<u8> v1 = {10};
  std::vector<u8> v2 = {20};
  std::vector<u8> v3 = {30};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(k1, v1);
  tree_->Insert(k2, v2);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Update(k1, std::vector<u8>{99}, {});
  tree_->Insert(k3, v3);
  tree_->Remove(k2);
  txn_man_->AbortTransaction();

  // Verify full rollback
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(k1, [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(v1)); }),
            OpResult::OK);  // Update undone
  ASSERT_EQ(tree_->LookUp(k2, [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(v2)); }),
            OpResult::OK);                                                         // delete undone
  ASSERT_EQ(tree_->LookUp(k3, [&](std::span<const u8>) {}), OpResult::NOT_FOUND);  // insert undone
  txn_man_->CommitTransaction();
}

TEST_F(TestSVCCTransactionManager, CommitMixedOperationsMultipleTuples) {
  std::vector<u8> k1 = {1};
  std::vector<u8> k2 = {2};
  std::vector<u8> k3 = {3};

  std::vector<u8> v1 = {10};
  std::vector<u8> v2 = {20};
  std::vector<u8> v3 = {30};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(k1, v1);
  tree_->Insert(k2, v2);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Update(k1, std::vector<u8>{99}, {});
  tree_->Insert(k3, v3);
  tree_->Remove(k2);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(k1, [&](std::span<const u8> w) { EXPECT_THAT(w, ElementsAreArray(std::vector<u8>({99}))); }),
            OpResult::OK);
  ASSERT_EQ(tree_->LookUp(k2, [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  ASSERT_EQ(tree_->LookUp(k3, [&](std::span<const u8> payload) { EXPECT_THAT(payload, ElementsAreArray(v3)); }),
            OpResult::OK);
  txn_man_->CommitTransaction();
}

}  // namespace leanstore

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count        = 4;
  FLAGS_wal_enable          = true;
  FLAGS_txn_mvcc            = false;  // TODO(XXX): Test SVCC for now
  FLAGS_wal_force_log_flush = false;  // Disable force log flush for testing
  FLAGS_wal_enable_recovery = false;
  FLAGS_wal_batch_write_kb  = 1024 * 1024;  // Very large to prevent group commit from being triggered

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
