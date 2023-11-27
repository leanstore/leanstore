#include "storage/btree/locking.h"
#include "storage/btree/node.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace leanstore::storage {

template <class T>
struct MockGuardO : public GuardO<T> {
  MockGuardO(buffer::BufferManager *buffer, pageid_t pid) : GuardO<T>(buffer, pid) {}

  MOCK_METHOD0(Die, void());

  virtual ~MockGuardO() { Die(); }
};

template <class T>
struct MockGuardX : public GuardX<T> {
  MockGuardX(buffer::BufferManager *buffer, pageid_t pid) : GuardX<T>(buffer, pid) {}

  MOCK_METHOD0(Die, void());

  static void Dumb([[maybe_unused]] MockGuardX<T> &&parent, [[maybe_unused]] MockGuardX<BTreeNode> &&child) {
    LOG_DEBUG("Calling Dumb method");
  }

  virtual ~MockGuardX() { Die(); }
};

class TestBTreeLocking : public BaseTest {
 protected:
  std::unique_ptr<BTree> tree_;
  static constexpr leng_t ROOT_PID = 1;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<BTree>(buffer_.get());
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }
};

TEST_F(TestBTreeLocking, GenerateGuard) {
  {
    auto op_guard = GuardO<BTreeNode>(buffer_.get(), 1);
    EXPECT_EQ(op_guard.Mode(), sync::GuardMode::OPTIMISTIC);
  }
  {
    auto guard = GuardX<BTreeNode>(buffer_.get(), 1);
    EXPECT_EQ(guard.Mode(), sync::GuardMode::EXCLUSIVE);
  }
  {
    auto guard = GuardS<BTreeNode>(buffer_.get(), 1);
    EXPECT_EQ(guard.Mode(), sync::GuardMode::SHARED);
  }
}

TEST_F(TestBTreeLocking, GuardUpgrade) {
  MockGuardO<BTreeNode> op_guard(buffer_.get(), ROOT_PID);
  EXPECT_EQ(op_guard.Mode(), sync::GuardMode::OPTIMISTIC);
  GuardS<BTreeNode> s_upgrade(std::move(op_guard));
  EXPECT_CALL(op_guard, Die()).Times(1);  // NOLINT
}

TEST_F(TestBTreeLocking, SimpleLockCoupling) {
  MockGuardO<MetadataPage> meta(buffer_.get(), METADATA_PAGE_ID);
  EXPECT_EQ(meta.Mode(), sync::GuardMode::OPTIMISTIC);
  GuardO<BTreeNode> node(buffer_.get(), ROOT_PID, meta);
  // Parent check won't MOVED the OPTIMISTIC GUARD
  EXPECT_EQ(meta.Mode(), sync::GuardMode::OPTIMISTIC);
  EXPECT_NE(meta.Mode(), sync::GuardMode::MOVED);
  EXPECT_CALL(meta, Die()).Times(1);
}

TEST_F(TestBTreeLocking, FakeSplitExclusiveGuard) {
  MockGuardX<MetadataPage> parent(buffer_.get(), METADATA_PAGE_ID);
  EXPECT_EQ(parent.Mode(), sync::GuardMode::EXCLUSIVE);
  MockGuardX<BTreeNode> node(buffer_.get(), ROOT_PID);
  EXPECT_EQ(parent.Mode(), sync::GuardMode::EXCLUSIVE);
  EXPECT_EQ(node.Mode(), sync::GuardMode::EXCLUSIVE);
  MockGuardX<MetadataPage>::Dumb(std::move(parent), std::move(node));
  EXPECT_CALL(parent, Die()).Times(1);  // NOLINT
  EXPECT_CALL(node, Die()).Times(1);    // NOLINT
}

}  // namespace leanstore::storage