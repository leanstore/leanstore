#include "leanstore/config.h"
#include "recovery/group_commit.h"
#include "test/base_test.h"

#include "fmt/format.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include <sys/stat.h>
#include <chrono>
#include <exception>
#include <filesystem>
#include <future>
#include <thread>
#include <utility>

namespace leanstore::recovery {

class TestGroupCommit : public BaseTest {
 protected:
  static constexpr int BLOB_SIZE = 8192;
  std::atomic<bool> keep_running_;
  u8 *random_blob_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    keep_running_ = true;
    random_blob_  = static_cast<u8 *>(aligned_alloc(BLK_BLOCK_SIZE, BLOB_SIZE));
    for (int i = 0; i < BLOB_SIZE; i++) { random_blob_[i] = rand() % 255; }
  }

  void TearDown() override {
    keep_running_ = false;
    free(random_blob_);
    BaseTest::TearDown();
  }
};

TEST_F(TestGroupCommit, BlobSupportVariant) {
  FLAGS_wal_enable    = true;
  FLAGS_blob_enable   = true;
  extidx_t extent_idx = 5;

  auto gct = std::make_unique<GroupCommitExecutor>(buffer_.get(), log_.get(), 0, FLAGS_worker_count, keep_running_);

  InitRandTransaction();
  auto &txn    = transaction::Transaction::active_txn;
  auto payload = std::span(random_blob_, BLOB_SIZE);
  txn.state    = transaction::Transaction::State::STARTED;

  // Simulate the Blob allocation
  auto start_pid = buffer_->AllocExtent(extent_idx);
  std::memcpy(reinterpret_cast<u8 *>(buffer_->ToPtr(start_pid)), payload.data(), BLOB_SIZE);
  buffer_->PrepareExtentEviction(start_pid);

  // Allocate Blob tuple then Log this Blob payload
  txn.ToFlushedLargePages().emplace_back(start_pid, storage::ExtentList::ExtentSize(extent_idx));
  txn.ToEvictedExtents().emplace_back(start_pid);
  EXPECT_TRUE(txn.HasBLOB());

  // Check required blob async commit
  EXPECT_EQ(txn.ToFlushedLargePages().size(), 1);
  EXPECT_EQ(txn.ToEvictedExtents().size(), 1);

  // Write extents
  EXPECT_EQ(txn.SerializedSize(), 128);
  alignas(CPU_CACHELINE_SIZE) u8 buffer[txn.SerializedSize()];
  auto s_txn = new (buffer) transaction::SerializableTransaction();
  s_txn->Construct(txn);
  gct->PrepareLargePageWrite(*s_txn);
  gct->PhaseTwo();
  EXPECT_EQ(gct->completed_lp_.cardinality(), 1);
  EXPECT_TRUE(gct->completed_lp_.contains(start_pid));

  // Commit current txn
  txn_man_->CommitTransaction();

  // Serialized txn
  auto s1_txn = new (buffer) transaction::SerializableTransaction();
  s1_txn->Construct(txn);
  gct->CompleteTransaction(*s_txn);
  EXPECT_EQ(gct->completed_lp_.cardinality(), 0);

  // Evaluate the raw BLOB content by reading all pages
  // DO NOT DO THIS IN REAL IMPLEMENTATION, CHECK test_blob_manager.cc FOR WHAT YOU SHOULD USE
  for (size_t idx = 1; idx < BLOB_SIZE / PAGE_SIZE; idx++) { buffer_->GetPageState(start_pid + idx).Init(); }
  for (size_t idx = 0; idx < BLOB_SIZE / PAGE_SIZE; idx++) { buffer_->FixShare(start_pid + idx); }
  auto retrieved_dat = std::span<u8>(reinterpret_cast<u8 *>(buffer_->ToPtr(start_pid)), payload.size());
  EXPECT_EQ(std::memcmp(retrieved_dat.data(), random_blob_, payload.size()), 0);
}

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count            = 1;
  FLAGS_txn_commit_group_size   = 1;
  FLAGS_wal_stealing_group_size = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
