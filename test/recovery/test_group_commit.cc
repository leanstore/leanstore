#include "leanstore/config.h"
#include "recovery/group_commit.h"
#include "test/base_test.h"

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

class TestGroupCommitBlob : public TestGroupCommit, public ::testing::WithParamInterface<int> {};

TEST_F(TestGroupCommit, BasicOperation) {
  worker_thread_id = 0;

  auto gct = std::make_unique<GroupCommitExecutor>(buffer_.get(), log_.get(), keep_running_);
  EXPECT_EQ(*(gct->keep_running_), true);
  auto &logger = log_->LocalLogWorker();
  InitRandTransaction();
  auto &txn = transaction::TransactionManager::active_txn;
  logger.ReserveDataLog(LogWorker::WAL_BUFFER_SIZE - 100, 0, 0);
  logger.SubmitActiveLogEntry();
  // The local logger should contain two entries: TX_START & very big DT_ENTRY
  EXPECT_EQ(logger.TotalFreeSpace(), 100 - sizeof(LogMetaEntry) - sizeof(DataEntry));

  auto thread = std::thread([&]() {
    // Hack(clone current env to this async thread for testing only)
    worker_thread_id                            = 0;
    transaction::TransactionManager::active_txn = txn;

    // Commit should be blocked until the wal buffer has enough space for commit log entry
    txn_man_->CommitTransaction();
  });
  auto future = std::async(std::launch::async, &std::thread::join, &thread);
  // The local log is full, commit txn (i.e. append TX_COMMIT log entry) should be unsucessful
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  // Start group commit to flush log entries to disk
  gct->ExecuteOneRound();
  // After flushing, commit should succeed
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
}

TEST_P(TestGroupCommitBlob, BlobSupportVariant) {
  FLAGS_blob_logging_variant = GetParam();
  extidx_t extent_idx        = 5;

  auto gct = std::make_unique<GroupCommitExecutor>(buffer_.get(), log_.get(), keep_running_);

  InitRandTransaction();
  auto &txn    = transaction::TransactionManager::active_txn;
  auto payload = std::span(random_blob_, BLOB_SIZE);

  // Simulate the Blob allocation
  auto start_pid = buffer_->AllocExtent(extent_idx, 0);
  std::memcpy(reinterpret_cast<u8 *>(buffer_->ToPtr(start_pid)), payload.data(), BLOB_SIZE);
  buffer_->PrepareExtentEviction(start_pid);

  // Allocate Blob tuple then Log this Blob payload
  txn.ToFlushedLargePages().emplace_back(start_pid, storage::ExtentList::ExtentSize(extent_idx));
  txn.ToEvictedExtents().emplace_back(start_pid, storage::ExtentList::ExtentSize(extent_idx));

  // Check required blob async commit
  gct->PrepareLargePageWrite(txn);
  gct->PhaseFour();
  EXPECT_EQ(gct->completed_lp_.size(), 1);
  for (auto &blob_id : gct->completed_lp_) { EXPECT_EQ(blob_id, start_pid); }
  gct->CompleteTxnBlobs(txn);
  EXPECT_EQ(gct->completed_lp_.size(), 0);

  // Commit current txn
  txn_man_->CommitTransaction();

  // Evaluate the BLOB content
  for (size_t idx = 0; idx < BLOB_SIZE / PAGE_SIZE; idx++) { buffer_->FixShare(start_pid + idx); }
  auto retrieved_dat = std::span<u8>(reinterpret_cast<u8 *>(buffer_->ToPtr(start_pid)), payload.size());
  EXPECT_EQ(std::memcmp(retrieved_dat.data(), random_blob_, payload.size()), 0);
}

INSTANTIATE_TEST_SUITE_P(TestGroupCommit, TestGroupCommitBlob, ::testing::Values(0, 1, 2));

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_wal_max_qd   = 8;
  FLAGS_worker_count = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
