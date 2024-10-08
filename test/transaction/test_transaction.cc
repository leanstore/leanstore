#include "common/typedefs.h"
#include "sync/page_state.h"
#include "test/base_test.h"

#include "gtest/gtest.h"

#include <future>
#include <memory>
#include <thread>

namespace leanstore::transaction {

class TestTransaction : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetupTestFile(); }

  void TearDown() override { BaseTest::TearDown(); }
};

TEST_F(TestTransaction, TransactionLifeTime) {
  FLAGS_wal_enable = true;

  InitRandTransaction();
  auto &txn    = TransactionManager::active_txn;
  auto &logger = log_->LocalLogWorker();
  auto &buffer = logger.log_buffer;

  EXPECT_TRUE(txn.IsRunning());
  EXPECT_FALSE(txn.HasBLOB());
  EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - sizeof(recovery::LogMetaEntry));
  EXPECT_EQ(txn.start_ts, 0);
  EXPECT_EQ(txn.state, Transaction::State::STARTED);
  EXPECT_EQ(&txn.LogWorker(), &logger);

  auto thread = std::thread([&]() {
    // Concurrently init another txn
    InitRandTransaction(1);

    auto &x_txn    = TransactionManager::active_txn;
    auto &x_logger = log_->LocalLogWorker();
    EXPECT_NE(&txn, &x_txn);
    EXPECT_NE(&logger, &x_logger);
    EXPECT_EQ(x_txn.start_ts, txn.start_ts + 1);
    EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - sizeof(recovery::LogMetaEntry));
    EXPECT_EQ(x_txn.state, Transaction::State::STARTED);
    EXPECT_EQ(&x_txn.LogWorker(), &x_logger);
    // Commit x_txn
    txn_man_->CommitTransaction();

    if (FLAGS_wal_enable_rfa) {
      EXPECT_EQ(x_logger.precommitted_queue_rfa.SizeApprox(), 1);
    } else {
      EXPECT_EQ(x_logger.precommitted_queue.SizeApprox(), 1);
    }
    EXPECT_EQ(x_txn.is_read_only_, false);
    EXPECT_EQ(x_txn.state, Transaction::State::READY_TO_COMMIT);
  });
  thread.join();

  txn_man_->CommitTransaction();
  if (FLAGS_wal_enable_rfa) {
    EXPECT_EQ(logger.precommitted_queue_rfa.SizeApprox(), 1);
  } else {
    EXPECT_EQ(logger.precommitted_queue.SizeApprox(), 1);
  }
  EXPECT_EQ(txn.is_read_only_, false);
  EXPECT_FALSE(txn.HasBLOB());
  EXPECT_EQ(txn.state, Transaction::State::READY_TO_COMMIT);
}

}  // namespace leanstore::transaction

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = 4;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}