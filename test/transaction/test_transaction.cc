#include "common/typedefs.h"
#include "sync/page_state.h"
#include "test/base_test.h"

#include "gtest/gtest.h"

#include <future>
#include <memory>
#include <thread>

using LW = leanstore::recovery::LogWorker;

namespace leanstore::transaction {

class TestTransaction : public BaseTest {
 protected:
  void SetUp() override { BaseTest::SetupTestFile(); }

  void TearDown() override { BaseTest::TearDown(); }
};

TEST_F(TestTransaction, TransactionLifeTime) {
  InitRandTransaction();
  auto &txn    = TransactionManager::active_txn;
  auto &logger = log_->LocalLogWorker();
  EXPECT_TRUE(txn.IsRunning());
  EXPECT_EQ(logger.TotalFreeSpace(), LW::WAL_BUFFER_SIZE - sizeof(recovery::LogMetaEntry));
  EXPECT_EQ(txn.StartTS(), 0);
  EXPECT_EQ(txn.wal_start_, 0);
  EXPECT_EQ(txn.state_, Transaction::State::STARTED);
  EXPECT_EQ(&txn.LogWorker(), &logger);

  auto thread = std::thread([&]() {
    // Concurrently init another txn
    InitRandTransaction(1);

    auto &x_txn    = TransactionManager::active_txn;
    auto &x_logger = log_->LocalLogWorker();
    EXPECT_NE(&txn, &x_txn);
    EXPECT_NE(&logger, &x_logger);
    EXPECT_EQ(x_txn.StartTS(), txn.StartTS() + 1);
    EXPECT_EQ(x_logger.TotalFreeSpace(), LW::WAL_BUFFER_SIZE - sizeof(recovery::LogMetaEntry));
    EXPECT_EQ(x_txn.wal_start_, 0);
    EXPECT_EQ(x_txn.state_, Transaction::State::STARTED);
    EXPECT_EQ(&x_txn.LogWorker(), &x_logger);
    // Commit x_txn
    txn_man_->CommitTransaction();

    if (FLAGS_wal_enable_rfa) {
      EXPECT_EQ(x_logger.precommitted_queue_rfa.size(), 1);
      x_txn = x_logger.precommitted_queue_rfa.back();
    } else {
      EXPECT_EQ(x_logger.precommitted_queue.size(), 1);
      x_txn = x_logger.precommitted_queue.back();
    }
    EXPECT_EQ(x_txn.is_read_only_, false);
    EXPECT_EQ(x_txn.state_, Transaction::State::READY_TO_COMMIT);
  });
  thread.join();

  txn_man_->CommitTransaction();
  if (FLAGS_wal_enable_rfa) {
    EXPECT_EQ(logger.precommitted_queue_rfa.size(), 1);
    txn = logger.precommitted_queue_rfa.back();
  } else {
    EXPECT_EQ(logger.precommitted_queue.size(), 1);
    txn = logger.precommitted_queue.back();
  }
  EXPECT_EQ(txn.is_read_only_, false);
  EXPECT_EQ(txn.state_, Transaction::State::READY_TO_COMMIT);
}

TEST_F(TestTransaction, AllocateBlobWithLogging) {
  FLAGS_blob_logging_variant  = -1;
  FLAGS_blob_log_segment_size = 1 * MB;

  // Txn context
  InitRandTransaction();
  auto &txn = TransactionManager::active_txn;
  EXPECT_TRUE(txn.to_write_pages_.empty());
  EXPECT_TRUE(txn.to_evict_extents_.empty());
  auto &logger = log_->LocalLogWorker();

  // Prepare key & Blob
  auto curr_w_cursor = 0;
  auto blob_size     = 3 * MB;
  auto random_blob   = static_cast<u8 *>(aligned_alloc(BLK_BLOCK_SIZE, blob_size));
  for (size_t i = 0; i < blob_size; i++) { random_blob[i] = rand() % 255; }

  // Now Log this Blob
  std::span payload{reinterpret_cast<u8 *>(random_blob), blob_size};
  txn.LogBlob(payload);

  // WAL should have (blob_size / FLAGS_blob_log_segment_size) WALBlob entries
  EXPECT_LT(logger.w_cursor, recovery::LogWorker::WAL_BUFFER_SIZE);
  EXPECT_LT(logger.gct_cursor.load(), logger.w_cursor);

  // First log entry is LogMetaEntry(TX_START)
  curr_w_cursor += sizeof(recovery::LogMetaEntry);
  auto log_tx = reinterpret_cast<recovery::LogMetaEntry *>(&logger.wal_buffer[0]);
  EXPECT_EQ(log_tx->size, curr_w_cursor);
  EXPECT_EQ(log_tx->type, recovery::LogEntry::Type::TX_START);

  for (size_t idx = 0; idx < (blob_size / FLAGS_blob_log_segment_size); idx++) {
    auto log_ent = reinterpret_cast<recovery::BlobEntry *>(&logger.wal_buffer[curr_w_cursor]);
    EXPECT_EQ(log_ent->type, recovery::LogEntry::Type::BLOB_ENTRY);
    EXPECT_EQ(log_ent->part_id, idx);
    EXPECT_EQ(log_ent->size, sizeof(recovery::BlobEntry) + FLAGS_blob_log_segment_size);  // WAL header + BLOB payload
    curr_w_cursor += log_ent->size;
  }

  free(random_blob);
}

}  // namespace leanstore::transaction

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = 4;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}