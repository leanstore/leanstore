#include "common/constants.h"
#include "recovery/log_entry.h"
#include "recovery/log_manager.h"
#include "transaction/transaction_manager.h"

#include "gtest/gtest.h"

#include <chrono>
#include <exception>
#include <future>
#include <thread>

namespace leanstore::recovery {

class TestLogManager : public ::testing::Test {
 protected:
  std::unique_ptr<LogManager> log_;
  std::atomic<bool> is_running_;

  void SetUp() override {
    is_running_ = true;
    log_        = std::make_unique<LogManager>(is_running_);
  }

  void TearDown() override { log_.reset(); }
};

TEST_F(TestLogManager, NormalOperation) {
  worker_thread_id = 0;

  auto &logger = log_->LocalLogWorker();
  EXPECT_EQ(logger.TotalFreeSpace(), logger.ContiguousFreeSpaceForNewEntry());
  u64 current_log_size = LogWorker::WAL_BUFFER_SIZE;
  // Simulate TX_START log
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogMetaEntry::Type::TX_START;
  logger.SubmitActiveLogEntry();
  current_log_size -= sizeof(LogMetaEntry);
  EXPECT_EQ(logger.TotalFreeSpace(), current_log_size);
  EXPECT_EQ(logger.TotalFreeSpace(), logger.ContiguousFreeSpaceForNewEntry());
  // Submit Big Data log
  auto &dt_entry = logger.ReserveDataLog(LogWorker::WAL_BUFFER_SIZE - 200, 0, 0);
  logger.SubmitActiveLogEntry();
  current_log_size -= dt_entry.size;
  // Extra padding might be added to enfore memory-alignment of the log entry
  EXPECT_EQ(logger.active_log->size, LogWorker::WAL_BUFFER_SIZE - 200 + sizeof(DataEntry));
  EXPECT_EQ(logger.TotalFreeSpace(), current_log_size);
  EXPECT_EQ(logger.TotalFreeSpace(), logger.ContiguousFreeSpaceForNewEntry());
  // As the wal buffer is now full, any effort to EnsureEnoughSpace(big-payload)
  //  results in an infinite loop
  std::thread thread([&]() { logger.EnsureEnoughSpace(200); });
  auto future = std::async(std::launch::async, &std::thread::join, &thread);
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  // Submit TX_COMMIT log, it should be right after the data log
  auto &commit_entry = logger.ReserveLogMetaEntry();
  commit_entry.type  = LogMetaEntry::Type::TX_COMMIT;
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(logger.TotalFreeSpace(), current_log_size - sizeof(LogMetaEntry));
  // Assume that the group-commit worker flushes the first two logs
  logger.gct_cursor.store(entry.size + dt_entry.size);
  // wait for the EnsureEnoughSpace to complete its execution
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // After the group-commit flush, w_cursor should circulate back to start,
  //  i.e. a cr entry should be inserted to the end
  auto size_of_3logs = entry.size + dt_entry.size + commit_entry.size;
  auto cr_entry      = reinterpret_cast<LogEntry *>(&logger.wal_buffer[size_of_3logs]);
  EXPECT_EQ(cr_entry->size, LogWorker::WAL_BUFFER_SIZE - size_of_3logs);
  // Task EnsureEnoughSpace should complete now, and w_cursor
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_EQ(logger.w_cursor, 0);
  EXPECT_EQ(logger.TotalFreeSpace(), LogWorker::WAL_BUFFER_SIZE - commit_entry.size - cr_entry->size);
  EXPECT_EQ(logger.ContiguousFreeSpaceForNewEntry(), logger.gct_cursor.load() - logger.w_cursor);
}

}  // namespace leanstore::recovery