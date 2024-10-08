#include "common/constants.h"
#include "recovery/log_entry.h"
#include "recovery/log_manager.h"
#include "test/base_test.h"
#include "transaction/transaction_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <chrono>
#include <exception>
#include <future>
#include <thread>

namespace leanstore::recovery {

class TestLogManager : public ::testing::Test {
 protected:
  std::atomic<bool> is_running_;

  void SetUp() override {
    is_running_   = true;
    FLAGS_db_path = BLOCK_DEVICE;
  }
};

TEST_F(TestLogManager, NormalOperation) {
  FLAGS_wal_enable             = true;
  FLAGS_wal_centralized_buffer = false;
  worker_thread_id             = 0;

  auto log_man = std::make_unique<LogManager>(is_running_);
  auto &logger = log_man->LocalLogWorker();
  auto &buffer = logger.log_buffer;

  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  u64 current_log_size = FLAGS_wal_buffer_size_mb * MB;
  // Simulate TX_START log
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogMetaEntry::Type::TX_START;
  logger.SubmitActiveLogEntry();
  current_log_size -= sizeof(LogMetaEntry);
  EXPECT_EQ(buffer.TotalFreeSpace(), current_log_size);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // Submit Big Data log
  auto &dt_entry = logger.ReserveDataLog(FLAGS_wal_buffer_size_mb * MB - 200, 0);
  logger.SubmitActiveLogEntry();
  current_log_size -= dt_entry.size;
  // Extra padding might be added to enfore memory-alignment of the log entry
  EXPECT_EQ(logger.active_log->size, FLAGS_wal_buffer_size_mb * MB - 200 + sizeof(DataEntry));
  EXPECT_EQ(buffer.TotalFreeSpace(), current_log_size);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // As the wal buffer is now full, any effort to EnsureEnoughSpace(big-payload)
  //  results in an infinite loop
  std::thread thread([&]() { buffer.EnsureEnoughSpace(&logger, 200); });
  auto future = std::async(std::launch::async, &std::thread::join, &thread);
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  // Submit TX_COMMIT log, it should be right after the data log
  auto &commit_entry = logger.ReserveLogMetaEntry();
  commit_entry.type  = LogMetaEntry::Type::TX_COMMIT;
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(buffer.TotalFreeSpace(), current_log_size - sizeof(LogMetaEntry));
  // Assume that the group-commit worker flushes the first two logs
  buffer.write_cursor.store(entry.size + dt_entry.size);
  // wait for the EnsureEnoughSpace to complete its execution
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // After the group-commit flush, w_cursor should circulate back to start,
  //  i.e. a cr entry should be inserted to the end
  auto size_of_3logs = entry.size + dt_entry.size + commit_entry.size;
  auto cr_entry      = reinterpret_cast<LogEntry *>(&buffer.wal_buffer[size_of_3logs]);
  EXPECT_EQ(cr_entry->size, FLAGS_wal_buffer_size_mb * MB - size_of_3logs);
  // Task EnsureEnoughSpace should complete now, and w_cursor
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_EQ(buffer.wal_cursor, 0);
  EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - commit_entry.size - cr_entry->size);
  EXPECT_EQ(buffer.ContiguousFreeSpaceForNewEntry(), buffer.write_cursor.load() - buffer.wal_cursor);
}

TEST_F(TestLogManager, CentralizedLogging) {
  FLAGS_wal_enable             = true;
  FLAGS_wal_centralized_buffer = true;
  worker_thread_id             = 0;

  auto log_man          = std::make_unique<LogManager>(is_running_);
  auto &logger          = log_man->LocalLogWorker();
  auto &buffer          = logger.log_buffer;
  auto &centralized_buf = *log_man->centralized_buf_;

  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), centralized_buf.ContiguousFreeSpaceForNewEntry());
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), FLAGS_worker_count * buffer.TotalFreeSpace());
  const auto upper_bound = FLAGS_wal_buffer_size_mb * MB;

  // Simulate TX_START log
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogMetaEntry::Type::TX_START;
  EXPECT_EQ(buffer.TotalFreeSpace(), upper_bound);
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), FLAGS_worker_count * upper_bound);

  // Submit a log entry
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(buffer.TotalFreeSpace(), upper_bound);
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), FLAGS_worker_count * upper_bound - sizeof(LogMetaEntry));

  // Generate N big log entries
  const auto log_size = FLAGS_wal_buffer_size_mb * MB - 200 + sizeof(DataEntry);
  for (auto idx = 0UL; idx < FLAGS_worker_count; idx++) {
    logger.ReserveDataLog(FLAGS_wal_buffer_size_mb * MB - 200, 0);
    EXPECT_EQ(buffer.TotalFreeSpace(), upper_bound);
    EXPECT_EQ(centralized_buf.TotalFreeSpace(),
              FLAGS_worker_count * upper_bound - sizeof(LogMetaEntry) - idx * log_size);

    logger.SubmitActiveLogEntry();
    EXPECT_EQ(buffer.TotalFreeSpace(), upper_bound);
    EXPECT_EQ(centralized_buf.TotalFreeSpace(),
              FLAGS_worker_count * upper_bound - sizeof(LogMetaEntry) - (idx + 1) * log_size);
    EXPECT_EQ(centralized_buf.TotalFreeSpace(), centralized_buf.ContiguousFreeSpaceForNewEntry());
  }

  // As the wal buffer is now full, any effort to EnsureEnoughSpace(big-payload) results in an infinite loop
  std::thread thread([&]() { centralized_buf.EnsureEnoughSpace(&logger, 200); });
  auto future = std::async(std::launch::async, &std::thread::join, &thread);
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

  // Submit TX_COMMIT log, it should be right after the data log
  auto &commit_entry = logger.ReserveLogMetaEntry();
  commit_entry.type  = LogMetaEntry::Type::TX_COMMIT;
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), FLAGS_worker_count * upper_bound -  // Max size of log buffer
                                                sizeof(LogMetaEntry) -            // TX_START log entry
                                                FLAGS_worker_count * log_size -   // N big log entries
                                                sizeof(LogMetaEntry));            // TX_COMMIT log entry
  // Assume that the group-commit worker flushes the first N+1 logs
  centralized_buf.write_cursor.store(entry.size + FLAGS_worker_count * log_size);
  // wait for the EnsureEnoughSpace to complete its execution
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  // After the group-commit flush, w_cursor should circulate back to start,
  //  i.e. a cr entry should be inserted to the end
  auto size_of_all_logs = entry.size + FLAGS_worker_count * log_size + commit_entry.size;
  auto cr_entry         = reinterpret_cast<LogEntry *>(&centralized_buf.wal_buffer[size_of_all_logs]);
  EXPECT_EQ(cr_entry->size, FLAGS_wal_buffer_size_mb * MB - size_of_all_logs);
  // Task EnsureEnoughSpace should complete now, and w_cursor
  EXPECT_EQ(future.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
  EXPECT_EQ(centralized_buf.wal_cursor, 0);
  EXPECT_EQ(centralized_buf.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - commit_entry.size - cr_entry->size);
  EXPECT_EQ(centralized_buf.ContiguousFreeSpaceForNewEntry(),
            centralized_buf.write_cursor.load() - centralized_buf.wal_cursor);
}

TEST_F(TestLogManager, AutonomousCommit) {
  FLAGS_wal_enable         = true;
  FLAGS_txn_commit_variant = static_cast<int>(transaction::CommitProtocol::WORKERS_WRITE_LOG);
  worker_thread_id         = 0;

  auto log_man = std::make_unique<LogManager>(is_running_);
  auto &logger = log_man->LocalLogWorker();
  auto &buffer = logger.log_buffer;

  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  u64 current_free_space = FLAGS_wal_buffer_size_mb * MB;
  // Simulate TX_START log
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogMetaEntry::Type::TX_START;
  logger.SubmitActiveLogEntry();
  current_free_space -= sizeof(LogMetaEntry);
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // Submit Big Data log
  auto &dt_entry = logger.ReserveDataLog(FLAGS_wal_buffer_size_mb * MB - 200, 0);
  logger.SubmitActiveLogEntry();
  current_free_space -= dt_entry.size;
  // Extra padding might be added to enfore memory-alignment of the log entry
  EXPECT_EQ(logger.active_log->size, FLAGS_wal_buffer_size_mb * MB - 200 + sizeof(DataEntry));
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space);
  EXPECT_EQ(buffer.TotalFreeSpace(), buffer.ContiguousFreeSpaceForNewEntry());
  // EnsureEnoughSpace should complete successfully as worker autonomously write logs,
  //   and wal_cursor should circular back to the beginning
  buffer.EnsureEnoughSpace(&logger, 200);
  EXPECT_EQ(buffer.TotalFreeSpace(), FLAGS_wal_buffer_size_mb * MB - current_free_space);
  EXPECT_EQ(buffer.wal_cursor, 0);
  // Submit TX_COMMIT log, it should be right after the data log
  current_free_space = FLAGS_wal_buffer_size_mb * MB - current_free_space;
  auto &commit_entry = logger.ReserveLogMetaEntry();
  commit_entry.type  = LogMetaEntry::Type::TX_COMMIT;
  logger.SubmitActiveLogEntry();
  EXPECT_EQ(buffer.wal_cursor, commit_entry.size);
  EXPECT_EQ(buffer.TotalFreeSpace(), current_free_space - sizeof(LogMetaEntry));
  EXPECT_EQ(buffer.ContiguousFreeSpaceForNewEntry(), buffer.write_cursor.load() - buffer.wal_cursor);
}

}  // namespace leanstore::recovery