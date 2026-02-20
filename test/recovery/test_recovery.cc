#include "common/constants.h"
#include "recovery/log_entry.h"
#include "recovery/log_manager.h"
#include "storage/btree/wal.h"
#include "test/base_test.h"

#include "gtest/gtest.h"
#include "share_headers/time.h"

#include <chrono>
#include <exception>
#include <future>
#include <thread>

namespace leanstore::recovery {

class TestRecovery : public BaseTest {
 protected:
  static constexpr u32 PAYLOAD_SZ = 200;
  LogWorker *logger_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    log_->WriteMasterRecord();
    logger_ = &(log_->LocalLogWorker());
    logger_->backend.Connect();
  }

  void TearDown() override { BaseTest::TearDown(); }

  void NewTxn(u64 no_data_logs) {
    // Simulate TX_START log
    auto &entry = logger_->ReserveLogMetaEntry();
    entry.type  = LogMetaEntry::Type::TX_START;
    logger_->SubmitActiveLogEntry();
    // Submit several data log entries
    for (auto pid = 0U; pid < no_data_logs; pid++) {
      logger_->SetCurrentGSN(logger_->GetCurrentGSN() + 1);
      auto &dt     = logger_->ReserveDataLog(PAYLOAD_SZ, pid);
      auto payload = reinterpret_cast<u8 *>(&dt) + sizeof(DataEntry);
      std::memset(payload, pid, PAYLOAD_SZ);
      auto wal      = reinterpret_cast<storage::WALEntry *>(&dt);
      wal->wal_type = storage::WalType::NEW_PAGE;
      logger_->SubmitActiveLogEntry();
    }
    // TX_COMMIT log
    auto &commit_entry       = logger_->ReserveLogCommitEntry(0);
    commit_entry.vector_size = 0;
    logger_->SubmitActiveLogEntry();
  }

  void EvaluateFirstMetadata(u64 chunk_size, u64 written_size) {
    auto fd = open(FLAGS_db_path.c_str(), O_RDONLY | O_DIRECT, S_IRWXU);
    Ensure(fd > 0);
    auto offset = StorageCapacity(FLAGS_db_path.c_str());
    auto ptr    = AlignedPtr<BLK_BLOCK_SIZE>(4 * KB);
    /* Evaluate Master record */
    auto cnt = pread(fd, ptr.get(), 4 * KB, offset - 4 * KB);
    EXPECT_EQ(cnt, 4 * KB);
    auto master = reinterpret_cast<LogMetaEntry *>(ptr.get());
    EXPECT_EQ(master->rc.gsn, 1);
    EXPECT_GT(master->rc.signature, 0);
    /* Evaluate First metadata */
    cnt = pread(fd, ptr.get(), 4 * KB, offset - 8 * KB);
    EXPECT_EQ(cnt, 4 * KB);
    auto metadata = reinterpret_cast<LogMetaEntry *>(&(ptr.get())[4 * KB - sizeof(LogMetaEntry)]);
    EXPECT_EQ(metadata->rc.chunk_size, chunk_size);
    EXPECT_EQ(metadata->size, written_size);
  }
};

TEST_F(TestRecovery, SmallChunks) {
  // Append several txns and persist the logs
  for (auto idx = 0; idx < 5; idx++) {
    NewTxn(10);
    logger_->log_buffer.LogFlush(logger_, false);
  }
  if (FLAGS_wal_fsync) { fdatasync(log_->wal_fd_); }

  // Try to read from block storage -- evaluate the metadata log entry
  EvaluateFirstMetadata(sizeof(LogMetaEntry) + sizeof(TxnCommitEntry) + (PAYLOAD_SZ + sizeof(DataEntry)) * 10, 4 * KB);

  // Try to analyze the written log chunks
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 1);
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  EXPECT_EQ(recovery_->page_log_[0].size(), 10);

  /* WAL debug */
  for (auto pid = 0; pid < 10; pid++) {
    auto prev_gsn = 0;
    auto count    = 0;
    LogIOSegment::LazyGenerator log_generator;
    for (auto &buffer : recovery_->page_log_[0][pid]) { log_generator.ReceiveInput(&buffer); }
    log_generator.Iterate([&](const LogEntry *log) {
      EXPECT_EQ(log->type, LogMetaEntry::Type::DATA_ENTRY);
      EXPECT_EQ(log->size, sizeof(DataEntry) + PAYLOAD_SZ);
      log->ValidateChksum();
      auto dt = reinterpret_cast<const DataEntry *>(log);
      EXPECT_EQ(dt->rc.txn, 0);
      EXPECT_THAT(dt->rc.gsn, IsInRange(prev_gsn, 60));
      prev_gsn = dt->rc.gsn;
      count++;
      return true;
    });
    EXPECT_EQ(count, 5);
  }
}

TEST_F(TestRecovery, LargeChunk) {
  // Append several txns and persist the logs
  for (auto idx = 0; idx < 5; idx++) { NewTxn(10); }
  logger_->log_buffer.LogFlush(logger_, false);
  if (FLAGS_wal_fsync) { fdatasync(log_->wal_fd_); }

  // Try to read from block storage -- evaluate the metadata log entry
  EvaluateFirstMetadata((sizeof(LogMetaEntry) + sizeof(TxnCommitEntry) + (PAYLOAD_SZ + sizeof(DataEntry)) * 10) * 5,
                        16 * KB);

  // Try to analyze the written log chunks
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 1);
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  EXPECT_EQ(recovery_->page_log_[0].size(), 10);

  /* WAL debug */
  for (auto pid = 0; pid < 10; pid++) {
    auto prev_gsn = 0;
    auto count    = 0;
    LogIOSegment::LazyGenerator log_generator;
    for (auto &buffer : recovery_->page_log_[0][pid]) { log_generator.ReceiveInput(&buffer); }
    log_generator.Iterate([&](const LogEntry *log) {
      EXPECT_EQ(log->type, LogMetaEntry::Type::DATA_ENTRY);
      EXPECT_EQ(log->size, sizeof(DataEntry) + PAYLOAD_SZ);
      log->ValidateChksum();
      auto dt = reinterpret_cast<const DataEntry *>(log);
      EXPECT_EQ(dt->rc.txn, 0);
      EXPECT_THAT(dt->rc.gsn, IsInRange(prev_gsn, 60));
      prev_gsn = dt->rc.gsn;
      count++;
      return true;
    });
    EXPECT_EQ(count, 5);
  }
}

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count            = 1;
  FLAGS_wal_enable              = true;
  FLAGS_wal_debug               = true;
  FLAGS_wal_enable_recovery     = false;
  FLAGS_wal_fsync               = true;
  FLAGS_wal_block_size_mb       = 1;
  FLAGS_wal_stealing_group_size = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}