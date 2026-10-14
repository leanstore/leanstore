#include "recovery/log_manager.h"
#include "test/base_test.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include <array>
#include <cstring>
#include <optional>

namespace leanstore::recovery {

class TestVectorCommit : public BaseTest {
 protected:
  static constexpr wid_t WORKER_A = 0;
  static constexpr wid_t WORKER_B = 1;
  static constexpr wid_t WORKER_C = 2;

  std::unique_ptr<storage::BTree> tree_;

  void SetUp() override {
    LogManager::global_min_gsn_flushed  = 0;
    LogManager::global_sync_to_this_gsn = 0;
    BaseTest::SetupTestFile();
    for (wid_t wid = 0; wid < FLAGS_worker_count; wid++) { Logger(wid).backend.Connect(); }
    LeanStore::worker_thread_id = WORKER_A;
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  void StartTxn(wid_t wid) {
    LeanStore::worker_thread_id = wid;
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::READ_UNCOMMITTED,
                               transaction::Transaction::Mode::OLTP);
    transaction::TransactionManager::active_txn.MarkAsWrite();
  }

  void CommitWithoutAck() { txn_man_->CommitTransaction(true); }

  auto Logger(wid_t wid) -> LogWorker & { return log_->logger_[wid]; }

  void Flush(wid_t wid) {
    LeanStore::worker_thread_id = wid;
    Logger(wid).WorkerStealsLog(true);
  }

  void AckRound() { log_->CommitExecutor(0).ExecuteOneRound(); }

  auto Pending(wid_t wid) -> u64 {
    auto &queue = Logger(wid).precommitted_queue;
    auto count  = 0UL;
    queue.LoopElements(queue.CurrentTail(), [&](auto &) {
      count++;
      return true;
    });
    return count;
  }

  static auto Key(u32 &key) -> std::span<u8> { return {reinterpret_cast<u8 *>(&key), sizeof(key)}; }

  void Insert(int key, u64 value) {
    u32 k = __builtin_bswap32(key);
    tree_->Insert(Key(k), {reinterpret_cast<const u8 *>(&value), sizeof(value)});
  }

  void Update(int key, u64 value) {
    u32 k = __builtin_bswap32(key);
    tree_->UpdateInPlace(
      Key(k), [&](std::span<u8> payload) { std::memcpy(payload.data(), &value, sizeof(value)); }, nullptr);
  }

  auto Read(int key) -> std::optional<u64> {
    u32 k = __builtin_bswap32(key);
    std::optional<u64> ret;
    tree_->LookUp(Key(k), [&](std::span<const u8> payload) {
      u64 value;
      std::memcpy(&value, payload.data(), sizeof(value));
      ret = value;
    });
    return ret;
  }

  /* Worker A creates the tree with `no_keys` keys, all of it durable and acknowledged */
  void Populate(int no_keys) {
    log_->WriteMasterRecord();
    StartTxn(WORKER_A);
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), recovery_.get(), 0);
    for (auto key = 0; key < no_keys; key++) { Insert(key, 0); }
    CommitWithoutAck();
    Flush(WORKER_A);
    AckRound();
    ASSERT_EQ(Pending(WORKER_A), 0);
  }

  /* Drop every log buffer that was not written, then recover the tree from the log */
  void CrashAndRecover() {
    tree_.reset();
    BaseTest::TearDown();
    FLAGS_wal_enable_recovery  = true;
    FLAGS_wal_instant_recovery = true;
    SetUp();
    StartTxn(WORKER_A);
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), recovery_.get(), 0);
    recovery_->RecoveryPreparation();
    recovery_->MaterializeLogs();
    recovery_->Analysis();
    recovery_->PrepareRedoEnv();
  }

  void ResetFlags() {
    FLAGS_wal_enable_recovery  = false;
    FLAGS_wal_instant_recovery = false;
  }
};

/* A transaction depending twice on the same worker must keep the larger GSN */
TEST_F(TestVectorCommit, DependencyKeepsLargestGSN) {
  static constexpr int NO_KEYS = 2000;
  Populate(NO_KEYS);

  StartTxn(WORKER_B);
  Update(NO_KEYS - 1, 1);
  Update(0, 1);
  const auto newest = Logger(WORKER_B).GetCurrentGSN();
  CommitWithoutAck();

  StartTxn(WORKER_C);
  Update(0, 2);
  Update(NO_KEYS - 1, 2);
  auto &vector = transaction::TransactionManager::active_txn.gsn_vector;
  ASSERT_TRUE(vector.contains(WORKER_B));
  EXPECT_EQ(vector[WORKER_B], newest);
  CommitWithoutAck();
}

}  // namespace leanstore::recovery

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count            = 3;
  FLAGS_wal_enable              = true;
  FLAGS_wal_variant             = 2;
  FLAGS_txn_commit_variant      = 3;
  FLAGS_wal_enable_recovery     = false;
  FLAGS_wal_fsync               = true;
  FLAGS_wal_block_size_mb       = 2;
  FLAGS_wal_batch_write_kb      = 16;
  FLAGS_wal_stealing_group_size = 1;
  FLAGS_txn_commit_group_size   = 3;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
