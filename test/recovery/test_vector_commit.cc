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
    LogManager::global_min_gsn_stable   = 0;
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

/**
 * T1 (A) -> T2 (B) -> T3 (C) update the same tuple. B and C write their own logs, A does not.
 * T3 depends on T1 only through T2, so acknowledging T3 before T1 is durable loses T3 on a crash:
 *  recovery marks T2 a loser, and the redo of the page stops at T2's entry.
 */
TEST_F(TestVectorCommit, TransitiveDependency) {
  Populate(1);

  StartTxn(WORKER_A);
  Update(0, 1);
  CommitWithoutAck();

  StartTxn(WORKER_B);
  Update(0, 2);
  CommitWithoutAck();
  Flush(WORKER_B);

  StartTxn(WORKER_C);
  Update(0, 3);
  CommitWithoutAck();
  Flush(WORKER_C);

  AckRound();
  EXPECT_EQ(Pending(WORKER_A), 1);
  EXPECT_EQ(Pending(WORKER_B), 1);
  EXPECT_EQ(Pending(WORKER_C), 1);
  const auto t3_acked = Pending(WORKER_C) == 0;

  CrashAndRecover();
  auto value = Read(0);
  txn_man_->CommitTransaction(true);
  ResetFlags();
  ASSERT_TRUE(value.has_value());
  if (t3_acked) { EXPECT_EQ(*value, 3); }
}

/**
 * Same chain, but T3 also updates a second page alone.
 * T3 is a loser through T2, so recovery must not redo its update of the second page either
 */
TEST_F(TestVectorCommit, TransitiveLoserIsNotPartiallyRecovered) {
  static constexpr int NO_KEYS = 2000;
  static constexpr int FIRST   = 0;
  static constexpr int LAST    = NO_KEYS - 1;
  Populate(NO_KEYS);

  StartTxn(WORKER_A);
  Update(FIRST, 1);
  CommitWithoutAck();

  StartTxn(WORKER_B);
  Update(FIRST, 2);
  CommitWithoutAck();
  Flush(WORKER_B);

  StartTxn(WORKER_C);
  Update(FIRST, 3);
  Update(LAST, 3);
  CommitWithoutAck();
  Flush(WORKER_C);

  CrashAndRecover();
  auto first = Read(FIRST);
  auto last  = Read(LAST);
  txn_man_->CommitTransaction(true);
  ResetFlags();
  EXPECT_EQ(first, 0);
  EXPECT_EQ(last, 0);
}

/* T4 has no dependencies of its own, but follows T3 on C, so it must wait for T3 */
TEST_F(TestVectorCommit, CommitWaitsForEarlierCommitOfItsWorker) {
  static constexpr int NO_KEYS = 2000;
  static constexpr int FIRST   = 0;
  static constexpr int LAST    = NO_KEYS - 1;
  Populate(NO_KEYS);

  StartTxn(WORKER_A);
  Update(FIRST, 1);
  CommitWithoutAck();

  StartTxn(WORKER_C);
  Update(FIRST, 3);
  CommitWithoutAck();

  StartTxn(WORKER_C);
  Update(LAST, 4);
  CommitWithoutAck();
  Flush(WORKER_C);

  AckRound();
  EXPECT_EQ(Pending(WORKER_C), 2);

  Flush(WORKER_A);
  AckRound();
  EXPECT_EQ(Pending(WORKER_A), 0);
  EXPECT_EQ(Pending(WORKER_C), 0);
}

/* Once the head of the chain is durable, every transaction of the chain is acknowledged and recovered */
TEST_F(TestVectorCommit, TransitiveDependencyResolved) {
  Populate(1);

  StartTxn(WORKER_A);
  Update(0, 1);
  CommitWithoutAck();

  StartTxn(WORKER_B);
  Update(0, 2);
  CommitWithoutAck();
  Flush(WORKER_B);

  StartTxn(WORKER_C);
  Update(0, 3);
  CommitWithoutAck();
  Flush(WORKER_C);

  Flush(WORKER_A);
  AckRound();
  EXPECT_EQ(Pending(WORKER_A), 0);
  EXPECT_EQ(Pending(WORKER_B), 0);
  EXPECT_EQ(Pending(WORKER_C), 0);

  CrashAndRecover();
  auto value = Read(0);
  txn_man_->CommitTransaction(true);
  ResetFlags();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 3);
}

/* A and B update two pages in opposite order, so T1 and T2 depend on each other */
TEST_F(TestVectorCommit, CyclicDependency) {
  static constexpr int NO_KEYS = 2000;
  Populate(NO_KEYS);
  static constexpr int FIRST = 0;
  static constexpr int LAST  = NO_KEYS - 1;

  StartTxn(WORKER_A);
  Update(FIRST, 1);
  auto t1 = transaction::TransactionManager::active_txn;
  transaction::TransactionManager::active_txn.ResetState();

  StartTxn(WORKER_B);
  Update(LAST, 2);
  Update(FIRST, 2);
  auto t2 = transaction::TransactionManager::active_txn;
  transaction::TransactionManager::active_txn.ResetState();

  LeanStore::worker_thread_id                 = WORKER_A;
  transaction::TransactionManager::active_txn = t1;
  Update(LAST, 1);
  ASSERT_TRUE(transaction::TransactionManager::active_txn.gsn_vector.contains(WORKER_B));
  CommitWithoutAck();

  LeanStore::worker_thread_id                 = WORKER_B;
  transaction::TransactionManager::active_txn = t2;
  ASSERT_TRUE(transaction::TransactionManager::active_txn.gsn_vector.contains(WORKER_A));
  CommitWithoutAck();

  Flush(WORKER_A);
  Flush(WORKER_B);
  AckRound();
  EXPECT_EQ(Pending(WORKER_A), 0);
  EXPECT_EQ(Pending(WORKER_B), 0);

  CrashAndRecover();
  auto first = Read(FIRST);
  auto last  = Read(LAST);
  txn_man_->CommitTransaction(true);
  ResetFlags();
  EXPECT_EQ(first, 2);
  EXPECT_EQ(last, 1);
}

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
