#include "leanstore/env.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gtest/gtest.h"

#include <cstring>
#include <unordered_map>

namespace leanstore::recovery {

static constexpr u32 NO_RECORDS = 20000;

class TestTreeRecovery : public BaseTest {
 protected:
  std::unique_ptr<storage::BTree> tree_;
  LogWorker *logger_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    LeanStore::worker_thread_id = 0;
    logger_                     = &(log_->LocalLogWorker());
    logger_->backend.Connect();
  }

  void InitializeTree() {
    InitRandTransaction();
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), recovery_.get(), 0);
    txn_man_->CommitTransaction();
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }

  void ForcePersistLogs() {
    if (logger_->log_buffer.TotalFreeSpace() > 0) { logger_->log_buffer.LogFlush(logger_, false); }
    if (FLAGS_wal_fsync) { fdatasync(log_->wal_fd_); }
  }

  template <typename value_t>
  void PrepareData(std::vector<std::pair<int, value_t>> &data, bool get_permutation = false, bool prefill_tree = true) {
    for (size_t idx = 0; idx < NO_RECORDS; idx++) {
      data.emplace_back(static_cast<int>(__builtin_bswap32(idx + 1)), static_cast<value_t>(idx * 100));
    }
    if (get_permutation) {
      for (size_t idx = 0; idx < rand() % NO_RECORDS + NO_RECORDS / 2; idx++) {
        std::next_permutation(data.begin(), data.end());
      }
    }
    ConvenientTxnWrapper([&]() { Ensure(!tree_->IsNotEmpty()); });
    if (prefill_tree) {
      txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::READ_UNCOMMITTED,
                                 transaction::Transaction::Mode::OLTP);
      for (auto &pair : data) {
        std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
        std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(value_t)};
        tree_->Insert(key, payload);
        if (logger_->log_buffer.TotalFreeSpace() >= FLAGS_wal_batch_write_kb) {
          logger_->log_buffer.LogFlush(logger_, false);
        }
      }
      txn_man_->CommitTransaction();
      ForcePersistLogs();
      ConvenientTxnWrapper([&]() { Ensure(tree_->IsNotEmpty()); });
    }
  }

  void EvaluateLogData(u64 no_pages) {
    for (auto pid = 1U; pid <= no_pages; pid++) {
      auto buffer   = AlignedPtr(PAGE_SIZE);
      auto prev_gsn = 1;
      LogIOSegment::LazyGenerator log_generator;
      for (auto &buffer : recovery_->page_log_[0][pid]) { log_generator.ReceiveInput(&buffer); }
      log_generator.Iterate([&](const LogEntry *log) {
        log->ValidateChksum();
        auto dt = reinterpret_cast<const DataEntry *>(log);
        EXPECT_GT(dt->rc.gsn, prev_gsn);
        /* First step should always be a page initialization */
        auto entry = reinterpret_cast<const storage::WALEntry *>(log);
        if (prev_gsn == 0) {
          EXPECT_TRUE((entry->wal_type == storage::WalType::NEW_PAGE) ||
                      (entry->wal_type == storage::WalType::NEW_ROOT) ||
                      (entry->wal_type == storage::WalType::FRESH_PAGE));
        }
        EXPECT_EQ(reinterpret_cast<const DataEntry *>(log)->prev_gsn, prev_gsn);
        prev_gsn = dt->rc.gsn;
        return true;
      });
    }
  }
};

TEST_F(TestTreeRecovery, BulkInsertRecovery) {
  FLAGS_wal_enable_recovery = false;

  /* Initialize recovery env */
  log_->WriteMasterRecord();
  spdlog::info("signature {}", leanstore::LeanStore::signature);
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);
  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, 342);

  /* Make a clone of the current buffer pool */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Reset the state */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  /* Load written log segments into the recovery manager */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->page_log_[0].size(), no_pages + 1);
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 3);  // TXN 0 and TXN 2
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  auto max_pid = std::max_element(std::begin(recovery_->page_log_[0]), std::end(recovery_->page_log_[0]),
                                  [](const auto &p1, const auto &p2) { return p1.first < p2.first; });
  EXPECT_EQ((*max_pid).first, 342);

  /* Try to evaluate recovered page data to the current page data */
  EvaluateLogData(no_pages);

  /* Try to redo every page */
  InitRandTransaction();
  recovery_->PrepareRedoEnv();
  auto metadata_page      = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
  metadata_page->roots[0] = 1;
  for (auto pid = 1U; pid <= no_pages; pid++) {
    sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
    recovery_->PerPageRedo(page, pid);
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
  }
}

TEST_F(TestTreeRecovery, MixWorkloads) {
  FLAGS_wal_enable_recovery = false;

  /* Initialize recovery env */
  log_->WriteMasterRecord();
  spdlog::info("signature {}", leanstore::LeanStore::signature);
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);
  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, 342);

  /* Random UpdateInPlace */
  InitRandTransaction();
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS / 100); idx++) {
    auto start_tid = RandomGenerator::GetRand<int>(idx + 1, NO_RECORDS - 50);
    for (auto tid = start_tid; tid < start_tid + 50; tid++) {
      auto &[key, value] = data[tid];
      tree_->UpdateInPlace(
        {reinterpret_cast<u8 *>(&key), sizeof(int)},
        [&](auto payload) {
          std::span new_w{reinterpret_cast<u8 *>(&value), sizeof(__uint128_t)};
          EXPECT_EQ(payload.size(), sizeof(__uint128_t));
          auto cmp = std::memcmp(payload.data(), new_w.data(), sizeof(__uint128_t));
          EXPECT_EQ(cmp, 0);
          std::memcpy(payload.data(), new_w.data(), new_w.size());
        },
        nullptr);
    }
  }
  txn_man_->CommitTransaction();

  /* Should be no change from UpdateInPlace */
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, 342);

  /* Random deletion */
  InitRandTransaction();
  for (auto idx = 0; idx < static_cast<int>(NO_RECORDS / 100); idx++) {
    auto start_tid = RandomGenerator::GetRand<int>(idx + 1, NO_RECORDS);
    for (auto tid = start_tid; tid < start_tid + 50; tid++) {
      int key = __builtin_bswap32(tid);
      tree_->Remove({reinterpret_cast<u8 *>(&key), sizeof(int)});
    }
  }
  no_pages = tree_->CountPages();
  EXPECT_LT(no_pages, 342);
  txn_man_->CommitTransaction();

  /* Force write logs if there are dirty ones */
  ForcePersistLogs();

  /* Make a clone of the current buffer pool */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Reset the state */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();

  /* Load written log segments into the recovery manager */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->page_log_[0].size(), 343);
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 8);
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  auto max_pid = std::max_element(std::begin(recovery_->page_log_[0]), std::end(recovery_->page_log_[0]),
                                  [](const auto &p1, const auto &p2) { return p1.first < p2.first; });
  EXPECT_EQ((*max_pid).first, 342);

  /* Try to evaluate recovered page data to the current page data */
  EvaluateLogData(no_pages);

  /* Try to redo every page */
  InitRandTransaction();
  recovery_->PrepareRedoEnv();
  auto metadata_page      = reinterpret_cast<storage::MetadataPage *>(buffer_->ToPtr(0));
  metadata_page->roots[0] = 1;
  for (auto pid = 1U; pid <= no_pages; pid++) {
    sync::ExclusiveGuard<storage::BTreeNode> page(buffer_.get(), pid);
    recovery_->PerPageRedo(page, pid);
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
  }
}

TEST_F(TestTreeRecovery, InstantRecovery) {
  FLAGS_wal_enable_recovery  = false;
  FLAGS_wal_instant_recovery = true;

  /* Initialize recovery env */
  log_->WriteMasterRecord();
  spdlog::info("signature {}", leanstore::LeanStore::signature);
  InitializeTree();
  std::vector<std::pair<int, __uint128_t>> data;
  PrepareData<__uint128_t>(data, true);
  auto no_pages = 0UL;
  ConvenientTxnWrapper([&]() { no_pages = tree_->CountPages(); });
  EXPECT_EQ(no_pages, 342);

  /* Make a clone of the current buffer pool */
  auto expected_result = reinterpret_cast<storage::Page *>(AllocHuge(PAGE_SIZE * (no_pages + 1)));
  std::memcpy(expected_result, buffer_->ToPtr(0), PAGE_SIZE * (no_pages + 1));

  /* Reset the state and enable recovery */
  TearDown();
  FLAGS_wal_enable_recovery = true;
  SetUp();
  InitializeTree();

  /* Load written log segments into the recovery manager */
  recovery_->RecoveryPreparation();
  recovery_->MaterializeLogs();
  recovery_->Analysis();
  EXPECT_EQ(recovery_->page_log_[0].size(), no_pages + 1);
  EXPECT_EQ(recovery_->tl_winners_[0].size(), 3);  // TXN 0 and TXN 2
  EXPECT_EQ(recovery_->tl_losers_[0].size(), 0);
  auto max_pid = std::max_element(std::begin(recovery_->page_log_[0]), std::end(recovery_->page_log_[0]),
                                  [](const auto &p1, const auto &p2) { return p1.first < p2.first; });
  EXPECT_EQ((*max_pid).first, 342);

  /* Try to evaluate recovered page data to the current page data */
  EvaluateLogData(no_pages);

  /* Try to redo every page */
  InitRandTransaction();
  recovery_->PrepareRedoEnv();

  /* Instant recovery */
  for (auto &pair : data) {
    std::span key{reinterpret_cast<u8 *>(&pair.first), sizeof(int)};
    std::span payload{reinterpret_cast<u8 *>(&pair.second), sizeof(__uint128_t)};

    auto found = tree_->LookUp(key, [&payload](std::span<const u8> data) {
      EXPECT_EQ(payload.size(), data.size());
      for (size_t idx = 0; idx < data.size(); idx++) { EXPECT_EQ(payload[idx], data[idx]); }
    });
    ASSERT_EQ(found, OpResult::OK);
  }

  /* Evaluate raw page content */
  for (auto pid = 1U; pid <= no_pages; pid++) {
    auto &lhs = *reinterpret_cast<storage::BTreeNode *>(&expected_result[pid]);
    auto &rhs = *reinterpret_cast<storage::BTreeNode *>(buffer_->ToPtr(pid));
    EXPECT_EQ(lhs.ToString(), rhs.ToString());
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
  FLAGS_wal_block_size_mb       = 2;
  FLAGS_wal_batch_write_kb      = 16;
  FLAGS_wal_stealing_group_size = 1;
  FLAGS_txn_commit_group_size   = 1;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
