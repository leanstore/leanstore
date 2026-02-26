#include "leanstore/config.h"
#include "leanstore/kv_interface.h"
#include "leanstore/leanstore.h"
#include "recovery/log_manager.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "storage/page.h"
#include "transaction/transaction_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <barrier>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <new>

namespace fs = std::filesystem;

#define CHECK_EXTENT_PAGE_STATE(expected_state, start_pid, pg_cnt)                   \
  ({                                                                                 \
    EXPECT_EQ(this->buffer_->GetPageState(start_pid).LockState(), (expected_state)); \
    for (auto pid = (start_pid) + 1; pid < (start_pid) + (pg_cnt); pid++) {          \
      auto &ps = this->buffer_->GetPageState(pid);                                   \
      EXPECT_EQ(ps.LockState(), sync::PageStateMode::UNLOCKED);                      \
    }                                                                                \
  })

template <typename T>
auto IsInRange(T lo, T hi) {
  return ::testing::AllOf(::testing::Ge((lo)), ::testing::Le((hi)));
}

namespace leanstore {

class BaseTest : public ::testing::Test {
 public:
  static constexpr u64 N_PAGES      = 2048;
  static constexpr u64 EXTRA_NO_PG  = 16;
  static constexpr u64 PHYSICAL_CAP = 1024;
  static constexpr u64 EVICT_SIZE   = 8;
  static constexpr u64 WAL_SIZE     = 1 * GB;

 protected:
  // Env
  int test_file_fd_;
  std::atomic<bool> is_running_;
  fs::path tmp_db_path;

  // All components of LeanStore
  std::unique_ptr<buffer::BufferManager> buffer_;
  std::unique_ptr<recovery::LogManager> log_;
  std::unique_ptr<transaction::TransactionManager> txn_man_;
  std::unique_ptr<recovery::RecoveryManager> recovery_;

  // Misc shared properties
  std::function<bool(pageid_t)> accept_all_lbd_ = []([[maybe_unused]] pageid_t pid) { return true; };

  void SetupTestFile(bool setup_fd = false) {
    fs::path tmp_file = fs::temp_directory_path() / "mockdb.wal";
    tmp_db_path       = tmp_file.string();
    truncate(tmp_db_path.c_str(), N_PAGES * PAGE_SIZE + 1 * GB + WAL_SIZE);  // DB + buffer + WAL
    std::ofstream(tmp_db_path).close();                                      // touch the file

    // Reset DB file for testing
    if (setup_fd) {
      test_file_fd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
      assert(test_file_fd_ > 0);
    }

    // Reset env
    FLAGS_blob_buffer_pool_gb   = 0;
    FLAGS_db_path               = tmp_db_path;
    LeanStore::worker_thread_id = 0;
    is_running_                 = true;
#ifdef ENABLE_TESTING
    transaction::Transaction::active_txn.ResetState();
#endif
    buffer_   = std::make_unique<buffer::BufferManager>(N_PAGES, PHYSICAL_CAP, EXTRA_NO_PG, EVICT_SIZE, is_running_);
    log_      = std::make_unique<recovery::LogManager>(is_running_);
    txn_man_  = std::make_unique<transaction::TransactionManager>(buffer_.get(), log_.get(), is_running_);
    recovery_ = std::make_unique<recovery::RecoveryManager>(buffer_.get());

    // Allocate metadata page (page 0)
    buffer_->ConstructLocalRing();
    buffer_->AllocMetadataPage();

    // Dirty works
    log_->InitializeCommitExecutor(buffer_.get(), is_running_);
  }

  void TearDown() override {
    is_running_ = false;
    txn_man_.reset();
    log_.reset();
    buffer_.reset();
    test_file_fd_ = 0;
    if (!tmp_db_path.empty()) {
      std::filesystem::remove(tmp_db_path);
      tmp_db_path.clear();
    }
    catalog.clear();
  }

  void ModifyPageContent(storage::Page *page) {
    std::memset(reinterpret_cast<u8 *>(page) + sizeof(storage::PageHeader), 111,
                PAGE_SIZE - sizeof(storage::PageHeader));
    page->p_gsn = 999999;
  }

  void InitRandTransaction(wid_t w_id = 0) {
    LeanStore::worker_thread_id = w_id;
    log_->LocalLogWorker().backend.Connect();
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::READ_UNCOMMITTED,
                               transaction::Transaction::Mode::OLTP);
    transaction::Transaction::active_txn.MarkAsWrite();
  }

  void ConvenientTxnWrapper(const std::function<void()> &fn) {
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, 0, transaction::IsolationLevel::READ_UNCOMMITTED,
                               transaction::Transaction::Mode::OLTP);
    fn();
    txn_man_->CommitTransaction();
  }
};

}  // namespace leanstore