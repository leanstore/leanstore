#include "leanstore/config.h"
#include "leanstore/env.h"
#include "recovery/log_manager.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "storage/page.h"
#include "transaction/transaction_manager.h"

#include "gtest/gtest.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <new>

namespace fs = std::filesystem;

#define BLOCK_DEVICE "/dev/nvme1n1"
#define CHECK_EXTENT_PAGE_STATE(expected_state, start_pid, pg_cnt)             \
  ({                                                                           \
    EXPECT_EQ(buffer_->GetPageState(start_pid).LockState(), (expected_state)); \
    for (auto pid = (start_pid) + 1; pid < (start_pid) + (pg_cnt); pid++) {    \
      auto &ps = buffer_->GetPageState(pid);                                   \
      EXPECT_EQ(ps.LockState(), sync::PageStateMode::EVICTED);                 \
    }                                                                          \
  })
#define REQUEST_TIER(idx) \
  ({ free_pages_->RequestFreeExtent(storage::ExtentList::ExtentSize(idx), accept_all_lbd_, start_pid, to_free_list); })

namespace leanstore {

class BaseTest : public ::testing::Test {
 protected:
  static constexpr u64 N_PAGES      = 2048;
  static constexpr u64 EXTRA_NO_PG  = 16;
  static constexpr u64 PHYSICAL_CAP = 1024;
  static constexpr u64 EVICT_SIZE   = 8;  // should be <= FLAGS_bm_aio_qd defined in main.cc

  // Env
  int test_file_fd_;
  std::atomic<bool> is_running_;

  // All components of LeanStore
  std::unique_ptr<storage::FreePageManager> free_pages_;
  std::unique_ptr<buffer::BufferManager> buffer_;
  std::unique_ptr<recovery::LogManager> log_;
  std::unique_ptr<transaction::TransactionManager> txn_man_;

  // Misc shared properties
  std::function<bool(pageid_t)> accept_all_lbd_ = []([[maybe_unused]] pageid_t pid) { return true; };

  void SetupTestFile(bool setup_fd = false) {
    // Reset DB file for testing
    FLAGS_db_path = BLOCK_DEVICE;
    if (setup_fd) {
      test_file_fd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
      assert(test_file_fd_ > 0);
    }

    // Reset all run-time here
    worker_thread_id = 0;
    is_running_      = true;
#ifdef ENABLE_TESTING
    transaction::TransactionManager::active_txn.ResetState();
#endif
    free_pages_ = std::make_unique<storage::FreePageManager>();
    buffer_     = std::make_unique<buffer::BufferManager>(N_PAGES, PHYSICAL_CAP, EXTRA_NO_PG, EVICT_SIZE, is_running_,
                                                      free_pages_.get());
    log_        = std::make_unique<recovery::LogManager>(is_running_);
    txn_man_    = std::make_unique<transaction::TransactionManager>(buffer_.get(), log_.get());

    // Allocate metadata page (page 0)
    buffer_->AllocMetadataPage();
  }

  void ModifyPageContent(storage::Page *page) {
    std::memset(reinterpret_cast<u8 *>(page) + sizeof(storage::PageHeader), 111,
                PAGE_SIZE - sizeof(storage::PageHeader));
    page->dirty = true;
  }

  void InitRandTransaction(wid_t w_id = 0) {
    worker_thread_id = w_id;
    txn_man_->StartTransaction(transaction::Transaction::Type::USER, transaction::IsolationLevel::READ_UNCOMMITTED,
                               transaction::Transaction::Mode::OLTP, false);
  }

  void TearDown() override {
    is_running_ = false;
    free_pages_.reset();
    txn_man_.reset();
    log_.reset();
    buffer_.reset();
    test_file_fd_                      = 0;
    storage::BTree::btree_slot_counter = 0;
  }
};

}  // namespace leanstore