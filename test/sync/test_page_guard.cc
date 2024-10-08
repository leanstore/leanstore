#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/page_guard.h"
#include "sync/page_guard/shared_guard.h"
#include "test/base_test.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include <thread>

namespace leanstore::sync {

static constexpr i32 NO_THREADS = 200;

class TestPageGuard : public BaseTest {
 protected:
  void SetUp() override {
    BaseTest::SetupTestFile();
    memset(reinterpret_cast<u8 *>(buffer_->ToPtr(0)), 0, PAGE_SIZE);
    assert(buffer_->ToPtr(0)->p_gsn == 0);
    assert(buffer_->GetPageState(0).LockState() == sync::PageState::UNLOCKED);
    InitRandTransaction();
  }

  void TearDown() override { BaseTest::TearDown(); }
};

TEST_F(TestPageGuard, OptimisticValidationNoThrow) {
  // Optimistic Locks + Shared locks work perfectly with each other
  OptimisticGuard<storage::Page> op_guard(buffer_.get(), 0);
  SharedGuard<storage::Page> s_guard(buffer_.get(), 0);
  EXPECT_NO_THROW(op_guard.ValidateOrRestart());
  OptimisticGuard<storage::Page> op_guard2(buffer_.get(), 0);
  EXPECT_NO_THROW(op_guard2.ValidateOrRestart());
}

TEST_F(TestPageGuard, OptimisticValidationThrow) {
  OptimisticGuard<storage::Page> op_guard(buffer_.get(), 0);
  { ExclusiveGuard<storage::Page> guard(buffer_.get(), buffer_->FixExclusive(0)); }
  EXPECT_THROW(op_guard.ValidateOrRestart(), RestartException);
}

TEST_F(TestPageGuard, ShareAndExclusiveTogether) {
  SharedGuard<storage::Page> s_guard(buffer_.get(), 0);
  auto thread = std::thread([&]() {
    InitRandTransaction();
    ExclusiveGuard<storage::Page> guard(buffer_.get(), buffer_->FixExclusive(0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  });
  EXPECT_NO_THROW(s_guard.Unlock());
  thread.join();
}

TEST_F(TestPageGuard, NormalOperation) {
  int counter                             = 0;
  std::atomic<int> optimistic_restart_cnt = 0;
  std::thread threads[NO_THREADS];

  for (int idx = 0; idx < NO_THREADS; idx++) {
    // 50% Write, 50% Read
    if (idx % 2 == 0) {
      threads[idx] = std::thread([&]() {
        InitRandTransaction();
        ExclusiveGuard<storage::Page> guard(buffer_.get(), buffer_->FixExclusive(0));
        counter++;
      });
    } else {
      threads[idx] = std::thread([&]() {
        InitRandTransaction();
        while (true) {
          OptimisticGuard<storage::Page> guard(buffer_.get(), 0);
          EXPECT_TRUE((0 <= counter) && (counter <= NO_THREADS / 2));
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          try {
            // In reality, we don't need to trigger the optimistic check like this
            //   as it is automatically triggered during guard destruction
            guard.ValidateOrRestart();
            break;
          } catch (const RestartException &) { optimistic_restart_cnt++; }
        }
      });
    }
  }

  for (auto &thread : threads) { thread.join(); }

  EXPECT_EQ(counter, NO_THREADS / 2);
  EXPECT_GT(optimistic_restart_cnt, 0);
}

}  // namespace leanstore::sync

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_bm_aio_qd    = 8;
  FLAGS_worker_count = leanstore::sync::NO_THREADS + 12;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}