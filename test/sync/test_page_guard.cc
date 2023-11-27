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
  PageState latch_;
  storage::Page page_;

  static void DumpPageLoader(pageid_t pid) { LOG_DEBUG("Load page %lu", pid); }

  void SetUp() override {
    BaseTest::SetupTestFile();
    memset(reinterpret_cast<u8 *>(&page_), 0, PAGE_SIZE);
    assert(page_.dirty == false);
    // 0 == (version = 0, state = UNLOCKED)
    latch_.StateAndVersion().store(0, std::memory_order_release);
    InitRandTransaction();
  }

  void TearDown() override { BaseTest::TearDown(); }

  auto FixShare() -> storage::Page * {
    latch_.LockShared();
    return &page_;
  }

  auto FixExclusive() -> storage::Page * {
    latch_.LockExclusive();
    return &page_;
  }
};

TEST_F(TestPageGuard, OptimisticValidationNoThrow) {
  // Optimistic Locks + Shared locks work perfectly with each other
  OptimisticGuard<storage::Page> op_guard(0, &page_, &latch_, &DumpPageLoader);
  SharedGuard<storage::Page> s_guard(0, FixShare(), &latch_);
  EXPECT_NO_THROW(op_guard.ValidateOrRestart());
  OptimisticGuard<storage::Page> op_guard2(0, &page_, &latch_, &DumpPageLoader);
  EXPECT_NO_THROW(op_guard2.ValidateOrRestart());
}

TEST_F(TestPageGuard, OptimisticValidationThrow) {
  OptimisticGuard<storage::Page> op_guard(0, &page_, &latch_, &DumpPageLoader);
  { ExclusiveGuard guard(0, FixExclusive(), &latch_); }
  EXPECT_THROW(op_guard.ValidateOrRestart(), RestartException);
}

TEST_F(TestPageGuard, ShareAndExclusiveTogether) {
  SharedGuard<storage::Page> s_guard(0, FixShare(), &latch_);
  auto thread = std::thread([&]() {
    InitRandTransaction();
    ExclusiveGuard guard(0, FixExclusive(), &latch_);
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
        ExclusiveGuard guard(0, FixExclusive(), &latch_);
        counter++;
      });
    } else {
      threads[idx] = std::thread([&]() {
        InitRandTransaction();
        while (true) {
          OptimisticGuard<storage::Page> guard(0, &page_, &latch_, &DumpPageLoader);
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