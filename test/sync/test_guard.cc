#include "common/exceptions.h"
#include "sync/hybrid_guard.h"
#include "sync/hybrid_latch.h"

#include "gtest/gtest.h"

#include <chrono>
#include <thread>

namespace leanstore::sync {

static constexpr int NO_THREADS = 500;

TEST(TestGuard, OptimisticValidationNoThrow) {
  HybridLatch latch;

  // Optimistic Locks + Shared locks work perfectly with each other
  HybridGuard op_guard(&latch, GuardMode::OPTIMISTIC);
  HybridGuard s_guard(&latch, GuardMode::SHARED);
  EXPECT_NO_THROW(op_guard.ValidateOptimisticLock());
  HybridGuard op_guard2(&latch, GuardMode::OPTIMISTIC);
  EXPECT_NO_THROW(op_guard2.ValidateOptimisticLock());
}

TEST(TestGuard, OptimisticValidationThrow) {
  HybridLatch latch;
  auto op_guard = std::make_unique<HybridGuard>(&latch, GuardMode::OPTIMISTIC);
  { HybridGuard guard(&latch, GuardMode::EXCLUSIVE); }
  EXPECT_THROW(op_guard->ValidateOptimisticLock(), RestartException);
}

TEST(TestGuard, OptimisticUpgradeSuccess) {
  HybridLatch latch;
  auto op_guard = std::make_unique<HybridGuard>(&latch, GuardMode::OPTIMISTIC);
  EXPECT_NO_THROW(op_guard->UpgradeOptimisticToExclusive());
}

TEST(TestGuard, OptimisticUpgradeFailure) {
  HybridLatch latch;
  auto op_guard = std::make_unique<HybridGuard>(&latch, GuardMode::OPTIMISTIC);
  { HybridGuard guard(&latch, GuardMode::EXCLUSIVE); }
  EXPECT_THROW(op_guard->UpgradeOptimisticToExclusive(), RestartException);
}

TEST(TestGuard, NormalOperation) {
  int counter                             = 0;
  std::atomic<int> optimistic_restart_cnt = 0;
  HybridLatch latch;

  std::thread threads[NO_THREADS];

  for (int idx = 0; idx < NO_THREADS; idx++) {
    // 50% Write, 50% Read
    if (idx % 2 == 0) {
      threads[idx] = std::thread([&]() {
        HybridGuard guard(&latch, GuardMode::EXCLUSIVE);
        counter++;
      });
    } else {
      threads[idx] = std::thread([&]() {
        while (true) {
          auto guard_mode = (rand() % 10 == 0) ? GuardMode::SHARED : GuardMode::OPTIMISTIC;
          HybridGuard guard(&latch, guard_mode);
          EXPECT_TRUE((0 <= counter) && (counter <= NO_THREADS / 2));
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          try {
            // In reality, we don't need to trigger the optimistic check like this
            //   as it is automatically triggered during guard destruction
            guard.ValidateOptimisticLock();
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