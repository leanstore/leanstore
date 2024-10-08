#include "leanstore/env.h"
#include "sync/range_lock.h"

#include "gtest/gtest.h"

namespace leanstore::sync {

static constexpr int NO_THREADS = 50;

TEST(TestRangeLock, Simple) {
  worker_thread_id = 0;

  RangeLock lock(1);
  EXPECT_TRUE(lock.TryLockRange(101, 50));
  EXPECT_FALSE(lock.TryLockRange(100, 2));
  EXPECT_TRUE(lock.TryLockRange(100, 1));
  EXPECT_FALSE(lock.TryLockRange(50, 100));
  EXPECT_FALSE(lock.TryLockRange(50, 200));
  EXPECT_FALSE(lock.TryLockRange(150, 50));
  EXPECT_FALSE(lock.TestRange(120, 10));
  lock.UnlockRange(101);
  EXPECT_FALSE(lock.TestRange(100, 1));
  EXPECT_FALSE(lock.TestRange(90, 20));
  EXPECT_TRUE(lock.TestRange(101, 10));
}

TEST(TestRangeLock, Concurrency) {
  RangeLock lock(NO_THREADS);
  std::thread threads[NO_THREADS];

  // NOLINTNEXTLINE
  for (int idx = 0; idx < NO_THREADS; idx++) {
    threads[idx] = std::thread([&, t_id = idx]() {
      worker_thread_id = t_id;

      for (auto i = 0; i < 10000; i++) {
        EXPECT_TRUE(lock.TryLockRange(t_id * 100 + 1, 100));
        EXPECT_FALSE(lock.TestRange(t_id * 100 + 1, 100));
        lock.UnlockRange(t_id * 100 + 1);
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

}  // namespace leanstore::sync