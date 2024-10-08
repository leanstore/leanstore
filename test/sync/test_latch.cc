#include "sync/hybrid_latch.h"

#include "gtest/gtest.h"

#include <thread>

static constexpr i32 NO_THREADS = 100;

namespace leanstore::sync {

TEST(TestHybridLatch, SerializeOperation) {
  HybridLatch latch;

  // Exclusive Latch then all subsequent latch attempts should fail
  latch.LockExclusive();
  for (auto idx = 0; idx < 5; idx++) { EXPECT_FALSE(latch.TryLockShared(latch.StateAndVersion())); }
  EXPECT_FALSE(latch.TryLockExclusive(latch.StateAndVersion()));
  latch.UnlockExclusive();

  // Shared Latch, then only 254 later shared latch attemps should success
  EXPECT_TRUE(latch.TryLockShared(latch.StateAndVersion()));
  for (size_t idx = 1; idx < HybridLatch::MAX_SHARED; idx++) {
    EXPECT_TRUE(latch.TryLockShared(latch.StateAndVersion()));
  }
  EXPECT_FALSE(latch.TryLockShared(latch.StateAndVersion()));
  EXPECT_FALSE(latch.TryLockExclusive(latch.StateAndVersion()));
  EXPECT_DEATH(latch.DowngradeLock(), "");
  for (size_t idx = 0; idx < HybridLatch::MAX_SHARED; idx++) { latch.UnlockShared(); }

  // No latch left, all unlock should be death
  EXPECT_DEATH(latch.UnlockShared(), "");
  EXPECT_DEATH(latch.UnlockExclusive(), "");
}

TEST(TestHybridLatch, DowngradeLock) {
  HybridLatch latch;
  latch.LockExclusive();
  EXPECT_FALSE(latch.TryLockShared(latch.StateAndVersion()));
  latch.DowngradeLock();
  for (size_t idx = 1; idx < HybridLatch::MAX_SHARED; idx++) {
    EXPECT_TRUE(latch.TryLockShared(latch.StateAndVersion()));
  }
  EXPECT_FALSE(latch.TryLockShared(latch.StateAndVersion()));
  EXPECT_DEATH(latch.UnlockExclusive(), "");
}

TEST(TestHybridLatch, NormalOperation) {
  int counter = 0;
  HybridLatch latch;

  std::thread threads[NO_THREADS];

  for (int idx = 0; idx < NO_THREADS; idx++) {
    // 50% Write, 50% Read
    if (idx % 2 == 0) {
      threads[idx] = std::thread([&]() {
        latch.LockExclusive();
        counter++;
        latch.UnlockExclusive();
      });
    } else {
      threads[idx] = std::thread([&]() {
        latch.LockShared();
        EXPECT_TRUE((0 <= counter) && (counter <= NO_THREADS / 2));
        latch.UnlockShared();
      });
    }
  }

  for (auto &thread : threads) { thread.join(); }

  EXPECT_EQ(counter, NO_THREADS / 2);
}

}  // namespace leanstore::sync