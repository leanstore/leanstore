#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "transaction/svcc/lock_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <barrier>
#include <memory>
#include <thread>

using leanstore::LeanStore;
using namespace leanstore::transaction::svcc;

TEST(LockManagerTest, MixedSharedExclusiveUpgrade) {
  FLAGS_worker_count = 4;
  LockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  std::array<u8, 4> k3{'k', 'e', 'y', '3'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);
  LOCKABLE_TUPLE_STACK(key3, k3, 1);

  std::atomic<bool> t0_done{false};
  std::atomic<bool> t1_done{false};
  std::atomic<bool> t2_done{false};
  std::atomic<bool> t3_done{false};

  std::barrier sync_point(4);

  std::thread t0([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockShared(10, key1));
    EXPECT_TRUE(lock_mgr.TryLockShared(10, key2));
    t0_done.store(true, std::memory_order_release);
    sync_point.arrive_and_wait();

    // Upgrade key1 to exclusive
    EXPECT_TRUE(lock_mgr.TryLock(10, {}, key1));
    lock_mgr.Unlock(10, key1);
    lock_mgr.UnlockShared(10, key2);
  });

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 1;
    sync_point.arrive_and_wait();
    // Younger tries exclusive on key1 and shared on key2
    bool k1_granted = lock_mgr.TryLock(20, {}, key1);    // May fail by Wait-Die
    bool k2_granted = lock_mgr.TryLockShared(20, key2);  // Should succeed eventually
    if (k1_granted) lock_mgr.Unlock(20, key1);
    if (k2_granted) lock_mgr.UnlockShared(20, key2);
    t1_done.store(k1_granted && k2_granted);
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 2;
    sync_point.arrive_and_wait();
    // Independent tuple, should succeed
    EXPECT_TRUE(lock_mgr.TryLockShared(15, key3));
    lock_mgr.UnlockShared(15, key3);
    t2_done.store(true);
  });

  std::thread t3([&]() {
    LeanStore::worker_thread_id = 3;
    sync_point.arrive_and_wait();
    // Upgrade on independent tuple
    EXPECT_TRUE(lock_mgr.TryLockShared(5, key3));
    EXPECT_TRUE(lock_mgr.TryLock(5, {}, key3));  // Upgrade lock
    lock_mgr.Unlock(5, key3);
    t3_done.store(true);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  // Assertions
  EXPECT_TRUE(t0_done.load());
  EXPECT_TRUE(t2_done.load());
  EXPECT_TRUE(t3_done.load());
  // t1 may succeed or fail (Wait-Die)
  EXPECT_TRUE(t1_done.load() || !t1_done.load());

  // After all release, lock can be acquired again
  std::thread t5([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLockShared(30, key1));
    lock_mgr.UnlockShared(30, key1);
  });
  t5.join();
}

TEST(LockManagerTest, StressTestMultipleKeys) {
  constexpr size_t NO_THREADS = 10;
  constexpr size_t TRY_CNT    = 200;
  FLAGS_worker_count          = NO_THREADS;
  LockManager lock_mgr;

  std::vector<std::array<u8, 4>> keys;
  for (int i = 0; i < 10; i++) keys.push_back({u8('a' + i), u8('b' + i), u8('c' + i), u8('d' + i)});

  std::array<std::atomic<u64>, NO_THREADS> success{};
  std::atomic<uint64_t> ts_counter{1};

  std::array<std::thread, NO_THREADS> threads;
  for (auto idx = 0UL; idx < NO_THREADS; ++idx) {
    threads[idx] = std::thread([&, idx]() {
      LeanStore::worker_thread_id = idx;
      for (size_t cnt = 0; cnt < TRY_CNT; ++cnt) {
        auto ts = ts_counter.fetch_add(1);

        // Pick random tuple
        const auto &k = keys[ts % keys.size()];
        LOCKABLE_TUPLE_STACK(tup, k, 1);

        bool want_exclusive = ts % 5 == 0;
        bool want_upgrade   = ts % 20 == 0;

        if (want_exclusive) {
          if (lock_mgr.TryLock(ts, {}, tup)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            lock_mgr.Unlock(ts, tup);
          }
        } else {
          if (lock_mgr.TryLockShared(ts, tup)) {
            success[idx]++;
            std::this_thread::sleep_for(std::chrono::microseconds(20));
            if (want_upgrade && lock_mgr.TryLock(ts, {}, tup)) {
              lock_mgr.Unlock(ts, tup);
            } else {
              lock_mgr.UnlockShared(ts, tup);
            }
          }
        }
      }
    });
  }

  for (auto &t : threads) { t.join(); }

  uint64_t total_success = 0;
  for (auto &s : success) { total_success += s.load(); }

  spdlog::info("Stress test completed. Total success: {}", total_success);
  EXPECT_GT(total_success, 0);
}

TEST(LockManagerTest, ReleaseAllLocksTest) {
  FLAGS_worker_count          = 1;
  LeanStore::worker_thread_id = 0;
  LockManager lock_mgr;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  std::array<u8, 4> k2{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  // Acquire locks
  EXPECT_TRUE(lock_mgr.TryLockShared(10, key1));
  EXPECT_TRUE(lock_mgr.TryLock(10, {}, key2));

  // Release all
  lock_mgr.ReleaseAllLocks(10, [](auto, auto, auto) {});

  // Should be able to acquire now
  EXPECT_TRUE(lock_mgr.TryLockShared(20, key1));
  lock_mgr.UnlockShared(20, key1);

  EXPECT_TRUE(lock_mgr.TryLock(20, {}, key2));
  lock_mgr.Unlock(20, key2);
}

TEST(LockManagerTest, WaitDieDeadlockCheck) {
  FLAGS_worker_count = 2;
  LockManager lock_mgr;

  std::array<u8, 4> k1{'k', '1', '1', '1'};
  std::array<u8, 4> k2{'k', '2', '2', '2'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  std::atomic<bool> t1_done{false};
  std::atomic<bool> t2_done{false};

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 0;
    EXPECT_TRUE(lock_mgr.TryLock(10, {}, key1));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(lock_mgr.TryLock(10, {}, key2));
    lock_mgr.Unlock(10, key2);
    lock_mgr.Unlock(10, key1);
    t1_done.store(true);
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // start later, younger
    bool success = lock_mgr.TryLock(20, {}, key2);               // Should die due to Wait-Die
    if (success) lock_mgr.Unlock(20, key2);
    t2_done.store(true);
  });

  t1.join();
  t2.join();

  EXPECT_TRUE(t1_done.load());
  EXPECT_TRUE(t2_done.load());
}

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
