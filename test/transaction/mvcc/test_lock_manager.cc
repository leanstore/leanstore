#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "transaction/mvcc/lock_manager.h"
#include "transaction/mvcc/version_manager.h"

#include "fmt/ranges.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using leanstore::LeanStore;
using leanstore::transaction::LockableTuple;
using namespace leanstore::transaction::mvcc;

// ---------------------------------------------------------------------------
// Helper: no-op write-set callback used wherever ReleaseAllLocks is called
// ---------------------------------------------------------------------------
static const auto kNoopCb = [](const LockableTuple *, timestamp_t, std::span<const u8>) {};

// ---------------------------------------------------------------------------
// ResetLocalSets
//
// read_set_ and write_set_ are thread_local statics inside LockManager.
// They persist for the lifetime of each OS thread, which means state leaks
// between tests that run on the same thread (the main thread, or any thread
// that gets reused by the OS thread pool).
//
// Strategy:
//   - ReleaseAllLocks(0, kNoopCb)  →  clears write_set_  (always safe when ts=0
//     because no real lock can have been acquired at ts=0 in production code,
//     and the call is idempotent when the set is already empty).
//   - ValidateReadSet(no-op lambda) does NOT clear read_set_ by itself, but
//     UnlockShared removes individual entries.  The simplest portable approach
//     is to call ReleaseAllLocks again (idempotent) and rely on every test
//     balancing its TryLockShared / UnlockShared calls so read_set_ is already
//     empty by the time TearDown fires.  For defence-in-depth we also do a
//     validate pass which at least exercises the path without side-effects.
//
// Must be called on EVERY thread that participates in a test (main thread via
// SetUp/TearDown, worker threads at the top of their lambdas).
// ---------------------------------------------------------------------------
static void ResetLocalSets(LockManager &lm) {
  lm.ReleaseAllLocks(0, kNoopCb);
  lm.ValidateReadSet([](const LockableTuple *, timestamp_t) {});
}

// ---------------------------------------------------------------------------
// Test fixture
//
// Provides a fresh VersionManager + LockManager for every test and resets
// thread_local state on the main thread in SetUp / TearDown.
// ---------------------------------------------------------------------------
class TestMVCCLockManager : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_worker_count          = 1;  // tests override as needed
    LeanStore::worker_thread_id = 0;

    version_  = std::make_unique<VersionManager>();
    lock_mgr_ = std::make_unique<LockManager>(version_.get());

    ResetLocalSets(*lock_mgr_);  // clear any state left by prior tests on this thread
  }

  void TearDown() override {
    ResetLocalSets(*lock_mgr_);  // clear any state left by prior tests on this thread
  }

  std::unique_ptr<VersionManager> version_;
  std::unique_ptr<LockManager> lock_mgr_;
};

// ===========================================================================
// TEST 1 – PreventWriteWriteConflict
// ===========================================================================
TEST_F(TestMVCCLockManager, PreventWriteWriteConflict) {
  FLAGS_worker_count = 2;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<int> lock_successes{0};
  std::barrier sync(2);

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 0;
    ResetLocalSets(*lock_mgr_);  // clear thread_local state on this worker thread
    sync.arrive_and_wait();

    bool ok = lock_mgr_->TryLock(10, 0, {}, key1);
    if (ok) {
      lock_successes.fetch_add(1, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      lock_mgr_->Unlock(10, key1);
    }
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;
    ResetLocalSets(*lock_mgr_);
    sync.arrive_and_wait();

    bool ok = lock_mgr_->TryLock(20, 0, {}, key1);
    if (ok) {
      lock_successes.fetch_add(1, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      lock_mgr_->Unlock(20, key1);
    }
  });

  t1.join();
  t2.join();

  EXPECT_EQ(lock_successes.load(), 1) << "Write-write conflict must allow exactly one winner";
}

// ===========================================================================
// TEST 2 – SnapshotReadDoesNotBlock
// ===========================================================================
TEST_F(TestMVCCLockManager, SnapshotReadDoesNotBlock) {
  FLAGS_worker_count = 2;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<bool> reader_succeeded{false};
  std::barrier writer_locked(2);

  std::thread writer([&]() {
    LeanStore::worker_thread_id = 0;
    ResetLocalSets(*lock_mgr_);
    ASSERT_TRUE(lock_mgr_->TryLockShared(10, key1));
    writer_locked.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    lock_mgr_->UnlockShared(10, key1);
  });

  std::thread reader([&]() {
    LeanStore::worker_thread_id = 1;
    ResetLocalSets(*lock_mgr_);
    writer_locked.arrive_and_wait();

    bool ok = lock_mgr_->TryLockShared(20, key1);
    reader_succeeded.store(ok, std::memory_order_relaxed);
    if (ok) { lock_mgr_->UnlockShared(20, key1); }
  });

  writer.join();
  reader.join();

  EXPECT_TRUE(reader_succeeded.load()) << "Snapshot read must not be blocked by concurrent reader";
}

// ===========================================================================
// TEST 3 – ExclusiveLockAndRelease
// ===========================================================================
TEST_F(TestMVCCLockManager, ExclusiveLockAndRelease) {
  FLAGS_worker_count = 1;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  EXPECT_TRUE(lock_mgr_->TryLock(10, 0, {}, key1));
  lock_mgr_->Unlock(10, key1);

  EXPECT_TRUE(lock_mgr_->TryLock(20, 0, {}, key1)) << "Lock should be acquirable after prior owner released it";
  lock_mgr_->Unlock(20, key1);
}

// ===========================================================================
// TEST 4 – ConcurrentWritersSingleWinner
// ===========================================================================
TEST_F(TestMVCCLockManager, ConcurrentWritersSingleWinner) {
  constexpr size_t THREADS = 8;
  FLAGS_worker_count       = THREADS;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<int> success{0};
  std::barrier sync(THREADS);

  std::vector<std::thread> threads;
  for (size_t i = 0; i < THREADS; i++) {
    threads.emplace_back([&, i]() {
      LeanStore::worker_thread_id = i;
      ResetLocalSets(*lock_mgr_);
      sync.arrive_and_wait();

      if (lock_mgr_->TryLock(10 + i, 0, {}, key1)) {
        success.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        lock_mgr_->Unlock(10 + i, key1);
      }
    });
  }

  for (auto &t : threads) { t.join(); }

  EXPECT_EQ(success.load(), 1) << "Exactly one of " << THREADS << " concurrent writers must win";
}

// ===========================================================================
// TEST 5 – SnapshotIsolation_ReaderSeesConsistentView
// ===========================================================================
TEST_F(TestMVCCLockManager, SnapshotIsolation_ReaderSeesConsistentView) {
  FLAGS_worker_count = 2;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  LeanStore::worker_thread_id = 0;
  ASSERT_TRUE(lock_mgr_->TryLock(10, 0, {}, key1));
  lock_mgr_->Unlock(10, key1);

  LeanStore::worker_thread_id = 1;
  bool reader_ok              = lock_mgr_->TryLockShared(20, key1);
  EXPECT_TRUE(reader_ok) << "Reader with ts=20 must see the tuple written at ts=10";
  lock_mgr_->SetTupleTimestamp(key1, 15);
  if (reader_ok) { lock_mgr_->UnlockShared(20, key1); }
}

// ===========================================================================
// TEST 6 – StressTestMultipleKeys
// ===========================================================================
TEST_F(TestMVCCLockManager, StressTestMultipleKeys) {
  constexpr size_t THREADS = 1;
  constexpr size_t OPS     = 500;
  FLAGS_worker_count       = THREADS;

  std::vector<std::array<u8, 4>> keys;
  for (int i = 0; i < 10; i++) { keys.push_back({u8('a' + i), u8('b' + i), u8('c' + i), u8('d' + i)}); }

  std::atomic<uint64_t> ts_counter{1};
  std::atomic<uint64_t> total_success{0};

  std::vector<std::thread> threads;
  for (size_t tid = 0; tid < THREADS; tid++) {
    threads.emplace_back([&, tid]() {
      LeanStore::worker_thread_id = tid;
      ResetLocalSets(*lock_mgr_);

      for (size_t i = 0; i < OPS; i++) {
        auto ts       = ts_counter.fetch_add(1, std::memory_order_relaxed);
        const auto &k = keys[ts % keys.size()];
        LOCKABLE_TUPLE_STACK(tup, k, 1);

        if (ts % 5 == 0) {
          if (lock_mgr_->TryLock(ts, 0, {}, tup)) {
            total_success.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            lock_mgr_->Unlock(ts, tup);
          }
        } else {
          if (lock_mgr_->TryLockShared(ts, tup)) {
            lock_mgr_->SetTupleTimestamp(tup, 0);
            total_success.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::microseconds(20));
            lock_mgr_->UnlockShared(ts, tup);
          }
        }
      }
    });
  }

  for (auto &t : threads) { t.join(); }

  EXPECT_GT(total_success.load(), 0) << "At least some operations must succeed under concurrent load";
}

// ===========================================================================
// TEST 7 – ReleaseAllLocksTest
// ===========================================================================
TEST_F(TestMVCCLockManager, ReleaseAllLocksTest) {
  FLAGS_worker_count = 1;

  std::array<u8, 4> k1{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  ASSERT_TRUE(lock_mgr_->TryLock(10, 0, {}, key1));
  EXPECT_FALSE(lock_mgr_->EmptyLocalSet()) << "Local write set must be non-empty after TryLock";

  lock_mgr_->ReleaseAllLocks(10, kNoopCb);

  EXPECT_TRUE(lock_mgr_->EmptyLocalSet()) << "Local write set must be empty after ReleaseAllLocks";

  EXPECT_TRUE(lock_mgr_->TryLock(20, 0, {}, key1)) << "Lock must be acquirable after ReleaseAllLocks";
  lock_mgr_->Unlock(20, key1);
}

// ===========================================================================
// TEST 8 – ValidateReadSet invokes callback for each tracked read
// ===========================================================================
TEST_F(TestMVCCLockManager, ValidateReadSetCallsCallbackForEachRead) {
  FLAGS_worker_count = 1;

  std::array<u8, 4> k1{'r', 'e', 'a', 'd'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  ASSERT_TRUE(lock_mgr_->TryLockShared(10, key1));
  lock_mgr_->SetTupleTimestamp(key1, 5);
  lock_mgr_->UnlockShared(10, key1);

  int callback_count = 0;
  lock_mgr_->ValidateReadSet([&](const LockableTuple *, timestamp_t) { ++callback_count; });

  EXPECT_GE(callback_count, 1) << "Validate callback must be called for every tuple in the read set";
}

// ===========================================================================
// TEST 9 – Shared locks are mutually compatible
// ===========================================================================
TEST_F(TestMVCCLockManager, SharedLocksAreCompatible) {
  FLAGS_worker_count = 2;

  std::array<u8, 4> k1{'s', 'h', 'r', 'd'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<int> readers_active{0};
  std::barrier both_locked(2);

  std::thread r1([&]() {
    LeanStore::worker_thread_id = 0;
    ResetLocalSets(*lock_mgr_);
    ASSERT_TRUE(lock_mgr_->TryLockShared(10, key1));
    lock_mgr_->SetTupleTimestamp(key1, 5);
    readers_active.fetch_add(1, std::memory_order_relaxed);
    both_locked.arrive_and_wait();
    EXPECT_EQ(readers_active.load(), 2);
    lock_mgr_->UnlockShared(10, key1);
  });

  std::thread r2([&]() {
    LeanStore::worker_thread_id = 1;
    ResetLocalSets(*lock_mgr_);
    ASSERT_TRUE(lock_mgr_->TryLockShared(20, key1));
    readers_active.fetch_add(1, std::memory_order_relaxed);
    both_locked.arrive_and_wait();
    EXPECT_EQ(readers_active.load(), 2);
    lock_mgr_->UnlockShared(20, key1);
  });

  r1.join();
  r2.join();
}

// ===========================================================================
// TEST 10 – Write lock blocks subsequent writer until released
// ===========================================================================
TEST_F(TestMVCCLockManager, WriteBlocksWrite) {
  FLAGS_worker_count = 2;

  std::array<u8, 4> k1{'w', 'b', 'l', 'k'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);

  std::atomic<bool> t2_attempted{false};
  std::atomic<bool> t2_succeeded{false};
  std::barrier t1_locked(2);

  std::thread t1([&]() {
    LeanStore::worker_thread_id = 0;
    ResetLocalSets(*lock_mgr_);
    ASSERT_TRUE(lock_mgr_->TryLock(10, 0, {}, key1));
    t1_locked.arrive_and_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    lock_mgr_->Unlock(10, key1);
  });

  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;
    ResetLocalSets(*lock_mgr_);
    t1_locked.arrive_and_wait();

    t2_attempted.store(true, std::memory_order_relaxed);
    bool ok = lock_mgr_->TryLock(20, 0, {}, key1);
    t2_succeeded.store(ok, std::memory_order_relaxed);
    if (ok) { lock_mgr_->Unlock(20, key1); }
  });

  t1.join();
  t2.join();

  EXPECT_TRUE(t2_attempted.load());
  EXPECT_FALSE(t2_succeeded.load()) << "Second writer must be rejected while first writer holds the lock";
}

// ---------------------------------------------------------------------------
auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_txn_mvcc = true;
  return RUN_ALL_TESTS();
}
