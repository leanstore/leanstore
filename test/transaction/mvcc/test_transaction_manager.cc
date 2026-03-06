#include "common/typedefs.h"
#include "test/base_test.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <future>

namespace leanstore {

using ::testing::ElementsAreArray;
using transaction::IsolationLevel;

// ---------------------------------------------------------------------------
// Base fixture — mirrors the SVCC fixture but with FLAGS_txn_mvcc = true.
// The flag is set in main() at the bottom of this file.
// ---------------------------------------------------------------------------

class TestMVCCTransactionManager : public BaseTest {
 protected:
  std::unique_ptr<storage::BTree> tree_;

  void SetUp() override {
    BaseTest::SetupTestFile();
    InitRandTransaction();
    tree_ = std::make_unique<storage::BTree>(buffer_.get(), 0);
    catalog.push_back(tree_.get());
    txn_man_->CommitTransaction();
  }

  void TearDown() override {
    tree_.reset();
    BaseTest::TearDown();
  }
};

// ---------------------------------------------------------------------------
// 1. Basic commit / abort — same semantics must hold under MVCC
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, CommitPersistsMultipleInserts) {
  std::vector<std::vector<u8>> keys   = {{1}, {2}, {3}};
  std::vector<std::vector<u8>> values = {{10}, {20}, {30}};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { ASSERT_EQ(tree_->Insert(keys[i], values[i]), OpResult::OK); }
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(tree_->LookUp(keys[i], [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(values[i])); }),
              OpResult::OK);
  }
  ASSERT_EQ(tree_->CountEntries(), 3);
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, AbortUndoMultipleInserts) {
  std::vector<std::vector<u8>> keys   = {{5}, {6}, {7}};
  std::vector<std::vector<u8>> values = {{50}, {60}, {70}};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { ASSERT_EQ(tree_->Insert(keys[i], values[i]), OpResult::OK); }
  txn_man_->AbortTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(tree_->LookUp(keys[i], [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  }
  ASSERT_EQ(tree_->CountEntries(), 0);
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, AbortMultipleUpdates) {
  std::vector<std::vector<u8>> keys    = {{7}, {8}, {9}};
  std::vector<std::vector<u8>> initial = {{1}, {2}, {3}};
  std::vector<std::vector<u8>> updated = {{10}, {20}, {30}};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Insert(keys[i], initial[i]); }
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) { tree_->Update(keys[i], updated[i], {}); }
  txn_man_->AbortTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_EQ(tree_->LookUp(keys[i], [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(initial[i])); }),
              OpResult::OK);
  }
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, AbortMixedOperationsMultipleTuples) {
  std::vector<u8> k1 = {1}, k2 = {2}, k3 = {3};
  std::vector<u8> v1 = {10}, v2 = {20}, v3 = {30};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(k1, v1);
  tree_->Insert(k2, v2);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Update(k1, std::vector<u8>{99}, {});
  tree_->Insert(k3, v3);
  tree_->Remove(k2);
  txn_man_->AbortTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(k1, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v1)); }), OpResult::OK);
  ASSERT_EQ(tree_->LookUp(k2, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v2)); }), OpResult::OK);
  ASSERT_EQ(tree_->LookUp(k3, [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 2. MVCC snapshot isolation — a reader started before a writer commits must
//    still see the old version of the tuple.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, SnapshotReaderSeesOldVersionBeforeCommit) {
  std::vector<u8> key     = {42};
  std::vector<u8> initial = {100};
  std::vector<u8> updated = {200};

  // Pre-insert the tuple in a setup transaction.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Insert(key, initial), OpResult::OK);
  txn_man_->CommitTransaction();

  // Synchronisation barriers between T1 (main thread) and T2 (worker thread).
  std::promise<void> t1_has_read;       // T2 waits until T1 has taken its snapshot read
  std::promise<void> t2_has_committed;  // T1 waits until T2 has committed its update
  auto t1_has_read_fut      = t1_has_read.get_future();
  auto t2_has_committed_fut = t2_has_committed.get_future();

  // T2 runs on a separate thread (active_txn is thread-local, so this is a
  // fully independent transaction context).
  std::thread t2([&]() {
    t1_has_read_fut.wait();  // don't start until T1 has recorded its snapshot read
    LeanStore::worker_thread_id = 1;
    txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
    ASSERT_EQ(tree_->Update(key, updated, {}), OpResult::OK);
    txn_man_->CommitTransaction();
    t2_has_committed.set_value();
  });

  // T1 — main thread.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);

  // T1 reads first; this establishes the snapshot timestamp.
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(initial)); }),
            OpResult::OK);

  // Signal T2 that the snapshot read is done, then wait for T2 to commit.
  t1_has_read.set_value();
  t2_has_committed_fut.wait();

  // T1 must still see the old version despite T2 having committed a newer one.
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(initial)); }),
            OpResult::OK);

  txn_man_->CommitTransaction();
  t2.join();
}

TEST_F(TestMVCCTransactionManager, SnapshotReaderDoesNotSeeAbortedUpdate) {
  std::vector<u8> key     = {7};
  std::vector<u8> initial = {10};
  std::vector<u8> dirty   = {99};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  tree_->Insert(key, initial);
  txn_man_->CommitTransaction();

  // Writer updates but then aborts.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  tree_->Update(key, dirty, {});
  txn_man_->AbortTransaction();

  // Reader must only see the committed initial value — no dirty read.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(initial)); }),
            OpResult::OK);
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, SnapshotReaderDoesNotSeeAbortedInsert) {
  std::vector<u8> key   = {55};
  std::vector<u8> value = {55};

  // Writer inserts and then aborts.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Insert(key, value), OpResult::OK);
  txn_man_->AbortTransaction();

  // The key must be invisible to any subsequent reader.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 3. Version chain correctness — multiple sequential updates; each committed
//    version must be visible to a reader whose snapshot was taken at that
//    point in time.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, VersionChainMultipleUpdatesVisible) {
  std::vector<u8> key = {10};
  std::vector<u8> v0  = {0};
  std::vector<u8> v1  = {1};
  std::vector<u8> v2  = {2};

  // Insert v0.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  tree_->Insert(key, v0);
  txn_man_->CommitTransaction();

  // Update to v1.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  tree_->Update(key, v1, {});
  txn_man_->CommitTransaction();

  // Update to v2.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  tree_->Update(key, v2, {});
  txn_man_->CommitTransaction();

  // A new reader after all commits must see v2.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SNAPSHOT_ISOLATION, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v2)); }), OpResult::OK);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 4. Write–write conflict (same key, two concurrent writers).
//    Under MVCC / OCC the second writer must be rejected (TryLock returns
//    false) because the first writer already holds the exclusive latch.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, WriteWriteConflictDetected) {
  std::vector<u8> key = {20};
  std::vector<u8> v0  = {0};
  std::vector<u8> v1  = {1};
  std::vector<u8> v2  = {2};

  // Pre-insert the tuple.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Insert(key, v0), OpResult::OK);
  txn_man_->CommitTransaction();

  // Barriers:
  //   t1_locked      — T2 doesn't attempt its write until T1 holds the X-lock
  //   t2_done        — T1 doesn't commit until T2 has finished its (failing) attempt
  std::promise<void> t1_locked;
  std::promise<void> t2_done;
  auto t1_locked_fut = t1_locked.get_future();
  auto t2_done_fut   = t2_done.get_future();

  // T2 — separate thread; must see a conflict when it tries to update the same key.
  std::thread t2([&]() {
    t1_locked_fut.wait();  // wait until T1 holds the lock
    LeanStore::worker_thread_id = 1;
    txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
    EXPECT_NE(tree_->Update(key, v2, {}), OpResult::OK);  // must fail — T1 owns the lock
    txn_man_->AbortTransaction();
    t2_done.set_value();
  });

  // T1 — main thread; acquires exclusive lock via Update, then waits for T2 to finish.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Update(key, v1, {}), OpResult::OK);
  t1_locked.set_value();  // T1 now holds the X-lock; let T2 proceed
  t2_done_fut.wait();     // wait until T2 has observed the conflict and aborted
  txn_man_->CommitTransaction();
  t2.join();

  // After T1 commits, v1 must be the visible value.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v1)); }), OpResult::OK);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 5. OCC read-set validation — T1 reads a key, T2 updates + commits the same
//    key, then T1 tries to commit: ValidateReadSet() must return false.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, OCCReadSetValidationFailsOnConflict) {
  std::vector<u8> key     = {30};
  std::vector<u8> initial = {10};
  std::vector<u8> updated = {20};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(key, initial);
  txn_man_->CommitTransaction();

  // Barriers:
  //   t1_has_read    — T2 doesn't start until T1 has read and populated its read set
  //   t2_has_committed — T1 doesn't validate until T2 has committed its update
  std::promise<void> t1_has_read;
  std::promise<void> t2_has_committed;
  auto t1_has_read_fut      = t1_has_read.get_future();
  auto t2_has_committed_fut = t2_has_committed.get_future();

  // T2 — separate thread; updates the key and commits, invalidating T1's read set.
  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;  // distinct worker ID from T1 (which uses 0)
    t1_has_read_fut.wait();
    txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
    ASSERT_EQ(tree_->Update(key, updated, {}), OpResult::OK);
    txn_man_->CommitTransaction();
    t2_has_committed.set_value();
  });

  // T1 — main thread (worker_thread_id == 0 from InitRandTransaction).
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8>) {}), OpResult::OK);
  t1_has_read.set_value();      // read set is populated; let T2 proceed
  t2_has_committed_fut.wait();  // wait until T2's commit has advanced the tuple's timestamp

  // T1's read set is now stale — OCC validation must fail.
  ASSERT_FALSE(txn_man_->ValidateReadSet());
  txn_man_->AbortTransaction();
  t2.join();
}

TEST_F(TestMVCCTransactionManager, OCCReadSetValidationSucceedsWithoutConflict) {
  std::vector<u8> key  = {31};
  std::vector<u8> val  = {11};
  std::vector<u8> key2 = {32};
  std::vector<u8> val2 = {12};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(key, val);
  tree_->Insert(key2, val2);
  txn_man_->CommitTransaction();

  // Barriers:
  //   t1_has_read      — T2 doesn't start until T1 has read key (and populated its read set)
  //   t2_has_committed — T1 doesn't validate until T2 has committed its update to key2
  std::promise<void> t1_has_read;
  std::promise<void> t2_has_committed;
  auto t1_has_read_fut      = t1_has_read.get_future();
  auto t2_has_committed_fut = t2_has_committed.get_future();

  // T2 — separate thread; updates key2 (not key), so T1's read set must remain valid.
  std::thread t2([&]() {
    LeanStore::worker_thread_id = 1;
    t1_has_read_fut.wait();
    txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
    ASSERT_EQ(tree_->Update(key2, std::vector<u8>{99}, {}), OpResult::OK);
    txn_man_->CommitTransaction();
    t2_has_committed.set_value();
  });

  // T1 — main thread; reads key only.
  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8>) {}), OpResult::OK);
  t1_has_read.set_value();      // read set populated; let T2 proceed
  t2_has_committed_fut.wait();  // wait for T2 to commit before validating

  // T2 only touched key2, so T1's read set (key only) must still be valid.
  ASSERT_TRUE(txn_man_->ValidateReadSet());
  txn_man_->CommitTransaction();
  t2.join();
}

// ---------------------------------------------------------------------------
// 6. Idempotent re-lock on the same key within the same transaction.
//    TryLock() must return true on a key already in the write set.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, RepeatedWriteToSameKeySucceeds) {
  std::vector<u8> key = {50};
  std::vector<u8> v0  = {0};
  std::vector<u8> v1  = {1};
  std::vector<u8> v2  = {2};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Insert(key, v0), OpResult::OK);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Update(key, v1, {}), OpResult::OK);
  // Second update on the same key within the same transaction — must not deadlock.
  ASSERT_EQ(tree_->Update(key, v2, {}), OpResult::OK);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v2)); }), OpResult::OK);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 7. Write-then-read within the same transaction must see the transaction's
//    own writes (read-your-writes guarantee).
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, ReadYourOwnWrites) {
  std::vector<u8> key = {60};
  std::vector<u8> v0  = {42};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Insert(key, v0), OpResult::OK);
  // Read back within the same transaction — must see v0.
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(v0)); }), OpResult::OK);
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, ReadYourOwnUpdates) {
  std::vector<u8> key     = {61};
  std::vector<u8> initial = {1};
  std::vector<u8> updated = {2};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(key, initial);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Update(key, updated, {}), OpResult::OK);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(updated)); }),
            OpResult::OK);
  txn_man_->CommitTransaction();
}

// ---------------------------------------------------------------------------
// 8. Delete visibility — after a committed remove, the key must be invisible;
//    after an aborted remove, it must be visible again.
// ---------------------------------------------------------------------------

TEST_F(TestMVCCTransactionManager, CommittedRemoveIsInvisible) {
  std::vector<u8> key = {70};
  std::vector<u8> val = {7};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(key, val);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Remove(key), OpResult::OK);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8>) {}), OpResult::NOT_FOUND);
  txn_man_->CommitTransaction();
}

TEST_F(TestMVCCTransactionManager, AbortedRemoveRestoresTuple) {
  std::vector<u8> key = {71};
  std::vector<u8> val = {71};

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  tree_->Insert(key, val);
  txn_man_->CommitTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->Remove(key), OpResult::OK);
  txn_man_->AbortTransaction();

  txn_man_->StartTransaction(Transaction::Type::USER, 0, IsolationLevel::SERIALIZABLE, Transaction::Mode::OLTP);
  ASSERT_EQ(tree_->LookUp(key, [&](std::span<const u8> p) { EXPECT_THAT(p, ElementsAreArray(val)); }), OpResult::OK);
  txn_man_->CommitTransaction();
}

}  // namespace leanstore

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count        = 4;
  FLAGS_wal_enable          = true;
  FLAGS_txn_mvcc            = true;  // <-- MVCC enabled (contrast with SVCC test)
  FLAGS_wal_force_log_flush = false;
  FLAGS_wal_enable_recovery = false;
  FLAGS_wal_batch_write_kb  = 1024 * 1024;

  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
