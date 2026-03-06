#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "transaction/lockable_tuple.h"
#include "transaction/mvcc/version_chain.h"
#include "transaction/mvcc/version_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <array>
#include <atomic>
#include <barrier>
#include <span>
#include <string>
#include <thread>
#include <vector>

using leanstore::LeanStore;
using leanstore::transaction::LockableTuple;
using namespace leanstore::transaction::mvcc;

// Build a mutable byte span from a string literal (no null terminator).
static std::vector<u8> MakePayload(std::string_view s) {
  return std::vector<u8>(reinterpret_cast<const u8 *>(s.data()), reinterpret_cast<const u8 *>(s.data()) + s.size());
}

static std::span<u8> AsSpan(std::vector<u8> &v) { return {v.data(), v.size()}; }

class VersionManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FLAGS_worker_count          = 4;
    LeanStore::worker_thread_id = 0;
    vm_                         = std::make_unique<VersionManager>();
  }

  std::unique_ptr<VersionManager> vm_;
};

// ---------------------------------------------------------------------------
// TC-VM-01  AppendVersion then ReadValidVersion – basic round-trip
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, AppendAndRead_BasicRoundTrip) {
  std::array<u8, 4> k{'k', 'e', 'y', '1'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  auto data = MakePayload("hello");
  vm_->AppendVersion(10, key, AsSpan(data));

  bool called    = false;
  timestamp_t ts = 0;
  bool ok        = vm_->ReadValidVersion(
    10, key,
    [&](std::span<const u8> payload) {
      called = true;
      EXPECT_EQ(std::string(reinterpret_cast<const char *>(payload.data()), payload.size()), "hello");
    },
    ts);

  EXPECT_TRUE(ok);
  EXPECT_TRUE(called);
  EXPECT_EQ(ts, 10);
}

// ---------------------------------------------------------------------------
// TC-VM-02  ReadValidVersion on a key that was never appended must throw
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, Read_MissingKey_Throws) {
  std::array<u8, 4> k{'n', 'o', 'p', 'e'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  timestamp_t ts = 0;
  EXPECT_THROW(vm_->ReadValidVersion(
                 10, key, [](std::span<const u8>) {}, ts),
               std::runtime_error)
    << "ReadValidVersion on unknown key must throw std::runtime_error";
}

// ---------------------------------------------------------------------------
// TC-VM-03  Reader ts < version ts → callback not invoked, returns false
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, Read_TooEarlyTs_ReturnsFalse) {
  std::array<u8, 4> k{'k', 'e', 'y', '2'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  auto data = MakePayload("future");
  vm_->AppendVersion(20, key, AsSpan(data));

  bool called    = false;
  timestamp_t ts = 0;
  bool ok        = vm_->ReadValidVersion(5, key, [&](std::span<const u8>) { called = true; }, ts);

  EXPECT_FALSE(ok) << "Reader at ts=5 must not see version written at ts=20";
  EXPECT_FALSE(called);
}

// ---------------------------------------------------------------------------
// TC-VM-04  Multiple versions on same key – correct snapshot returned
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, MultipleVersions_CorrectSnapshot) {
  std::array<u8, 4> k{'k', 'e', 'y', '3'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  auto d1 = MakePayload("v1");
  auto d2 = MakePayload("v2");
  auto d3 = MakePayload("v3");
  vm_->AppendVersion(10, key, AsSpan(d1));
  vm_->AppendVersion(20, key, AsSpan(d2));
  vm_->AppendVersion(30, key, AsSpan(d3));

  std::string seen;
  timestamp_t ts = 0;
  bool ok        = vm_->ReadValidVersion(
    25, key, [&](std::span<const u8> p) { seen = std::string(reinterpret_cast<const char *>(p.data()), p.size()); },
    ts);

  EXPECT_TRUE(ok);
  EXPECT_EQ(seen, "v2");
  EXPECT_EQ(ts, 20);
}

// ---------------------------------------------------------------------------
// TC-VM-05  AppendVersion is idempotent on GetOrInsert – same key twice
//           creates only one VersionChain, not two
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, AppendVersion_SameKeyTwice_SingleChain) {
  std::array<u8, 4> k{'k', 'e', 'y', '4'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  auto d1 = MakePayload("first");
  auto d2 = MakePayload("second");
  vm_->AppendVersion(10, key, AsSpan(d1));
  vm_->AppendVersion(20, key, AsSpan(d2));

  // Both versions must be accessible via one chain
  std::string seen;
  timestamp_t ts = 0;
  vm_->ReadValidVersion(
    15, key, [&](std::span<const u8> p) { seen = std::string(reinterpret_cast<const char *>(p.data()), p.size()); },
    ts);
  EXPECT_EQ(seen, "first");

  vm_->ReadValidVersion(
    25, key, [&](std::span<const u8> p) { seen = std::string(reinterpret_cast<const char *>(p.data()), p.size()); },
    ts);
  EXPECT_EQ(seen, "second");
}

// ---------------------------------------------------------------------------
// TC-VM-06  Different keys have independent version chains
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, DifferentKeys_IndependentChains) {
  std::array<u8, 4> k1{'a', 'a', 'a', 'a'};
  std::array<u8, 4> k2{'b', 'b', 'b', 'b'};
  LOCKABLE_TUPLE_STACK(key1, k1, 1);
  LOCKABLE_TUPLE_STACK(key2, k2, 1);

  auto d1 = MakePayload("chain_a");
  auto d2 = MakePayload("chain_b");
  vm_->AppendVersion(10, key1, AsSpan(d1));
  vm_->AppendVersion(10, key2, AsSpan(d2));

  std::string s1, s2;
  timestamp_t ts = 0;
  vm_->ReadValidVersion(
    10, key1, [&](std::span<const u8> p) { s1 = std::string(reinterpret_cast<const char *>(p.data()), p.size()); }, ts);
  vm_->ReadValidVersion(
    10, key2, [&](std::span<const u8> p) { s2 = std::string(reinterpret_cast<const char *>(p.data()), p.size()); }, ts);

  EXPECT_EQ(s1, "chain_a");
  EXPECT_EQ(s2, "chain_b");
}

// ---------------------------------------------------------------------------
// TC-VM-07  AdvanceLocalTimestamp is monotonic – assert fires on regression
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, AdvanceLocalTimestamp_MonotonicEnforced) {
  vm_->AdvanceLocalTimestamp(0, 10);
  vm_->AdvanceLocalTimestamp(0, 20);
  vm_->AdvanceLocalTimestamp(0, 20);  // same value — OK (>=)

  // Going backwards must fire the assert in debug builds
  EXPECT_DEBUG_DEATH(vm_->AdvanceLocalTimestamp(0, 5), "") << "Regressing local timestamp must fail an assertion";
}

// ---------------------------------------------------------------------------
// TC-VM-08  Sweep removes old versions; newer versions remain readable
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, Sweep_RemovesOldVersions_NewerStillReadable) {
  std::array<u8, 4> k{'k', 'e', 'y', '5'};
  LOCKABLE_TUPLE_STACK(key, k, 1);

  auto d1 = MakePayload("old");
  auto d2 = MakePayload("new");
  vm_->AppendVersion(10, key, AsSpan(d1));
  vm_->AppendVersion(30, key, AsSpan(d2));

  // Advance all workers' timestamps so min_ts = 25 inside Sweep
  for (wid_t w = 0; w < 4; w++) { vm_->AdvanceLocalTimestamp(w, 25); }

  vm_->Sweep();  // should remove ts=10 (< 25), keep ts=30

  std::string seen;
  timestamp_t ts = 0;
  bool ok        = vm_->ReadValidVersion(
    30, key, [&](std::span<const u8> p) { seen = std::string(reinterpret_cast<const char *>(p.data()), p.size()); },
    ts);

  EXPECT_TRUE(ok);
  EXPECT_EQ(seen, "new");
}

// ---------------------------------------------------------------------------
// TC-VM-09  Concurrent AppendVersion on different keys – no crashes or deadlocks
// ---------------------------------------------------------------------------
TEST_F(VersionManagerTest, ConcurrentAppend_DifferentKeys_NoRace) {
  constexpr size_t THREADS = 4;
  constexpr size_t NO_OPS  = 1000;
  FLAGS_worker_count       = THREADS;

  std::vector<std::array<u8, THREADS>> keys;
  for (int i = 0; i < static_cast<int>(THREADS); i++) { keys.push_back({u8('k'), u8('0' + i), u8('0'), u8('0')}); }

  std::atomic<int> done{0};
  std::barrier sync(THREADS);

  std::vector<std::thread> threads;
  for (size_t tid = 0; tid < THREADS; tid++) {
    threads.emplace_back([&, tid]() {
      LeanStore::worker_thread_id = tid;
      sync.arrive_and_wait();

      LOCKABLE_TUPLE_STACK(key, keys[tid], 1);

      for (auto idx = 0UL; idx < NO_OPS; idx++) {
        auto payload = MakePayload(fmt::format("data_{}_{}", tid, idx));
        vm_->AppendVersion(10 + idx, key, AsSpan(payload));
        done.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto &t : threads) { t.join(); }
  EXPECT_EQ(done.load(), static_cast<int>(THREADS * NO_OPS));
}

// ---------------------------------------------------------------------------
auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_txn_mvcc = true;
  return RUN_ALL_TESTS();
}
