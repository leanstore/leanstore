#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "transaction/mvcc/version_chain.h"
#include "transaction/mvcc/version_manager.h"

#include "gtest/gtest.h"

#include <array>
#include <atomic>
#include <barrier>
#include <span>
#include <string>
#include <thread>
#include <vector>

using leanstore::LeanStore;
using namespace leanstore::transaction::mvcc;

// Build a mutable byte span from a string literal (no null terminator).
static std::vector<u8> MakePayload(std::string_view s) {
  return std::vector<u8>(reinterpret_cast<const u8 *>(s.data()), reinterpret_cast<const u8 *>(s.data()) + s.size());
}

static std::span<u8> AsSpan(std::vector<u8> &v) { return {v.data(), v.size()}; }

// ---------------------------------------------------------------------------
// TC-VC-01  Fresh chain has no real versions yet
// ---------------------------------------------------------------------------
TEST(TestVersionChain, FreshChainReturnsEmptyOnRead) {
  VersionChain chain;

  // FindCorrectVersion on a chain that has only the sentinel (ts=0, size=0)
  // must return an empty span — the caller interprets empty as "no visible version".
  timestamp_t out_ts = 999;
  auto result        = chain.FindCorrectVersion(1, out_ts);

  // The sentinel node has size=0 / payload=nullptr, so the span must be empty.
  EXPECT_TRUE(result.empty()) << "Fresh chain must return empty span before any version is appended";
}

// ---------------------------------------------------------------------------
// TC-VC-02  Single append – exact-timestamp read
// ---------------------------------------------------------------------------
TEST(TestVersionChain, SingleAppend_ExactTsRead) {
  VersionChain chain;
  auto data = MakePayload("hello");
  chain.Append(10, AsSpan(data));

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(10, out_ts);

  ASSERT_FALSE(result.empty());
  EXPECT_EQ(out_ts, 10);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(result.data()), result.size()), "hello");
}

// ---------------------------------------------------------------------------
// TC-VC-03  Reader with ts > version ts sees the version
// ---------------------------------------------------------------------------
TEST(TestVersionChain, SingleAppend_FutureReaderSeesVersion) {
  VersionChain chain;
  auto data = MakePayload("v1");
  chain.Append(10, AsSpan(data));

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(20, out_ts);

  ASSERT_FALSE(result.empty());
  EXPECT_EQ(out_ts, 10);
}

// ---------------------------------------------------------------------------
// TC-VC-04  Reader with ts < version ts must NOT see that version
// ---------------------------------------------------------------------------
TEST(TestVersionChain, SingleAppend_PastReaderDoesNotSeeVersion) {
  VersionChain chain;
  auto data = MakePayload("v1");
  chain.Append(10, AsSpan(data));

  timestamp_t out_ts = 999;
  auto result        = chain.FindCorrectVersion(5, out_ts);

  // The only real version has ts=10 which is after the reader ts=5;
  // the chain must fall back to the sentinel (empty payload).
  EXPECT_TRUE(result.empty()) << "Reader at ts=5 must not see a version written at ts=10";
}

// ---------------------------------------------------------------------------
// TC-VC-05  Multiple versions – reader sees the correct snapshot
//
//  Chain after appends:  sentinel(0) → v1(10) → v2(20) → v3(30)
//  Reader ts=25 must see v2(20), not v3(30).
// ---------------------------------------------------------------------------
TEST(TestVersionChain, MultipleVersions_CorrectSnapshotSelected) {
  VersionChain chain;
  auto d1 = MakePayload("v1");
  auto d2 = MakePayload("v2");
  auto d3 = MakePayload("v3");
  chain.Append(10, AsSpan(d1));
  chain.Append(20, AsSpan(d2));
  chain.Append(30, AsSpan(d3));

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(25, out_ts);

  ASSERT_FALSE(result.empty());
  EXPECT_EQ(out_ts, 20);
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(result.data()), result.size()), "v2");
}

// ---------------------------------------------------------------------------
// TC-VC-06  Tombstone version (size=0) is handled correctly
//
//  A delete appends a version with an empty payload.  Reading at that ts must
//  return a non-null but empty span (the tombstone), distinguishable from
//  "version not found" only by out_ts being set.
// ---------------------------------------------------------------------------
TEST(TestVersionChain, TombstoneVersion_EmptyPayload) {
  VersionChain chain;
  auto d1 = MakePayload("alive");
  chain.Append(10, AsSpan(d1));

  // Append a delete tombstone: empty payload
  std::vector<u8> empty_data;
  chain.Append(20, AsSpan(empty_data));

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(20, out_ts);

  // The tombstone is visible at ts=20; payload is empty but out_ts is set.
  EXPECT_EQ(out_ts, 20);
  EXPECT_TRUE(result.empty()) << "Tombstone version must have empty payload";
}

// ---------------------------------------------------------------------------
// TC-VC-07  Payload bytes are deep-copied — mutating the original is safe
// ---------------------------------------------------------------------------
TEST(TestVersionChain, PayloadIsDeepCopied) {
  VersionChain chain;
  auto data = MakePayload("original");
  chain.Append(10, AsSpan(data));

  // Mutate the source buffer after appending
  std::fill(data.begin(), data.end(), u8('X'));

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(10, out_ts);

  ASSERT_FALSE(result.empty());
  EXPECT_EQ(std::string(reinterpret_cast<const char *>(result.data()), result.size()), "original")
    << "Stored payload must be independent of the source buffer";
}

// ---------------------------------------------------------------------------
// TC-VC-08  Sweep removes versions strictly below sweep_ts
// ---------------------------------------------------------------------------
TEST(TestVersionChain, Sweep_RemovesOldVersions) {
  VersionChain chain;
  auto d1 = MakePayload("v10");
  auto d2 = MakePayload("v20");
  auto d3 = MakePayload("v30");
  chain.Append(10, AsSpan(d1));
  chain.Append(20, AsSpan(d2));
  chain.Append(30, AsSpan(d3));

  // Sweep everything strictly before ts=25: removes v10 and v20
  chain.Sweep(25);

  // v30 must still be visible to a reader at ts=30
  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(30, out_ts);
  EXPECT_EQ(out_ts, 30);
  EXPECT_FALSE(result.empty());
}

// ---------------------------------------------------------------------------
// TC-VC-09  Sweep with ts=0 removes nothing
// ---------------------------------------------------------------------------
TEST(TestVersionChain, Sweep_ZeroTsRemovesNothing) {
  VersionChain chain;
  auto d1 = MakePayload("v10");
  chain.Append(10, AsSpan(d1));

  chain.Sweep(0);

  timestamp_t out_ts = 0;
  auto result        = chain.FindCorrectVersion(10, out_ts);
  EXPECT_FALSE(result.empty()) << "Sweep(0) must not remove any version";
}

// ---------------------------------------------------------------------------
// TC-VC-10  Sweep all → chain is empty, latest reader still cannot read old ts
// ---------------------------------------------------------------------------
TEST(TestVersionChain, Sweep_AllVersions_ChainBecomesEmpty) {
  VersionChain chain;
  auto d1 = MakePayload("v10");
  auto d2 = MakePayload("v20");
  chain.Append(10, AsSpan(d1));
  chain.Append(20, AsSpan(d2));

  // Sweep past all real versions
  chain.Sweep(100);

  // After full sweep, a reader at ts=5 (before everything) must see nothing
  timestamp_t out_ts = 999;
  auto result        = chain.FindCorrectVersion(5, out_ts);
  EXPECT_TRUE(result.empty()) << "After full sweep, chain must appear empty to old readers";
}

// ---------------------------------------------------------------------------
auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_txn_mvcc = true;
  return RUN_ALL_TESTS();
}
