#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "gtest/gtest.h"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>

#include <unordered_set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(btree_n, 1e5, "");
DEFINE_uint32(btree_t, 2, "");
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
TEST(BTree, VS)
{
  tbb::task_scheduler_init taskScheduler(FLAGS_btree_t);
  LeanStore db;
  auto& btree = db.registerVSBTree("test");
  // -------------------------------------------------------------------------------------
  const u64 n = FLAGS_btree_n;
  const u64 max_key_length = 100;
  const u64 max_payloads_length = 200;
  vector<string> keys;
  vector<string> payloads;
  PerfEvent e;
  // -------------------------------------------------------------------------------------
  for (u64 i = 0; i < n; i++) {
    string i_str = std::to_string(i) + "-";
    const u64 key_length = i_str.length() + utils::RandomGenerator::getRand<u64>(1, max_key_length);
    keys.push_back(string(key_length, '0'));
    memcpy(keys.back().data(), i_str.data(), i_str.length());
    utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(keys.back().data() + i_str.length()), key_length - i_str.length());
    // -------------------------------------------------------------------------------------
    const u64 payload_length = utils::RandomGenerator::getRand<u64>(1, max_payloads_length);
    payloads.push_back(string(payload_length, '0'));
    utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(payloads.back().data()), payload_length);
  }
  // -------------------------------------------------------------------------------------
  atomic<u64> inserted_counter = 0;
  {
    e.setParam("op", "insert");
    PerfEventBlock b(e, n);
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      string result(max_payloads_length, '0');
      for (u64 i = range.begin(); i < range.end(); i++) {
        if (!btree.lookupOne(reinterpret_cast<u8*>(keys[i].data()), keys[i].length(), [](const u8*, u16) {})) {
          btree.insert(reinterpret_cast<u8*>(keys[i].data()), keys[i].length(), payloads[i].length(), reinterpret_cast<u8*>(payloads[i].data()));
          inserted_counter++;
        }
      }
    });
  }
  {
    e.setParam("op", "lookup");
    PerfEventBlock b(e, n);
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 i = range.begin(); i < range.end(); i++) {
        btree.lookupOne(reinterpret_cast<u8*>(keys[i].data()), keys[i].length(), [&](const u8* result, u16 result_length) {
          EXPECT_EQ(result_length, payloads[i].length());
          EXPECT_EQ(std::memcmp(result, payloads[i].data(), result_length), 0);
        });
      }
    });
  }
  {
    e.setParam("op", "scan");
    PerfEventBlock b(e, n);
    string result(max_payloads_length, '0');
    string key("0");
    u64 counter = 0;
    btree.rangeScan(
        reinterpret_cast<u8*>(key.data()), 1,
        [&](u8*, u16, std::function<string()>& getKey) {
          counter++;
          auto c_key = getKey();
          return true;
        },
        []() {});
    EXPECT_EQ(counter, inserted_counter);
  }
  btree.printInfos(100);
  {
    e.setParam("op", "delete");
    PerfEventBlock b(e, n);
    tbb::parallel_for(tbb::blocked_range<s64>(0, n), [&](const tbb::blocked_range<s64>& range) {
      for (s64 i = range.begin(); i < range.end(); i++) {
        btree.remove(reinterpret_cast<u8*>(keys[i].data()), keys[i].length());
      }
    });
  }
  EXPECT_EQ(btree.countEntries(), 0);
  btree.printInfos(100);
  {
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      string result(max_payloads_length, '0');
      for (u64 i = range.begin(); i < range.end(); i++) {
        EXPECT_FALSE(btree.lookup(reinterpret_cast<u8*>(keys[i].data()), keys[i].length(), [](const u8*, u16) {}));
      }
    });
  }
  // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
