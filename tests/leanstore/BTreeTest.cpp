#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
#include "/opt/PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include <unordered_set>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
TEST(BTree, VariableSize)
{
   tbb::task_scheduler_init taskScheduler(2);
   LeanStore db;
   auto &btree = db.registerVSBTree("test");
   // -------------------------------------------------------------------------------------
   const u64 n = 1e5;
   const u64 max_key_length = 100;
   const u64 max_payloads_length = 200;
   vector<string> keys;
   vector<string> payloads;
   // -------------------------------------------------------------------------------------
   string result(max_payloads_length, '0');
   u64 result_length;
   // -------------------------------------------------------------------------------------
   for ( u64 i = 0; i < n; i++ ) {
      string i_str = std::to_string(i) + " - ";
      const u64 key_length = i_str.length() + utils::RandomGenerator::getRand<u64>(1, max_key_length);
      keys.push_back(string(key_length, '0'));
      memcpy(keys.back().data(), i_str.data(), i_str.length());
      utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(keys.back().data() + i_str.length()), key_length - i_str.length());
      // -------------------------------------------------------------------------------------
      const u64 payload_length = utils::RandomGenerator::getRand<u64>(1, max_payloads_length);
      payloads.push_back(string(payload_length, '0'));
      utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(payloads.back().data()), payload_length);
   }
   // -------------------------------------------------------------------------------------
   tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64> &range) {
      for ( u64 i = range.begin(); i < range.end(); i++ ) {
         if ( !btree.lookup(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), result_length, reinterpret_cast<u8 *>(result.data()))) {
            btree.insert(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), payloads[i].length(), reinterpret_cast<u8 *>(payloads[i].data()));
         }
      }
   });
   // -------------------------------------------------------------------------------------
   tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64> &range) {
      for ( u64 i = range.begin(); i < range.end(); i++ ) {
         if ( btree.lookup(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), result_length, reinterpret_cast<u8 *>(result.data()))) {
            EXPECT_EQ(result_length, payloads[i].length());
            EXPECT_EQ(std::memcmp(result.data(), payloads[i].data(), result_length), 0);
         }
      }
   });
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
