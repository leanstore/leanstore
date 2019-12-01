#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "gtest/gtest.h"
#include "gflags/gflags.h"
#include "/opt/PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
TEST(BTree, VariableSize)
{
   LeanStore db;
   auto &btree = db.registerVSBTree("test");
   // -------------------------------------------------------------------------------------
   const u64 n = 10e4;
   const u64 max_key_length = 100;
   const u64 max_payloads_length = 200;
   vector<string> keys;
   vector<string> payloads;
   // -------------------------------------------------------------------------------------
   string result(max_payloads_length, '0');
   u64 result_length;
   // -------------------------------------------------------------------------------------
   for ( u64 i = 0; i < n; i++ ) {
      const u64 key_length = utils::RandomGenerator::getRand<u64>(1, max_key_length);
      keys.push_back(string(key_length, '0'));
      utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(keys.back().data()), key_length);
      // -------------------------------------------------------------------------------------
      const u64 payload_length = utils::RandomGenerator::getRand<u64>(1, max_payloads_length);
      payloads.push_back(string(payload_length, '0'));
      utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(payloads.back().data()), payload_length);
   }
   // -------------------------------------------------------------------------------------
   for ( u64 i = 0; i < n; i++ ) {
      const auto height = btree::vs::height(btree.root);
      btree.insert(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), payloads[i].length(), reinterpret_cast<u8 *>(payloads[i].data()));
      btree.lookup(reinterpret_cast<u8 *>(keys[0].data()), keys[0].length(), result_length, reinterpret_cast<u8 *>(result.data()));
      EXPECT_EQ(result_length, payloads[0].length());
      if ( std::memcmp(result.data(), payloads[0].data(), result_length) != 0 ) {
         raise(SIGTRAP);
      }
   }
   // -------------------------------------------------------------------------------------
   btree.lookup(reinterpret_cast<u8 *>(keys[n - 1].data()), keys[n - 1].length(), result_length, reinterpret_cast<u8 *>(result.data()));
   EXPECT_EQ(result_length, payloads[n - 1].length());
   EXPECT_EQ(std::memcmp(result.data(), payloads[n - 1].data(), result_length), 0);
   for ( u64 i = 0; i < n; i++ ) {
      btree.lookup(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), result_length, reinterpret_cast<u8 *>(result.data()));
      EXPECT_EQ(result_length, payloads[i].length());
      EXPECT_EQ(std::memcmp(result.data(), payloads[i].data(), result_length), 0);
   }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
