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
   const u64 n = 10e6;
   const u64 max_key_length = 100;
   const u64 max_payloads_length = 200;
   vector<string> keys;
   vector<string> payloads;
   vector<bool> inserted;
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
      if(!btree.lookup(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), result_length, reinterpret_cast<u8 *>(result.data()))){
         btree.insert(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), payloads[i].length(), reinterpret_cast<u8 *>(payloads[i].data()));
         inserted.push_back(true);
      } else {
         inserted.push_back(false);
      }
   }
   // -------------------------------------------------------------------------------------
   for ( u64 i = 0; i < n; i++ ) {
      if(inserted[i]) {
         btree.lookup(reinterpret_cast<u8 *>(keys[i].data()), keys[i].length(), result_length, reinterpret_cast<u8 *>(result.data()));
         EXPECT_EQ(result_length, payloads[i].length());
         EXPECT_EQ(std::memcmp(result.data(), payloads[i].data(), result_length), 0);
      }
   }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
