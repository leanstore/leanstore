#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/WALMacros.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using Payload = u8[128];
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   db.startProfilingThread();
   auto& crm = db.getCRManager();
   storage::btree::BTree* btree;
   crm.scheduleJobSync(0, [&]() { btree = &db.registerBTree("mvcc"); });
   // -------------------------------------------------------------------------------------
   union {
      u64 x;
      u8 b[8];
   } k;
   k.x = 10;  // TODO: fold
   Payload p;
   struct value_t {
      u64 v1;
   };
   utils::RandomGenerator::getRandString(p, sizeof(Payload));
   // -------------------------------------------------------------------------------------
   using OP_RESULT = leanstore::storage::btree::BTree::OP_RESULT;
   // -------------------------------------------------------------------------------------
   if (FLAGS_tmp == 0) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->removeSI(k.b, sizeof(k.x));
         ensure(ret == OP_RESULT::OK);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      sleep(10);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(ret != OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   } else if (FLAGS_tmp == 10) {
      // transaction abort
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         sleep(3);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      sleep(1);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::ABORT_TX);
         if (ret == OP_RESULT::ABORT_TX) {
            cr::Worker::my().abortTX();
         } else {
            cr::Worker::my().commitTX();
         }
      });
      sleep(5);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         cout << (int)ret << endl;
         ensure(ret == OP_RESULT::DUPLICATE);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.joinAll();
   } else if (FLAGS_tmp == 1) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      sleep(2);
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->updateSI(
             k.b, sizeof(k.b), [&](u8* payload, u16 payload_length) { *reinterpret_cast<u64*>(payload) = 100; }, WALUpdate1(value_t, v1));
         ensure(ret == OP_RESULT::OK);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         sleep(6);
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      crm.joinAll();
   } else {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         auto ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == OP_RESULT::OK);
         ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      sleep(6);
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->removeSI(k.b, sizeof(k.x));
         ensure(ret == OP_RESULT::OK);
         sleep(3);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const auto ret = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(ret == OP_RESULT::OK);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   }
   crm.joinAll();
   // -------------------------------------------------------------------------------------
   return 0;
}
