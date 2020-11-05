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
   if (FLAGS_tmp == 0) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const bool ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == true);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const bool ret = btree->removeSI(k.b, sizeof(k.x));
         ensure(ret == true);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const bool lookup = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(lookup == true);
         cr::Worker::my().commitTX();
      });
      sleep(10);
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const bool lookup = btree->lookupSI(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(lookup == false);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   } else if (FLAGS_tmp == 1) {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         *reinterpret_cast<u64*>(p) = 200;
         const bool ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == true);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      sleep(2);
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         bool ret = btree->updateSI(
             k.b, sizeof(k.b), [&](u8* payload, u16 payload_length) { *reinterpret_cast<u64*>(payload) = 100; }, WALUpdate1(value_t, v1));
         ensure(ret == true);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const bool lookup =
             btree->lookupSI(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(lookup == true);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         sleep(6);
         cr::Worker::my().startTX();
         const bool lookup =
             btree->lookupSI(k.b, sizeof(k.x), [&](const u8* payload, u16) { cout << *reinterpret_cast<const u64*>(payload) << endl; });
         ensure(lookup == true);
         cr::Worker::my().commitTX();
      });
      crm.joinAll();
   } else {
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const bool ret = btree->insertSI(k.b, sizeof(k.x), sizeof(p), p);
         ensure(ret == true);
         const bool lookup = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(lookup == true);
         sleep(4);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const bool lookup = btree->lookupSI(k.b, sizeof(k.x), [&](const u8*, u16) {});
         ensure(lookup == false);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
      sleep(6);
      crm.scheduleJobAsync(0, [&]() {
         cr::Worker::my().startTX();
         const bool ret = btree->removeSI(k.b, sizeof(k.x));
         ensure(ret == true);
         sleep(3);
         cr::Worker::my().commitTX();
      });
      crm.scheduleJobAsync(1, [&]() {
         cr::Worker::my().startTX();
         const bool lookup = btree->lookupSI(k.b, sizeof(k.x), [&](const u8* value, u16 value_length) {
            ensure(value_length == sizeof(p));
            ensure(std::memcmp(p, value, value_length) == 0);
         });
         ensure(lookup == true);
         cr::Worker::my().commitTX();
      });
      // -------------------------------------------------------------------------------------
   }
   crm.joinAll();
   // -------------------------------------------------------------------------------------
   return 0;
}
