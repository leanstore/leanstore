#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = u64;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   leanstore::LeanStore db;
   auto& crm = db.getCRManager();
   unique_ptr<BTreeInterface<Key, Payload>> adapter;
   auto& vs_btree = db.registerBTree("debug");
   adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
   auto& table = *adapter;
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() {
      for (u64 i = 0; i < 100; i++) {
         cr::Worker::my().startTX();
         u64 x = 100;
         table.insert(i, x);
         cr::Worker::my().commitTX();
      }
   });
   crm.joinAll();
   while (1) {
   }
   return 0;
}
