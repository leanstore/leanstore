#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/LeanStore.hpp"
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
   auto &crm = db.getCRManager();
   unique_ptr<BTreeInterface<Key, Payload>> adapter;
   auto& vs_btree = db.registerBTree("debug");
   adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
   auto& table = *adapter;
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobAsync(0, [&]() {
      u64 x = 100;
      table.insert(10, x);
   });
   crm.joinAll();
   crm.scheduleJobSync(1, [&]() {
      u64 result;
      table.lookup(10, result);
      cout << result << endl;
   });
   // -------------------------------------------------------------------------------------
   crm.scheduleJobAsync(0, [&]() { cout << "Hello World from WT 0" << endl; });
   crm.scheduleJobAsync(1, [&]() { cout << "Hello World from WT 1" << endl; });
   crm.scheduleJobAsync(2, [&]() { cout << "Hello World from WT 2" << endl; });
   crm.scheduleJobAsync(2, [&]() { cout << "Hello World from WT X" << endl; });
   crm.joinAll();
   return 0;
}
