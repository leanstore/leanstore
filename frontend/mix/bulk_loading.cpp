#include "../shared/LeanStoreAdapter.hpp"
#include "../shared/Schema.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/parallel_for.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<128>;
using KVTable = Relation<YCSBKey, YCSBPayload>;
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // Always init with the maximum number of threads (FLAGS_worker_threads)
   LeanStore db;
   auto& crm = db.getCRManager();
   LeanStoreAdapter<KVTable> table;
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "YCSB"); });
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   const TX_MODE tx_type = TX_MODE::OLTP;
   // -------------------------------------------------------------------------------------
   const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   cout << "Inserting " << tuple_count << " values" << endl;
   begin = chrono::high_resolution_clock::now();
   crm.scheduleJobAsync(0, [&]() {
      cr::Worker::my().startTX(tx_type, isolation_level);
      YCSBPayload payload;
      utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
      for (u64 i = 0; i < tuple_count; i++) {
         YCSBKey key = i;
         table.insert({key}, {payload});
         WorkerCounters::myCounters().tx++;
      }
      cr::Worker::my().commitTX();
   });
   crm.joinAll();
   end = chrono::high_resolution_clock::now();
   cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
   // -------------------------------------------------------------------------------------
   const u64 written_pages = db.getBufferManager().consumedPages();
   const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
   cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
