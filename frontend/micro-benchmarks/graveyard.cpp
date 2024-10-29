#include "../shared/GenericSchema.hpp"
#include "../shared/LeanStoreAdapter.hpp"
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
using namespace leanstore;
// -------------------------------------------------------------------------------------
using GYKey = u64;
using GYPayload = u64;
using KVTable = Relation<GYKey, GYPayload>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   auto& crm = db.getCRManager();
   LeanStoreAdapter<KVTable> table;
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "GY"); });
   // -------------------------------------------------------------------------------------
   const u64 N = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(GYKey) + sizeof(GYPayload));
   // Insert values
   cout << "Inserting " << N << " values" << endl;
   utils::Parallelize::range(FLAGS_worker_threads, N, [&](u64 t_i, u64 begin, u64 end) {
      crm.scheduleJobAsync(t_i, [&, begin, end]() {
         jumpmuTry()
         {
            cr::Worker::my().startTX(TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::READ_COMMITTED);
            for (u64 i = begin; i < end; i++) {
               table.insert({i}, {i});
            }
            cr::Worker::my().commitTX();
         }
         jumpmuCatch() { ensure(false); }
      });
   });
   crm.joinAll();
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   db.startProfilingThread();
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   crm.scheduleJobAsync(0, [&]() {
      running_threads_counter++;
      jumpmuTry()
      {
         cr::Worker::my().startTX(FLAGS_olap_mode ? leanstore::TX_MODE::OLAP : leanstore::TX_MODE::OLTP,
                                  leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
         while (keep_running) {
            u64 counter = 0;
            table.scan(
                {0},
                [&](const KVTable::Key& key, const KVTable& value) {
                   counter++;
                   return true;
                },
                [&]() {});
            ensure(counter == N);
            WorkerCounters::myCounters().tx++;
         }
         cr::Worker::my().commitTX();
      }
      jumpmuCatch() { UNREACHABLE(); }
      running_threads_counter--;
   });
   sleep(1);
   crm.scheduleJobAsync(1, [&]() {
      running_threads_counter++;
      jumpmuTry()
      {
         u64 deleted = 0;
         cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
         for (u64 i = 0; i < N; i++) {
            if (i % 2 == 0) {
               table.erase({i});
               deleted++;
            }
         }
         cr::Worker::my().commitTX();
         cr::Worker::my().startTX(leanstore::TX_MODE::OLAP, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
         u64 found = 0;
         table.scan(
             {0},
             [&](const KVTable::Key&, const KVTable&) {
                found++;
                return true;
             },
             [&]() {});
         ensure(found == N - deleted);
         cr::Worker::my().commitTX();
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
      running_threads_counter--;
   });
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
      }
      crm.joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
