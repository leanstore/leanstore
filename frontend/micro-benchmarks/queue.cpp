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
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = u64;
using KVTable = Relation<Key, Payload>;
// -------------------------------------------------------------------------------------
DEFINE_uint64(sleep_for_seconds, 0, "");
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   auto& crm = db.getCRManager();
   LeanStoreAdapter<KVTable> table;
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "queue"); });
   // -------------------------------------------------------------------------------------
   const u64 N = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));
   // Insert values
   cout << "Inserting " << N << " values" << endl;
   utils::Parallelize::range(FLAGS_worker_threads, N, [&](u64 t_i, u64 begin, u64 end) {
      crm.scheduleJobAsync(t_i, [&, begin, end]() {
         jumpmuTry()
         {
            cr::Worker::my().startTX(TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
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
      while (keep_running) {
         jumpmuTry()
         {
            cr::Worker::my().startTX(FLAGS_olap_mode ? leanstore::TX_MODE::OLAP : leanstore::TX_MODE::OLTP,
                                     leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
            sleep(FLAGS_sleep_for_seconds ? FLAGS_sleep_for_seconds : FLAGS_run_for_seconds);
            cr::Worker::my().commitTX();
         }
         jumpmuCatch() { UNREACHABLE(); }
         // cr::Worker::my().shutdown();
         // break;
      }
      running_threads_counter--;
   });
   crm.scheduleJobAsync(1, [&]() {
      running_threads_counter++;
      while (keep_running) {
         jumpmuTry()
         {
            cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
            Key oldest = 0, newest = 0;
            table.scan(
                {0},
                [&](const KVTable::Key& key, const KVTable&) {
                   oldest = key.my_key;
                   return false;
                },
                [&]() {});
            table.scanDesc(
                {std::numeric_limits<Key>::max()},
                [&](const KVTable::Key& key, const KVTable&) {
                   newest = key.my_key;
                   return false;
                },
                [&]() {});
            bool ret = table.erase({oldest});
            ensure(ret);
            table.insert({newest + 1}, {});
            cr::Worker::my().commitTX();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().tx++; }
         }
         jumpmuCatch() { ensure(false); }
      }
      running_threads_counter--;
   });
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) {
         MYPAUSE();
      }
      crm.joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
