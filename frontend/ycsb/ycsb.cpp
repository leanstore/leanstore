#include "../shared/LeanStoreAdapter.hpp"
#include "Schema.hpp"
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
#include <tbb/task_scheduler_init.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_bool(ycsb_single_statement_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_uint32(ycsb_sleepy_thread, 0, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<120>;
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
   LeanStore db;
   auto& crm = db.getCRManager();
   LeanStoreAdapter<KVTable> table;
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "YCSB"); });
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   TX_MODE tx_type = FLAGS_ycsb_single_statement_tx ? TX_MODE::SINGLE_STATEMENT : TX_MODE::OLTP;
   // -------------------------------------------------------------------------------------
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   // Insert values
   const u64 n = ycsb_tuple_count;
   if (FLAGS_recover) {
      // Warmup
      cout << "Warmup: Scanning..." << endl;
      {
         begin = chrono::high_resolution_clock::now();
         utils::Parallelize::range(FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
            crm.scheduleJobAsync(t_i, [&, begin, end]() {
               for (u64 i = begin; i < end; i++) {
                  YCSBPayload result;
                  table.lookup1({i}, [&](const KVTable& record) { result = record.my_payload; });
               }
            });
         });
         crm.joinAll();
         end = chrono::high_resolution_clock::now();
      }
      // -------------------------------------------------------------------------------------
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   } else {
      cout << "Inserting " << ycsb_tuple_count << " values" << endl;
      begin = chrono::high_resolution_clock::now();
      utils::Parallelize::range(FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
         crm.scheduleJobAsync(t_i, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               YCSBKey& key = i;
               cr::Worker::my().startTX(tx_type, isolation_level);
               table.insert({key}, {payload});
               cr::Worker::my().commitTX();
            }
         });
      });
      crm.joinAll();
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      // -------------------------------------------------------------------------------------
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   db.startProfilingThread();
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads - ((FLAGS_ycsb_sleepy_thread) ? 1 : 0); t_i++) {
      crm.scheduleJobAsync(t_i, [&]() {
         running_threads_counter++;
         while (keep_running) {
            jumpmuTry()
            {
               YCSBKey key;
               if (FLAGS_zipf_factor == 0) {
                  key = utils::RandomGenerator::getRandU64(0, ycsb_tuple_count);
               } else {
                  key = zipf_random->rand();
               }
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  cr::Worker::my().startTX(tx_type, isolation_level);
                  table.lookup1({key}, [&](const KVTable&) {});  // result = record.my_payload;
                  cr::Worker::my().commitTX();
               } else {
                  UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));
                  // -------------------------------------------------------------------------------------
                  cr::Worker::my().startTX(tx_type, isolation_level);
                  table.update1(
                      {key}, [&](KVTable& rec) { rec.my_payload = result; }, tabular_update_descriptor);
                  cr::Worker::my().commitTX();
               }
               WorkerCounters::myCounters().tx++;
            }
            jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_ycsb_sleepy_thread) {
      const leanstore::TX_MODE tx_type = FLAGS_olap_mode ? leanstore::TX_MODE::OLAP : leanstore::TX_MODE::OLTP;
      crm.scheduleJobAsync(FLAGS_worker_threads - 1, [&]() {
         running_threads_counter++;
         while (keep_running) {
            jumpmuTry()
            {
               cr::Worker::my().startTX(tx_type, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
               sleep(FLAGS_ycsb_sleepy_thread);
               cr::Worker::my().commitTX();
            }
            jumpmuCatch() {}
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
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
