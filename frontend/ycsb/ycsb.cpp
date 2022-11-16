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
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_insert_threads, 0, "");
DEFINE_uint32(ycsb_threads, 0, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
DEFINE_bool(ycsb_warmup, true, "");
DEFINE_uint32(ycsb_sleepy_thread, 0, "");
DEFINE_uint32(ycsb_ops_per_tx, 1, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<8>;
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
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   db.registerConfigEntry("ycsb_threads", FLAGS_ycsb_threads);
   db.registerConfigEntry("ycsb_ops_per_tx", FLAGS_ycsb_ops_per_tx);
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   const TX_MODE tx_type = TX_MODE::OLTP;
   // -------------------------------------------------------------------------------------
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   // Insert values
   const u64 n = ycsb_tuple_count;
   // -------------------------------------------------------------------------------------
   if (FLAGS_tmp4) {
      // -------------------------------------------------------------------------------------
      std::ofstream csv;
      csv.open("zipf.csv", ios::trunc);
      csv.seekp(0, ios::end);
      csv << std::setprecision(2) << std::fixed;
      std::unordered_map<u64, u64> ht;
      auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
      for (u64 t_i = 0; t_i < (FLAGS_tmp4 ? FLAGS_tmp4 : 1e6); t_i++) {
         u64 key = zipf_random->rand();
         if (ht.find(key) == ht.end()) {
            ht[key] = 0;
         } else {
            ht[key]++;
         }
      }
      csv << "key,count" << endl;
      for (auto& [key, value] : ht) {
         csv << key << "," << value << endl;
      }
      cout << ht.size() << endl;
      return 0;
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      // Warmup
      if (FLAGS_ycsb_warmup) {
         cout << "Warmup: Scanning..." << endl;
         {
            begin = chrono::high_resolution_clock::now();
            utils::Parallelize::range(FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
               crm.scheduleJobAsync(t_i, [&, begin, end]() {
                  for (u64 i = begin; i < end; i++) {
                     YCSBPayload result;
                     table.lookup1({static_cast<YCSBKey>(i)}, [&](const KVTable& record) { result = record.my_payload; });
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
      }
   } else {
      cout << "Inserting " << ycsb_tuple_count << " values" << endl;
      begin = chrono::high_resolution_clock::now();
      utils::Parallelize::range(FLAGS_ycsb_insert_threads ? FLAGS_ycsb_insert_threads : FLAGS_worker_threads, n, [&](u64 t_i, u64 begin, u64 end) {
         crm.scheduleJobAsync(t_i, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               YCSBKey key = i;
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
   const u32 exec_threads = FLAGS_ycsb_threads ? FLAGS_ycsb_threads : FLAGS_worker_threads;
   for (u64 t_i = 0; t_i < exec_threads - ((FLAGS_ycsb_sleepy_thread) ? 1 : 0); t_i++) {
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
               cr::Worker::my().startTX(tx_type, isolation_level);
               for (u64 op_i = 0; op_i < FLAGS_ycsb_ops_per_tx; op_i++) {
                  if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                     table.lookup1({key}, [&](const KVTable&) {});  // result = record.my_payload;
                     // leanstore::storage::BMC::global_bf->evictLastPage();  // to ignore the replacement strategy effect on MVCC experiment
                  } else {
                     UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);
                     utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));
                     // -------------------------------------------------------------------------------------
                     table.update1(
                         {key}, [&](KVTable& rec) { rec.my_payload = result; }, tabular_update_descriptor);
                  }
               }
               cr::Worker::my().commitTX();
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
      crm.scheduleJobAsync(exec_threads - 1, [&]() {
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
      }
      crm.joinAll();
   }
   cout << "-------------------------------------------------------------------------------------" << endl;
   return 0;
}
