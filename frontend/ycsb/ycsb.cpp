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
// -------------------------------------------------------------------------------------
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_int64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_bool(ycsb_single_statement_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<120>;
using KVTable = Relation<YCSBKey, YCSBPayload>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore::addS64Flag("tuple_count", &FLAGS_ycsb_tuple_count);
   LeanStore db;
   LeanStoreAdapter<KVTable> table;
   // -------------------------------------------------------------------------------------
   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() { table = LeanStoreAdapter<KVTable>(db, "YCSB"); });
   db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
   // -------------------------------------------------------------------------------------
   if(FLAGS_ycsb_tuple_count == 0){
      FLAGS_ycsb_tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.4 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   }
   // Insert values
   if (!FLAGS_recover) {
      cout << "Inserting " << FLAGS_ycsb_tuple_count << " values" << endl;
      begin = chrono::high_resolution_clock::now();
      utils::Parallelize::range(FLAGS_worker_threads, FLAGS_ycsb_tuple_count, [&](u64 t_i, u64 begin, u64 end) {
         crm.scheduleJobAsync(t_i, [&, begin, end]() {
            for (u64 i = begin; i < end; i++) {
               YCSBPayload payload;
               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
               YCSBKey& key = i;
               cr::Worker::my().startTX();
               table.insert({key}, {payload});
               cr::Worker::my().commitTX();
            }
         });
      });
      while(!crm.allJoinable()){
         sleep(1);
         const u64 written_pages = db.getBufferManager().consumedPages();
         const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
         cout << "current inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      }
      crm.joinAll();
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   cout << "building zipf generator" <<endl;
   auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, FLAGS_ycsb_tuple_count, FLAGS_zipf_factor);
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   db.startProfilingThread();
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&]() {
         running_threads_counter++;
         while (keep_running) {
            jumpmuTry()
            {
               YCSBKey key = zipf_random->rand();
               assert(key < (u64) FLAGS_ycsb_tuple_count);
               if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  cr::Worker::my().startTX();
                  table.lookup1({key}, [&](const KVTable&) {});  // result = record.my_payload;
                  cr::Worker::my().commitTX();
               } else {
                  YCSBPayload payload;
                  UpdateDescriptorGenerator1(tabular_update_descriptor, KVTable, my_payload);

                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  // -------------------------------------------------------------------------------------
                  cr::Worker::my().startTX();
                  table.update1(
                      {key}, [&](KVTable& rec) { rec.my_payload = payload; }, tabular_update_descriptor);
                  cr::Worker::my().commitTX();
               }
               WorkerCounters::myCounters().tx++;
               WorkerCounters::myCounters().tx_counter++;
            }
            jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
         }
         running_threads_counter--;
      });
   }
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
