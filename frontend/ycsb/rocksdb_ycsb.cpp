#include <gflags/gflags.h>

#include "../shared/RocksDBAdapter.hpp"
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
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_bool(ycsb_single_statement_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_header, true, "");
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<64>;
using YCSBTable = Relation<YCSBKey, YCSBPayload>;
// -------------------------------------------------------------------------------------
thread_local rocksdb::Transaction* RocksDB::txn = nullptr;
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("WiredTiger TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   RocksDB rocks_db;
   rocks_db.prepareThread();
   RocksDBAdapter<YCSBTable> table(rocks_db);
   // -------------------------------------------------------------------------------------
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   cout << "Inserting " << ycsb_tuple_count << " values" << endl;
   begin = chrono::high_resolution_clock::now();
   leanstore::utils::Parallelize::range(FLAGS_worker_threads, ycsb_tuple_count, [&](u64 t_i, u64 begin, u64 end) {
      for (u64 i = begin; i < end; i++) {
         YCSBPayload payload;
         leanstore::utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
         YCSBKey& key = i;
         table.insert({key}, {payload});
      }
   });
   end = chrono::high_resolution_clock::now();
   cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
   cout << calculateMTPS(begin, end, ycsb_tuple_count) << " M tps" << endl;
   // -------------------------------------------------------------------------------------
   std::vector<thread> threads;
   auto zipf_random = std::make_unique<leanstore::utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   cout << "~Transactions" << endl;
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i] {
         running_threads_counter++;
         while (keep_running) {
            jumpmuTry()
            {
               YCSBKey key;
               if (FLAGS_zipf_factor == 0) {
                  key = leanstore::utils::RandomGenerator::getRandU64(0, ycsb_tuple_count);
               } else {
                  key = zipf_random->rand();
               }
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               if (FLAGS_ycsb_read_ratio == 100 || leanstore::utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  table.lookup1({key}, [&](const YCSBTable&) {});  // result = record.my_payload;
               } else {
                  UpdateDescriptorGenerator1(tabular_update_descriptor, YCSBTable, my_payload);
                  leanstore::utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&result), sizeof(YCSBPayload));
                  table.update1(
                      {key}, [&](YCSBTable& rec) { rec.my_payload = result; }, tabular_update_descriptor);
               }
               thread_committed[t_i]++;
            }
            jumpmuCatch() { thread_aborted[t_i]++; }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   threads.emplace_back([&]() {
      running_threads_counter++;
      if (FLAGS_print_header) {
         cout << "t,tag,oltp_committed,oltp_aborted" << endl;
      }
      u64 time = 0;
      while (keep_running) {
         u64 total_committed = 0, total_aborted = 0;
         for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
            total_committed += thread_committed[t_i].exchange(0);
            total_aborted += thread_aborted[t_i].exchange(0);
         }
         cout << time++ << "," << FLAGS_tag << "," << total_committed << "," << total_aborted << endl;
         sleep(1);
      }
      running_threads_counter--;
   });
   // Shutdown threads
   sleep(FLAGS_run_for_seconds);
   keep_running = false;
   while (running_threads_counter) {
   }
   for (auto& thread : threads) {
      thread.join();
   }
   return 0;
}
