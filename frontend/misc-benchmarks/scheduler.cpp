#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_uint64(connections, 100, "");
DEFINE_double(olap_gib, 1, "");
DEFINE_bool(pg, true, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<128>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   FLAGS_bulk_insert = true;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   unique_ptr<BTreeInterface<Key, Payload>> adapter;
   auto& vs_btree = db.registerBTree("scheduler");
   adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
   auto& table = *adapter;
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   const u64 target_pages = FLAGS_target_gib * 1024 * 1024 * 1024 / PAGE_SIZE;
   Payload dummy_payload;
   utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload));
   // -------------------------------------------------------------------------------------
   vector<thread> threads;
   auto convert_gib_to_n = [](const double gib) { return gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload)); };
   const u64 max_key = convert_gib_to_n(FLAGS_target_gib);
   // -------------------------------------------------------------------------------------
   // Insert values
   {
      tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      {
         tbb::parallel_for(tbb::blocked_range<u64>(0, max_key), [&](const tbb::blocked_range<u64>& range) {
            for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
               table.insert(t_i, dummy_payload);
            }
         });
      }
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   u64 size_at_insert_point = db.getBufferManager().consumedPages();
   const u64 mib = size_at_insert_point * PAGE_SIZE / 1024 / 1024;
   cout << "Inserted volume: (pages, MiB) = (" << size_at_insert_point << ", " << mib << ")" << endl;
   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
   // Connection: either divide the workload into HW threads. or one after another
   atomic<u64> connections_done = 0, threads_counter = 0;
   auto olap = [&](const u64 c_i) {
      const u64 range = convert_gib_to_n(FLAGS_olap_gib);
      if (FLAGS_pg) {
         Payload local_payload;
         for (u64 t_i = 0; t_i < range; t_i++) {
            table.lookup(t_i, local_payload);
         }
         connections_done++;
      } else {
         vector<thread> threads;
         const u64 chunk_size = range / FLAGS_worker_threads;
         for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
            threads.emplace_back(
                [&, chunk_size](const u64 begin) {
                   threads_counter++;
                   Payload local_payload;
                   for (u64 key = begin; key < chunk_size; key++) {
                      table.lookup(key, local_payload);
                   }
                },
                t_i * chunk_size);
         }
         for (auto& thread : threads) {
            thread.join();
         }
         connections_done++;
      }
   };
   chrono::high_resolution_clock::time_point begin, end;
   begin = chrono::high_resolution_clock::now();
   for (unsigned c_i = 0; c_i < FLAGS_connections; c_i++) {
      threads.emplace_back([&, c_i]() {
         threads_counter++;
         olap(c_i);
      });
   }
   for (auto& thread : threads) {
      thread.join();
   }
   ensure(connections_done == FLAGS_connections);
   end = chrono::high_resolution_clock::now();
   cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
   cout << threads_counter << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}
