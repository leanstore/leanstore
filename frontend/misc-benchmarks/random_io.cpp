#include "Units.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/BTreeAdapter.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/tbb.h>
#include <gflags/gflags.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_double(target_gib, 10, "");
DEFINE_bool(fs, true, "");
DEFINE_bool(verify, false, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<128>;
// -------------------------------------------------------------------------------------
int main(int argc, char **argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   PerfEvent e;
   e.setParam("threads", FLAGS_worker_threads);
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   unique_ptr<BTreeInterface<Key, Payload>> adapter;
   if ( FLAGS_fs ) {
      auto &fs_btree = db.registerFSBTree<Key, Payload>("rio");
      adapter.reset(new BTreeFSAdapter(fs_btree));
   } else {
      auto &vs_btree = db.registerVSBTree("rio");
      adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
   }
   auto &table = *adapter;
   // -------------------------------------------------------------------------------------
   const u64 target_pages = FLAGS_target_gib * 1024 * 1024 * 1024 / PAGE_SIZE;
   Payload dummy_payload;
   utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(&dummy_payload), sizeof(Payload));
   // -------------------------------------------------------------------------------------
   vector<thread> threads;
   u64 largest_keys[FLAGS_worker_threads];
   // -------------------------------------------------------------------------------------
   for ( unsigned i = 0; i < FLAGS_worker_threads; i++ ) {
      threads.emplace_back([&](u64 t_i) {
         u64 key = t_i;
         while ( db.getBufferManager().consumedPages() < target_pages ) {
            table.insert(key, dummy_payload);
            key += FLAGS_worker_threads;
         }
         largest_keys[t_i] = key;
         cout << "t_i = " << t_i << endl;
      }, i);
   }
   for ( auto &thread: threads ) {
      thread.join();
   }
   threads.clear();
   u64 max_key = *std::max_element(largest_keys, largest_keys + FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   cout << "max key = " << max_key << endl;
   // -------------------------------------------------------------------------------------
   for ( unsigned t_i = 0; t_i < FLAGS_worker_threads; t_i++ ) {
      threads.emplace_back([&]() {
         Payload local_payload;
         while ( true ) {
            Key rand_k = utils::RandomGenerator::getRandU64(0, max_key);
            table.lookup(rand_k, local_payload);
         }
      });
   }
   thread stats_thread([&]() {
      while ( true ) {
         sleep(1);
         cout << db.getBufferManager().debugging_counters.read_operations_counter.exchange(0) << endl;
         cout << db.getBufferManager().debugging_counters.cold_hit_counter.exchange(0) << endl;
      }
   });

   for ( auto &thread: threads ) {
      thread.join();
   }
   // -------------------------------------------------------------------------------------
   return 0;
}