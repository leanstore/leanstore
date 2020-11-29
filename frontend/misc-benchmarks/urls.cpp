#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/BTreeSlotted.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_string(in, "", "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // Check if parameters make sense
   // -------------------------------------------------------------------------------------
   tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   auto& vs_btree = db.registerBTree("urls");
   // -------------------------------------------------------------------------------------
   utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
   const u64 tuple_count = input_strings.size();
   tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
         vs_btree.insertLL(reinterpret_cast<u8*>(const_cast<char*>(input_strings[t_i].data())), input_strings[t_i].size(), 8,
                         reinterpret_cast<u8*>(&t_i));
      }
   });
   cout << "Inserted volume: (mib) = (" << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
   // -------------------------------------------------------------------------------------
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
      threads.emplace_back([&]() {
         running_threads_counter++;
         while (keep_running) {
            u64 k = utils::RandomGenerator::getRandU64(0, tuple_count);
            bool flag = true;
            vs_btree.lookupOneLL(reinterpret_cast<u8*>(const_cast<char*>(input_strings[k].data())), input_strings[k].size(),
                               [&](const u8* payload, u16 payload_length) {
                                  flag &= (payload_length == 8);
                                  flag &= (*reinterpret_cast<const u64*>(payload) == k);
                               });
            ensure(flag);
            WorkerCounters::myCounters().tx++;
         }
         running_threads_counter--;
      });
   // -------------------------------------------------------------------------------------
   {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      // -------------------------------------------------------------------------------------
      while (running_threads_counter) {
         _mm_pause();
      }
      for (auto& thread : threads) {
         thread.join();
      }
   }
   cout << "Inserted volume: (mib) = (" << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
   // -------------------------------------------------------------------------------------
   return 0;
}
