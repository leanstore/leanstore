#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/ThreadCounters.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
DEFINE_bool(verify, false, "");
DEFINE_bool(load_per_tuples, true, "");
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
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  PerfEvent e;
  e.setParam("threads", FLAGS_worker_threads);
  chrono::high_resolution_clock::time_point begin, end;
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("rio");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  db.startDebuggingThread();
  // -------------------------------------------------------------------------------------
  const u64 target_pages = FLAGS_target_gib * 1024 * 1024 * 1024 / PAGE_SIZE;
  Payload dummy_payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
  u64 max_key = 0;
  // -------------------------------------------------------------------------------------
  if (FLAGS_load_per_tuples) {
    // Insert values
    {
      const u64 n = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
      max_key = n;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      begin = chrono::high_resolution_clock::now();
      {
        tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
          for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
            table.insert(t_i, dummy_payload);
          }
        });
      }
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
    }
  } else {
    u64 largest_keys[FLAGS_worker_threads];
    // -------------------------------------------------------------------------------------
    for (unsigned i = 0; i < FLAGS_worker_threads; i++) {
      threads.emplace_back(
          [&](u64 t_i) {
            u64 key = t_i;
            while (db.getBufferManager().consumedPages() < target_pages) {
              table.insert(key, dummy_payload);
              key += FLAGS_worker_threads;
            }
            largest_keys[t_i] = key;
          },
          i);
    }
    for (auto& thread : threads) {
      thread.join();
    }
    threads.clear();
    max_key = *std::max_element(largest_keys, largest_keys + FLAGS_worker_threads);
    cout << "max key = " << max_key << endl;
  }
  // -------------------------------------------------------------------------------------
  u64 size_at_insert_point = db.getBufferManager().consumedPages();
  const u64 mib = size_at_insert_point * PAGE_SIZE / 1024 / 1024;
  cout << "Inserted volume: (pages, MiB) = (" << size_at_insert_point << ", " << mib << ")" << endl;
  cout << "-------------------------------------------------------------------------------------" << endl;
  // -------------------------------------------------------------------------------------
  for (unsigned t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back(
        [&](const u64 t_i) {
          ThreadCounters::registerThread("worker_" + std::to_string(t_i));
          Payload local_payload;
          while (true) {
            Key rand_k = utils::RandomGenerator::getRandU64(0, max_key);
            if (FLAGS_load_per_tuples) {
              ensure(table.lookup(rand_k, local_payload));
            } else {
              table.lookup(rand_k, local_payload);
            }
            WorkerCounters::myCounters().tx++;
          }
        },
        t_i);
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
