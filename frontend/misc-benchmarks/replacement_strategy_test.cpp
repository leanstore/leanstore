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
using namespace leanstore;
// -------------------------------------------------------------------------------------
DEFINE_double(window_gib, 1, "");
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<128>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("Leanstore Frontend");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("rio");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  Payload dummy_payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
  // -------------------------------------------------------------------------------------
  // Insert values
  {
    const u64 n = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
    cout << "-------------------------------------------------------------------------------------" << endl;
    cout << "Inserting values" << endl;
    {
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
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
  const u64 max_key = FLAGS_window_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
  for (unsigned t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back([&](u64 t_i) {
      const u64 r_id = ThreadCounters::registerThread("worker_" + std::to_string(t_i));
      Payload local_payload;
      while (true) {
        Key rand_k = utils::RandomGenerator::getRandU64(0, max_key);
        ensure(table.lookup(rand_k, local_payload));
        WorkerCounters::myCounters().tx++;
      }
      ThreadCounters::removeThread(r_id);
    }, t_i);
  }
  db.startDebuggingThread();
  for (auto& thread : threads) {
    thread.join();
  }
  // -------------------------------------------------------------------------------------
  return 0;
}
