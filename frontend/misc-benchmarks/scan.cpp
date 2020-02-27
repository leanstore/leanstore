#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
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
  chrono::high_resolution_clock::time_point begin, end;
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  auto& vs_btree = db.registerVSBTree("scan");
  BTreeVSAdapter<Key, Payload> table(vs_btree);
  // -------------------------------------------------------------------------------------
  const u64 target_pages = FLAGS_target_gib * 1024 * 1024 * 1024 / PAGE_SIZE;
  Payload dummy_payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
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
  u64 max_key = *std::max_element(largest_keys, largest_keys + FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  cout << "max key = " << max_key << endl;
  // -------------------------------------------------------------------------------------
  union {
    u64 x;
    u8 key_start[8];
  };
  x = 0ul;
  u64 counter = 0;
  vs_btree.scan(key_start, sizeof(x),
                [&](u8* payload, u16, std::function<string()>&) {
                  ensure(memcmp(payload, reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload)) == 0);
                  counter++;
                  return true;
                }, [](){});
  cout << counter << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
