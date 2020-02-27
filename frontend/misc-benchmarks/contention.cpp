#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
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
DEFINE_uint64(cm_threads_pro_page, 4, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<8>;
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
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("contention");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  db.registerConfigEntry("cm_threads_pro_page", [&](ostream& out) {out << FLAGS_cm_threads_pro_page;});
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage
  const u64 tuples_in_a_page = EFFECTIVE_PAGE_SIZE / (sizeof(Key) + sizeof(Payload));
  // -------------------------------------------------------------------------------------
  u64 size_at_insert_point;
  // Insert values
  {
    const u64 n = tuple_count;
    for (u64 t_i = 0; t_i < n; t_i++) {
      table.insert(t_i, payload);
    }
    size_at_insert_point = db.getBufferManager().consumedPages();
    const u64 mib = size_at_insert_point * PAGE_SIZE / 1024 / 1024;
    cout << "Inserted volume: (pages, MiB) = (" << size_at_insert_point << ", " << mib << ")" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  cout << setprecision(4) << endl;
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  db.startDebuggingThread();
  // -------------------------------------------------------------------------------------
  auto spawn_update_thread = [&](u64 k) {
    threads.emplace_back(
        [&](u64 key) {
          running_threads_counter++;
          while (keep_running) {
            table.update(key, payload);
            WorkerCounters::myCounters().tx++;
          }
          running_threads_counter--;
        },
        k);
  };
  for (u64 t_i = 0; t_i < FLAGS_worker_threads / FLAGS_cm_threads_pro_page; t_i++) {
    const u64 offset = tuples_in_a_page * t_i;
    for (u64 i = 0; i < FLAGS_cm_threads_pro_page; i++)
      spawn_update_thread(offset + i);
  }
  // -------------------------------------------------------------------------------------
  ensure(threads.size() <= FLAGS_worker_threads);
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
  // -------------------------------------------------------------------------------------
  const s64 amplification_pages = db.getBufferManager().consumedPages() - size_at_insert_point;
  cout << amplification_pages << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
