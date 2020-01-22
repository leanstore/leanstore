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
DEFINE_uint64(latest_read_ratio, 100, "");
DEFINE_double(latest_window_offset_gib, 0.1, "");
DEFINE_uint64(latest_window_ms, 1000, "");
DEFINE_double(latest_window_gib, 1, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<128>;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  // Check if parameters make sense
  double last_window_offset = FLAGS_latest_window_gib + (FLAGS_run_for_seconds * 1000.0 / FLAGS_latest_window_ms * FLAGS_latest_window_offset_gib);
  ensure(last_window_offset <= FLAGS_target_gib - 1);
  // -------------------------------------------------------------------------------------
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  LeanStore db;
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("latest");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  db.registerConfigEntry("latest_read_ratio", [&](ostream& out) { out << FLAGS_latest_read_ratio; });
  db.registerConfigEntry("latest_window_gib", [&](ostream& out) { out << FLAGS_latest_window_gib; });
  db.registerConfigEntry("latest_window_ms", [&](ostream& out) { out << FLAGS_latest_window_ms; });
  db.registerConfigEntry("latest_window_offset_gib", [&](ostream& out) { out << FLAGS_latest_window_offset_gib; });
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage
  const u64 window_tuple_count =
      FLAGS_latest_window_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage
  // -------------------------------------------------------------------------------------
  u64 size_at_insert_point;
  // Insert values
  {
    const u64 n = tuple_count;
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(t_i, payload);
      }
    });
    size_at_insert_point = db.getBufferManager().consumedPages();
    const u64 mib = size_at_insert_point * PAGE_SIZE / 1024 / 1024;
    cout << "Inserted volume: (pages, MiB) = (" << size_at_insert_point << ", " << mib << ")" << endl;
    cout << "-------------------------------------------------------------------------------------" << endl;
  }
  // -------------------------------------------------------------------------------------
  atomic<u64> window_offset = window_tuple_count;
  auto zipf_random = std::make_unique<utils::ZipfGenerator>(window_tuple_count, FLAGS_zipf_factor);
  // vector<u64> zipf_keys(window_tuple_count);
  // tbb::parallel_for(tbb::blocked_range<u64>(0, window_tuple_count), [&](const tbb::blocked_range<u64>& range) {
  //   for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
  //     zipf_keys[t_i] = zipf_random->rand();
  //   }
  // });
  // -------------------------------------------------------------------------------------
  cout << setprecision(4) << endl;
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  db.startDebuggingThread();
  for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back([&]() {
      running_threads_counter++;
      while (keep_running) {
        Key key = window_offset - zipf_random->rand();
        // Key key = window_offset - utils::RandomGenerator::getRandU64(0, window_tuple_count);
        if ((FLAGS_latest_read_ratio > 0) &&
            (FLAGS_latest_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_latest_read_ratio)) {
          table.lookup(key, payload);
        } else {
          table.update(key, payload);
        }
        WorkerCounters::myCounters().tx++;
      }
      running_threads_counter--;
    });
  }
  const u64 step_size =
      FLAGS_latest_window_offset_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage;
  cout << tuple_count << "\t:\t" << window_tuple_count << "\t:\t" << step_size << endl;
  ensure(step_size < window_tuple_count);
  if (FLAGS_run_for_seconds > FLAGS_latest_window_ms / 1000)
    threads.emplace_back([&]() {
      running_threads_counter++;
      while (keep_running && window_offset + step_size <= tuple_count) {
        usleep(FLAGS_latest_window_ms * 1000);
        window_offset += step_size;
      }
      ensure(keep_running == false);
      running_threads_counter--;
    });
  // -------------------------------------------------------------------------------------
  {
    // Shutdown threads
    sleep(FLAGS_run_for_seconds);
    keep_running = false;
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
