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
DEFINE_uint64(latest_read_ratio, 0, "");
DEFINE_double(latest_window_offset_gib, 0.1, "");
DEFINE_uint64(latest_window_ms, 1000, "");
DEFINE_double(latest_window_gib, 1, "");
DEFINE_bool(force_parallel, false, "");
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
  auto& vs_btree = db.registerBTree("latest");
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
  const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
  const u64 window_tuple_count =
      FLAGS_latest_window_gib * 1024 * 1024 * 1024 * 1.0 / 1.0 / (sizeof(Key) + sizeof(Payload));  // 1.0 corresponds to 100% space usage
  // -------------------------------------------------------------------------------------
  const u64 n = tuple_count;
  // Insert values
  if (!FLAGS_force_parallel) {
    for (u64 t_i = 0; t_i < n; t_i++) {
      table.insert(t_i, payload);
    }
  } else {
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(t_i, payload);
      }
    });
  }
  const u64 pages_at_insertion = db.getBufferManager().consumedPages();
  const u64 mib = pages_at_insertion * PAGE_SIZE / 1024 / 1024;
  cout << "Inserted volume: (pages, MiB) = (" << pages_at_insertion << ", " << mib << ")" << endl;
  cout << "-------------------------------------------------------------------------------------" << endl;
  // -------------------------------------------------------------------------------------
  atomic<u64> window_offset = window_tuple_count;
  const u64 step_size =
      FLAGS_latest_window_offset_gib * 1024 * 1024 * 1024 * 1.0 / 1.0 / (sizeof(Key) + sizeof(Payload));  // 1.0 corresponds to 100% space usage;
  auto zipf_random = std::make_unique<utils::ZipfGenerator>(window_tuple_count, FLAGS_zipf_factor);
  // -------------------------------------------------------------------------------------
  cout << setprecision(4) << endl;
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  // -------------------------------------------------------------------------------------
  for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++)
    threads.emplace_back([&]() {
      running_threads_counter++;
      Payload local_payload;
      const u64 max = window_offset + (FLAGS_run_for_seconds * step_size);
      while (keep_running) {
        Key k = utils::RandomGenerator::getRandU64(0, max);
        table.lookup(k, local_payload);
      }
      running_threads_counter--;
    });
  sleep(FLAGS_warmup_for_seconds);
  keep_running = false;
  while (running_threads_counter) {
    _mm_pause();
  }
  for (auto& thread : threads) {
    thread.join();
  }
  threads.clear();
  keep_running = true;
  // -------------------------------------------------------------------------------------
  const u64 size_after_warmup = db.getBufferManager().consumedPages();
  db.startDebuggingThread();
  for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back([&]() {
      running_threads_counter++;
      Payload local_payload;
      while (keep_running) {
        Key key = window_offset - zipf_random->rand();
        if ((FLAGS_latest_read_ratio > 0) &&
            (FLAGS_latest_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_latest_read_ratio)) {
          table.lookup(key, local_payload);
        } else {
          table.update(key, payload);
        }
        WorkerCounters::myCounters().tx++;
      }
      running_threads_counter--;
    });
  }
  cout << "tuple_count,window_tuple_count,step_size" << endl;
  cout << tuple_count << "," << window_tuple_count << "," << step_size << endl;
  ensure(step_size < window_tuple_count);
  if (FLAGS_run_for_seconds > (FLAGS_latest_window_ms / 1000))
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
    // -------------------------------------------------------------------------------------
    while (running_threads_counter) {
      _mm_pause();
    }
    for (auto& thread : threads) {
      thread.join();
    }
    // -------------------------------------------------------------------------------------
    // for (u64 t_i = 0; t_i < tuple_count; t_i++) {
    //   table.lookup(t_i, payload);
    //   WorkerCounters::myCounters().tx++;
    // }
  }
  // -------------------------------------------------------------------------------------
  const s64 amplification_pages = db.getBufferManager().consumedPages() - size_after_warmup;
  cout << amplification_pages << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
