#include "Exceptions.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <emmintrin.h>
#include <fcntl.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
DEFINE_uint64(samples_count, (1 << 20), "");
DEFINE_bool(mixed, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
using namespace leanstore::buffermanager;
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  ensure(FLAGS_worker_threads > 0);
  vector<thread> threads;
  atomic<bool> keep_running = true;
  // -------------------------------------------------------------------------------------
  const u64 max_size = FLAGS_samples_count;
  u64* sequence = new u64[max_size];
  // -------------------------------------------------------------------------------------
  // LeanStore DB
  using Key = u64;
  using Payload = u64;
  LeanStore db;
  auto& vs_btree = db.registerBTree("fairness");
  BTreeVSAdapter<Key, Payload> table(vs_btree);
  db.startProfilingThread();
  // -------------------------------------------------------------------------------------
  Payload dummy_payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&dummy_payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  struct alignas(64) BF {
    atomic<u64> seq_id = 0;
  };
  BF bf;
  // -------------------------------------------------------------------------------------
  // Insert values
  const u64 n = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
  tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
    for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
      table.insert(t_i, dummy_payload);
    }
  });
  cout << "Insert: done" << endl;
  // -------------------------------------------------------------------------------------
  atomic<u64> threads_counter = 0;
  for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
    threads.emplace_back([&, t_i]() {
      threads_counter++;
      if (FLAGS_pin_threads)
        utils::pinThisThreadRome(FLAGS_pp_threads + t_i);
      while (keep_running) {
        if (FLAGS_mixed && t_i % 4 == 0) {
          table.lookup(t_i, dummy_payload);
        } else {
          table.update(t_i, dummy_payload);
        }
        sequence[bf.seq_id++ % max_size] = t_i;
      }
      threads_counter--;
    });
  }
  sleep(FLAGS_run_for_seconds);
  keep_running = false;
  while (threads_counter) {
  }
  for (auto& thread : threads) {
    thread.join();
  }
  threads.clear();
  std::ofstream csv;
  string filename = FLAGS_csv_path + ".csv";
  std::ofstream::openmode open_flags;
  if (FLAGS_csv_truncate) {
    open_flags = ios::trunc;
  } else {
    open_flags = ios::app;
  }
  csv.open(filename.c_str(), open_flags);
  csv.seekp(0, ios::end);
  csv << std::setprecision(2) << std::fixed;
  if (csv.tellp() == 0) {
    csv << "i,t,c_worker_threads,c_pin_threads,c_smt,c_hash" << endl;
  }
  for (u64 i = 0; i < max_size; i++) {
    csv << i << "," << sequence[i] << "," << FLAGS_worker_threads << "," << FLAGS_pin_threads << "," << FLAGS_smt << "," << db.getConfigHash()
        << endl;
  }
  return 0;
}
