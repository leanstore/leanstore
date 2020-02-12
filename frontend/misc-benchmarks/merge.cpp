#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/vs/BTreeSlotted.hpp"
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
DEFINE_bool(random_insert, false, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = BytesPayload<100>;
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
  auto& vs_btree = db.registerVSBTree("merge");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  db.startDebuggingThread();
  // -------------------------------------------------------------------------------------
  u64 merges_counter = 0;
  auto compress_bf = [&](Key k) {
    BufferFrame* bf;
    u8 key_bytes[sizeof(Key)];
    vs_btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16) { bf = &db.getBufferManager().getContainingBufferFrame(payload); });
    OptimisticGuard c_guard = OptimisticGuard(bf->header.lock);
    auto parent_handler = vs_btree.findParent(reinterpret_cast<void*>(&vs_btree), *bf);
    merges_counter += vs_btree.checkSpaceUtilization(reinterpret_cast<void*>(&vs_btree), *bf, c_guard, parent_handler);
  };
  auto print_stats = [&]() {
    cout << "Inner = " << vs_btree.countInner() << endl;
    cout << "Pages = " << vs_btree.countPages() << endl;
    cout << "Inserted volume: (mib) = (" << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
  };
  // -------------------------------------------------------------------------------------
  auto print_fill_factors = [&](std::ofstream& csv, s32 flag) {
    u64 t_i = 0;
    vs_btree.iterateAllPages([&](leanstore::btree::vs::BTreeNode&) { return 0; },
                             [&](leanstore::btree::vs::BTreeNode& leaf) {
                               csv << t_i++ << "," << leaf.fillFactorAfterCompaction() << "," << flag << endl;
                               return 0;
                             });
  };
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  u64 tuple_count;
  tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / (sizeof(Key) + sizeof(Payload));
  // -------------------------------------------------------------------------------------
  chrono::high_resolution_clock::time_point begin, end;
  begin = chrono::high_resolution_clock::now();
  // -------------------------------------------------------------------------------------
  if (FLAGS_random_insert) {
    vector<u64> keys(tuple_count);
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        keys[t_i] = t_i;
      }
    });
    std::random_shuffle(keys.begin(), keys.end());
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(keys[t_i], payload);
        WorkerCounters::myCounters().tx++;
      }
    });
  } else {
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(t_i, payload);
        WorkerCounters::myCounters().tx++;
      }
    });
  }
  end = chrono::high_resolution_clock::now();
  cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
  print_stats();
  // -------------------------------------------------------------------------------------
  std::ofstream csv;
  std::ofstream::openmode open_flags;
  open_flags = ios::trunc;
  csv.open("merge.csv", open_flags);
  csv.seekp(0, ios::end);
  csv << std::setprecision(2) << std::fixed;
  csv << "i,ff,flag" << endl;
  // -------------------------------------------------------------------------------------
  print_fill_factors(csv, 0);
  // -------------------------------------------------------------------------------------
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
  threads.emplace_back([&]() {
    running_threads_counter++;
    while (keep_running) {
      Key k = utils::RandomGenerator::getRandU64(0, tuple_count);
      compress_bf(k);
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
  // -------------------------------------------------------------------------------------
  tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
    for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
      Payload result;
      ensure(table.lookup(t_i, result));
      ensure(result == payload);
    }
  });
  // -------------------------------------------------------------------------------------
  print_fill_factors(csv, 1);
  // -------------------------------------------------------------------------------------
  print_stats();
  cout << merges_counter << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
