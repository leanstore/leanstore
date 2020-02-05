#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/storage/btree/vs/BTreeSlotted.hpp"
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
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
using Key = u64;
using Payload = u64;
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
  // -------------------------------------------------------------------------------------
  Payload payload;
  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(Payload));
  // -------------------------------------------------------------------------------------
  const u64 tuple_count = FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));  // 2.0 corresponds to 50% space usage
  // const u64 tuples_in_a_page = EFFECTIVE_PAGE_SIZE * 1.0 / 2.0 / (sizeof(Key) + sizeof(Payload));
  // -------------------------------------------------------------------------------------
  PerfEvent e;
  // Insert values
  {
    const u64 n = tuple_count;
    tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(t_i, payload);
      }
    });
  }
  cout << "Inserted volume: (mib) = (" << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
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
  u64 merges_counter = 0;
  auto compress_bf = [&](Key k) {
    BufferFrame* bf;
    u8 key_bytes[sizeof(Key)];
    vs_btree.lookup(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16) { bf = &db.getBufferManager().getContainingBufferFrame(payload); });
    OptimisticGuard c_guard = OptimisticGuard(bf->header.lock);
    auto parent_handler = vs_btree.findParent(reinterpret_cast<void*>(&vs_btree), *bf);
    merges_counter += vs_btree.checkSpaceUtilization(reinterpret_cast<void*>(&vs_btree), *bf, c_guard, parent_handler);
  };
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
  vector<thread> threads;
  db.startDebuggingThread();
  // -------------------------------------------------------------------------------------
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
  cout << "Inserted volume: (mib) = (" << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
  // -------------------------------------------------------------------------------------
  print_fill_factors(csv, 1);
  // -------------------------------------------------------------------------------------
  cout << merges_counter << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
