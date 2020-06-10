#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/counters/ThreadCounters.hpp"
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
DEFINE_bool(verify, false, "");
DEFINE_bool(print_fill_factors, false, "");  // 1582587
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
  // LeanStore DB
  LeanStore db;
  unique_ptr<BTreeInterface<Key, Payload>> adapter;
  auto& vs_btree = db.registerVSBTree("merge");
  adapter.reset(new BTreeVSAdapter<Key, Payload>(vs_btree));
  auto& table = *adapter;
  // -------------------------------------------------------------------------------------
  std::ofstream csv;
  std::ofstream::openmode open_flags;
  if (FLAGS_csv_truncate) {
    open_flags = ios::trunc;
  } else {
    open_flags = ios::app;
  }
  csv.open(FLAGS_csv_path + "_merge.csv", open_flags);
  csv.seekp(0, ios::end);
  csv << std::setprecision(2) << std::fixed;
  if (FLAGS_csv_truncate) {
    csv << "i,ff,flag,bstar,su_merge,tag" << endl;
  }
  // -------------------------------------------------------------------------------------
  auto compress_bf = [&](u8* key_bytes, u16 key_length) {
    static u64 sleep_counter = 0;
    static u64 sleep_ms = 100;
    static u64 succ_counter = 0;
    // -------------------------------------------------------------------------------------
    BufferFrame* bf;
    ensure(vs_btree.lookupOne(key_bytes, key_length, [&](const u8* payload, u16) { bf = &db.getBufferManager().getContainingBufferFrame(payload); }));
    OptimisticGuard o_guard = OptimisticGuard(bf->header.latch);
    auto parent_handler = vs_btree.findParent(reinterpret_cast<void*>(&vs_btree), *bf);
    // -------------------------------------------------------------------------------------
    auto p_guard = parent_handler.getParentReadPageGuard<leanstore::btree::vs::BTreeNode>();
    auto c_guard = OptimisticPageGuard<leanstore::btree::vs::BTreeNode>::manuallyAssembleGuard(std::move(o_guard), bf);
    auto ret_code = vs_btree.kWayMerge(p_guard, c_guard, parent_handler);
    WorkerCounters::myCounters().dt_researchy[0][1]++;
    if (ret_code == leanstore::btree::vs::BTree::KWayMergeReturnCode::FULL_MERGE) {
      succ_counter++;
    }
    sleep_counter++;
    if (sleep_counter == 10) {
      sleep_counter = 0;
      auto l_succ_counter = succ_counter;
      succ_counter = 0;
      if (l_succ_counter < FLAGS_y) {
        usleep(sleep_ms);
        sleep_ms++;
      } else {
        sleep_ms = 100;
      }
    }
    p_guard.kill();
    c_guard.kill();
  };
  auto print_stats = [&]() {
    cout << "Inner = " << vs_btree.countInner() << endl;
    cout << "Pages = " << vs_btree.countPages() << endl;
    cout << "Inserted volume: (pages, mib) = (" << db.getBufferManager().consumedPages() << ","
         << db.getBufferManager().consumedPages() * 1.0 * PAGE_SIZE / 1024 / 1024 << ")" << endl;
  };
  // -------------------------------------------------------------------------------------
  auto print_fill_factors = [&](std::ofstream& csv, s32 flag) {
    u64 t_i = 0;
    vs_btree.iterateAllPages([&](leanstore::btree::vs::BTreeNode&) { return 0; },
                             [&](leanstore::btree::vs::BTreeNode& leaf) {
                               csv << t_i++ << "," << leaf.fillFactorAfterCompaction() << "," << flag << "," << FLAGS_bstar << "," << FLAGS_su_merge
                                   << "," << FLAGS_tag << endl;
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
  // -------------------------------------------------------------------------------------
  db.startDebuggingThread();
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  begin = chrono::high_resolution_clock::now();
  if (FLAGS_in != "") {
    utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
    tuple_count = input_strings.size();
    PerfEvent e;
    PerfEventBlock b(e, tuple_count);
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        vs_btree.insert(reinterpret_cast<u8*>(const_cast<char*>(input_strings[t_i].data())), input_strings[t_i].size(), 8,
                        reinterpret_cast<u8*>(&t_i));
      }
    });
  } else if (FLAGS_random_insert) {
    vector<u64> random_keys(tuple_count);
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        random_keys[t_i] = t_i;
      }
    });
    std::random_shuffle(random_keys.begin(), random_keys.end());
    sleep(1);
    tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
      for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
        table.insert(random_keys[t_i], payload);
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
  // -------------------------------------------------------------------------------------
  end = chrono::high_resolution_clock::now();
  cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
  sleep(1);
  print_stats();
  // -------------------------------------------------------------------------------------
  if (FLAGS_print_fill_factors)
    print_fill_factors(csv, 0);
  // -------------------------------------------------------------------------------------
  atomic<bool> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  // -------------------------------------------------------------------------------------
  vector<thread> threads;
  sleep(1);
  if (FLAGS_su_merge) {
    threads.emplace_back([&]() {
      running_threads_counter++;
      ThreadCounters::registerThread("merge");
      if (FLAGS_in == "") {
        while (keep_running) {
          Key k = utils::RandomGenerator::getRandU64(0, tuple_count);
          u8 key_bytes[sizeof(Key)];
          compress_bf(key_bytes, fold(key_bytes, k));
          WorkerCounters::myCounters().tx++;
        }
      } else {
        utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
        while (keep_running) {
          Key k = utils::RandomGenerator::getRandU64(0, tuple_count);
          compress_bf(reinterpret_cast<u8*>(const_cast<char*>(input_strings[k].data())), input_strings[k].size());
          WorkerCounters::myCounters().tx++;
        }
      }
      running_threads_counter--;
    });
  }
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
  sleep(1);
  if (FLAGS_verify) {
    if (FLAGS_in != "") {
      utils::FVector<std::string_view> input_strings(FLAGS_in.c_str());
      tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
        for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
          bool flag = true;
          vs_btree.lookupOne(reinterpret_cast<u8*>(const_cast<char*>(input_strings[t_i].data())), input_strings[t_i].size(),
                             [&](const u8* payload, u16 payload_length) {
                               flag &= (payload_length == 8);
                               flag &= (*reinterpret_cast<const u64*>(payload) == t_i);
                             });
          ensure(flag);
        }
      });
    } else {
      tbb::parallel_for(tbb::blocked_range<u64>(0, tuple_count), [&](const tbb::blocked_range<u64>& range) {
        for (u64 t_i = range.begin(); t_i < range.end(); t_i++) {
          Payload result;
          ensure(table.lookup(t_i, result));
          ensure(result == payload);
        }
      });
    }
  }
  // -------------------------------------------------------------------------------------
  if (FLAGS_print_fill_factors)
    print_fill_factors(csv, 1);
  // -------------------------------------------------------------------------------------
  print_stats();
  // -------------------------------------------------------------------------------------
  return 0;
}
