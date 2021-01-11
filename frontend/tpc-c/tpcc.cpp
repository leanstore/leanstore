#include "adapter.hpp"
#include "leanstore/counters/ThreadCounters.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
#include "schema.hpp"
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
LeanStoreAdapter<warehouse_t> warehouse;
LeanStoreAdapter<district_t> district;
LeanStoreAdapter<customer_t> customer;
LeanStoreAdapter<customer_wdl_t> customerwdl;
LeanStoreAdapter<history_t> history;
LeanStoreAdapter<neworder_t> neworder;
LeanStoreAdapter<order_t> order;
LeanStoreAdapter<order_wdc_t> order_wdc;
LeanStoreAdapter<orderline_t> orderline;
LeanStoreAdapter<item_t> item;
LeanStoreAdapter<stock_t> stock;
// -------------------------------------------------------------------------------------
// yeah, dirty include...
#include "tpcc_workload.hpp"
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
  double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
  return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("Leanstore TPC-C");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  LeanStore db;
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  warehouseCount = FLAGS_tpcc_warehouse_count;
  warehouse = LeanStoreAdapter<warehouse_t>(db, "warehouse");
  district = LeanStoreAdapter<district_t>(db, "district");
  customer = LeanStoreAdapter<customer_t>(db, "customer");
  customerwdl = LeanStoreAdapter<customer_wdl_t>(db, "customerwdl");
  history = LeanStoreAdapter<history_t>(db, "history");
  neworder = LeanStoreAdapter<neworder_t>(db, "neworder");
  order = LeanStoreAdapter<order_t>(db, "order");
  order_wdc = LeanStoreAdapter<order_wdc_t>(db, "order_wdc");
  orderline = LeanStoreAdapter<orderline_t>(db, "orderline");
  item = LeanStoreAdapter<item_t>(db, "item");
  stock = LeanStoreAdapter<stock_t>(db, "stock");
  // -------------------------------------------------------------------------------------
  db.registerConfigEntry("tpcc_warehouse_count", [&](ostream& out) { out << FLAGS_tpcc_warehouse_count; });
  db.registerConfigEntry("tpcc_warehouse_affinity", [&](ostream& out) { out << FLAGS_tpcc_warehouse_affinity; });
  db.registerConfigEntry("run_until_tx", [&](ostream& out) { out << FLAGS_run_until_tx; });
  // -------------------------------------------------------------------------------------
  // tbb::task_scheduler_init task_scheduler(FLAGS_worker_threads);
  tbb::task_scheduler_init task_scheduler(thread::hardware_concurrency());
  load();
  task_scheduler.terminate();
  task_scheduler.initialize(FLAGS_worker_threads);
  // -------------------------------------------------------------------------------------
  double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
  cout << "data loaded - consumed space in GiB = " << gib << endl;
  // -------------------------------------------------------------------------------------
  atomic<u64> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
  db.startDebuggingThread();
  if (FLAGS_tpcc_warehouse_affinity) {
    if (FLAGS_tpcc_warehouse_count < FLAGS_worker_threads) {
      cerr << "There must be more warehouses than threads in affinity mode" << endl;
      exit(1);
    }
    const u64 warehouses_pro_thread = FLAGS_tpcc_warehouse_count / FLAGS_worker_threads;
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      u64 w_begin = 1 + (t_i * warehouses_pro_thread);
      u64 w_end = w_begin + (warehouses_pro_thread - 1);
      if (t_i == FLAGS_worker_threads - 1) {
        w_end = FLAGS_tpcc_warehouse_count;
      }
      threads.emplace_back([&, t_i, w_begin, w_end]() {
        running_threads_counter++;
        pthread_setname_np(pthread_self(), "worker");
        const u64 r_id = ThreadCounters::registerThread("worker_" + std::to_string(t_i));
        if (FLAGS_pin_threads)
          utils::pinThisThreadRome(FLAGS_pp_threads + t_i);
        while (keep_running) {
          tx(urand(w_begin, w_end));
          WorkerCounters::myCounters().tx++;
        }
        ThreadCounters::removeThread(r_id);
        running_threads_counter--;
      });
    }
  } else {
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&, t_i]() {
        running_threads_counter++;
        pthread_setname_np(pthread_self(), "worker");
        const u64 r_id = ThreadCounters::registerThread("worker_" + std::to_string(t_i));
        if (FLAGS_pin_threads)
          utils::pinThisThreadRome(FLAGS_pp_threads + t_i);
        while (keep_running) {
          Integer w_id;
          if (FLAGS_zipf_factor == 0) {
            w_id = urand(1, FLAGS_tpcc_warehouse_count);
          } else {
            w_id = 1 + (random->rand() % (FLAGS_tpcc_warehouse_count));
          }
          tx(w_id);
          WorkerCounters::myCounters().tx++;
        }
        ThreadCounters::removeThread(r_id);
        running_threads_counter--;
      });
    }
  }
  {
    if (FLAGS_run_until_tx) {
      while (true) {
        if (db.getGlobalStats().accumulated_tx_counter >= FLAGS_run_until_tx) {
          cout << FLAGS_run_until_tx << " has been reached";
          break;
        }
        usleep(500);
      }
    } else {
      // Shutdown threads
      sleep(FLAGS_run_for_seconds);
    }
    keep_running = false;
    while (running_threads_counter) {
      MYPAUSE();
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  // -------------------------------------------------------------------------------------
  gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
  cout << "Consumed space in GiB = " << gib << endl;
  // -------------------------------------------------------------------------------------
  return 0;
}
