#include <leanstore/counters/WorkerCounters.hpp>

#include "adapter.hpp"
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
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_uint32(tpcc_seconds, 10, "");
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
int main(int argc, char** argv)
{
  gflags::SetUsageMessage("Leanstore TPC-C");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // -------------------------------------------------------------------------------------
  LeanStore db;
  tbb::task_scheduler_init taskScheduler(FLAGS_worker_threads);
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
  // -------------------------------------------------------------------------------------
  load();
  double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
  cout << "data loaded - consumed space in GiB = " << gib << endl;
  // -------------------------------------------------------------------------------------
  auto print_tables_counts = [&]() {
    cout << "warehouse: " << warehouse.count() << endl;
    cout << "district: " << district.count() << endl;
    cout << "customer: " << customer.count() << endl;
    cout << "customerwdl: " << customerwdl.count() << endl;
    cout << "history: " << history.count() << endl;
    cout << "neworder: " << neworder.count() << endl;
    cout << "order: " << order.count() << endl;
    cout << "order_wdc: " << order_wdc.count() << endl;
    cout << "orderline: " << orderline.count() << endl;
    cout << "item: " << item.count() << endl;
    cout << "stock: " << stock.count() << endl;
    cout << endl;
  };
  auto print_tables_pages = [&]() {
    // cout << "warehouse: " << warehouse.btree->pages.load() << endl;
    // cout << "district: " << district.btree->pages.load() << endl;
    // cout << "customer: " << customer.btree->pages.load() << endl;
    // cout << "customerwdl: " << customerwdl.btree->pages.load() << endl;
    // cout << "history: " << history.btree->pages.load() << endl;
    // cout << "neworder: " << neworder.btree->pages.load() << endl;
    // cout << "order: " << order.btree->pages.load() << endl;
    // cout << "order_wdc: " << order_wdc.btree->pages.load() << endl;
    // cout << "orderline: " << orderline.btree->pages.load() << endl;
    // cout << "item: " << item.btree->pages.load() << endl;
    // cout << "stock: " << stock.btree->pages.load() << endl;
    cout << endl;
  };
  //   print_tables_counts();

  // -------------------------------------------------------------------------------------
  atomic<u64> keep_running = true;
  atomic<u64> running_threads_counter = 0;
  vector<thread> threads;
  auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
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
      threads.emplace_back(
          [&](u64 w_begin, u64 w_end) {
            running_threads_counter++;
            while (keep_running) {
              tx(urand(w_begin, w_end));
              WorkerCounters::myCounters().tx++;
            }
            running_threads_counter--;
          },
          w_begin, w_end);
    }
  } else {
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&]() {
        running_threads_counter++;
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
        running_threads_counter--;
      });
    }
  }
  {
    // Shutdown threads
    sleep(FLAGS_tpcc_seconds);
    keep_running = false;
    while (running_threads_counter) {
      _mm_pause();
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  print_tables_pages();
  // -------------------------------------------------------------------------------------
  gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
  cout << "consumed space in GiB = " << gib << endl;
  // -------------------------------------------------------------------------------------
  //   print_tables_counts();
  // -------------------------------------------------------------------------------------
  return 0;
}
