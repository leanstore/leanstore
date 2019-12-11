#include "types.hpp"
#include "adapter.hpp"
#include "schema.hpp"
// -------------------------------------------------------------------------------------
#include "/opt/PerfEvent.hpp"
#include <gflags/gflags.h>
#include <tbb/tbb.h>
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <limits>
#include <csignal>
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_uint32(tpcc_threads, 10, "");
DEFINE_uint32(tpcc_tx_count, 1000000, "");
DEFINE_uint32(tpcc_tx_rounds, 10, "");
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
/*
StdMap<warehouse_t> warehouse;
StdMap<district_t> district;
StdMap<customer_t> customer;
StdMap<customer_wdl_t> customerwdl;
StdMap<history_t> history;
StdMap<neworder_t> neworder;
StdMap<order_t> order;
StdMap<order_wdc_t> order_wdc;
StdMap<orderline_t> orderline;
StdMap<item_t> item;
StdMap<stock_t> stock;
 */
// -------------------------------------------------------------------------------------
// yeah, dirty include...
#include "tpcc_workload.hpp"
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
int main(int argc, char **argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   PerfEvent e;
   tbb::task_scheduler_init taskScheduler(FLAGS_tpcc_threads);
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
   chrono::high_resolution_clock::time_point begin, end;
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
//   print_tables_counts();

   unsigned n = FLAGS_tpcc_tx_count;
   PerfEventBlock b(e, n);
   b.print_in_destructor = false;
   // -------------------------------------------------------------------------------------
   atomic<bool> last_second_news_enabled = true;
   atomic<u64> last_second_tx_done = 0;
   thread last_second_news([&] {
      while ( last_second_news_enabled ) {
         u64 tx_done_local = last_second_tx_done.exchange(0);
         cout << endl;
         cout << tx_done_local << " txs in the last second" << endl;
         b.scale = tx_done_local;
         b.e.stopCounters();
         b.printCounters();
         b.e.startCounters();
         sleep(1);
      }
   });
   for ( unsigned j = 0; j < FLAGS_tpcc_tx_rounds; j++ ) {
      begin = chrono::high_resolution_clock::now();
      tbb::parallel_for(tbb::blocked_range<u64>(0, n), [&](const tbb::blocked_range<u64> &range) {
         for ( u64 i = range.begin(); i < range.end(); i++ ) {
            tx();
            last_second_tx_done++;
         }
      });
      end = chrono::high_resolution_clock::now();
//      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
   }
   last_second_news_enabled.store(false);
   last_second_news.join();
   // -------------------------------------------------------------------------------------
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "consumed space in GiB = " << gib << endl;
   // -------------------------------------------------------------------------------------
//   print_tables_counts();
   // -------------------------------------------------------------------------------------
   return 0;
}
