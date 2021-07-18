#include "../shared/RocksDBAdapter.hpp"
#include "TPCCWorkload.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <rocksdb/db.h>

#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("RocksDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   RocksDB rocks_db;
   RocksDBAdapter<warehouse_t> warehouse(rocks_db);
   RocksDBAdapter<district_t> district(rocks_db);
   RocksDBAdapter<customer_t> customer(rocks_db);
   RocksDBAdapter<customer_wdl_t> customerwdl(rocks_db);
   RocksDBAdapter<history_t> history(rocks_db);
   RocksDBAdapter<neworder_t> neworder(rocks_db);
   RocksDBAdapter<order_t> order(rocks_db);
   RocksDBAdapter<order_wdc_t> order_wdc(rocks_db);
   RocksDBAdapter<orderline_t> orderline(rocks_db);
   RocksDBAdapter<item_t> item(rocks_db);
   RocksDBAdapter<stock_t> stock(rocks_db);
   // -------------------------------------------------------------------------------------
   TPCCWorkload<RocksDBAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                     FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove, true, true);
   // -------------------------------------------------------------------------------------
   tpcc.loadItem();
   tpcc.loadWarehouse();
   for (u64 w_id = 1; w_id <= FLAGS_tpcc_warehouse_count; w_id++) {
      tpcc.loadStock(w_id);
      tpcc.loadDistrinct(w_id);
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         tpcc.loadCustomer(w_id, d_id);
         tpcc.loadOrders(w_id, d_id);
      }
   }
   // -------------------------------------------------------------------------------------
   std::vector<thread> threads;
   atomic<u64> running_threads_counter = 0;
   atomic<u64> keep_running = true;
   atomic<u64> counter = 0;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&]() {
         running_threads_counter++;
         tpcc.prepare();
         while (keep_running) {
            Integer w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
            tpcc.tx(w_id);
            counter++;
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   threads.emplace_back([&]() {
      running_threads_counter++;
      while (keep_running) {
         cout << counter.exchange(0) << endl;
         sleep(1);
      }
      running_threads_counter--;
   });
   sleep(FLAGS_run_for_seconds);
   keep_running = false;
   while (running_threads_counter) {
   }
   for (auto& thread : threads) {
      thread.join();
   }
   // -------------------------------------------------------------------------------------
   return 0;
}
