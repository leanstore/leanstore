#include "../shared/WiredTigerAdapter.hpp"
#include "TPCCWorkload.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

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
thread_local WT_SESSION* WiredTigerDB::session = nullptr;
thread_local WT_CURSOR* WiredTigerDB::cursor[20] = {nullptr};
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("WiredTiger TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   WiredTigerDB rocks_db;
   rocks_db.prepareThread();
   WiredTigerAdapter<warehouse_t> warehouse(rocks_db);
   WiredTigerAdapter<district_t> district(rocks_db);
   WiredTigerAdapter<customer_t> customer(rocks_db);
   WiredTigerAdapter<customer_wdl_t> customerwdl(rocks_db);
   WiredTigerAdapter<history_t> history(rocks_db);
   WiredTigerAdapter<neworder_t> neworder(rocks_db);
   WiredTigerAdapter<order_t> order(rocks_db);
   WiredTigerAdapter<order_wdc_t> order_wdc(rocks_db);
   WiredTigerAdapter<orderline_t> orderline(rocks_db);
   WiredTigerAdapter<item_t> item(rocks_db);
   WiredTigerAdapter<stock_t> stock(rocks_db);
   // -------------------------------------------------------------------------------------
   TPCCWorkload<WiredTigerAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                        FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove);
   // -------------------------------------------------------------------------------------
   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   if (1) {
      tpcc.loadItem();
      tpcc.loadWarehouse();
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         threads.emplace_back([&]() {
            rocks_db.prepareThread();
            while (true) {
               const u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               tpcc.loadStock(w_id);
               tpcc.loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  tpcc.loadCustomer(w_id, d_id);
                  tpcc.loadOrders(w_id, d_id);
               }
            }
         });
      }
      for (auto& thread : threads) {
         thread.join();
      }
      threads.clear();
   }
   // -------------------------------------------------------------------------------------
   atomic<u64> running_threads_counter = 0;
   atomic<u64> keep_running = true;
   atomic<u64> counter = 0;
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         rocks_db.prepareThread();
         tpcc.prepare();
         while (keep_running) {
            Integer w_id;
            if (FLAGS_tpcc_warehouse_affinity) {
               w_id = t_i + 1;
            } else {
               w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
            }
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
