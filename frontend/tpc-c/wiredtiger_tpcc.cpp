#include "Units.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
#include "types.hpp"
// -------------------------------------------------------------------------------------
#include "schema.hpp"
#include "wiredtiger_adapter.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
DEFINE_uint32(tpcc_warehouse_count, 1, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_cross_warehouses, true, "");
DEFINE_bool(tpcc_remove, true, "");
// -------------------------------------------------------------------------------------
thread_local WT_SESSION* WiredTigerDB::session = nullptr;
thread_local WT_CURSOR* WiredTigerDB::cursor[20] = {nullptr};
// -------------------------------------------------------------------------------------
WiredTigerAdapter<warehouse_t> warehouse;
WiredTigerAdapter<district_t> district;
WiredTigerAdapter<customer_t> customer;
WiredTigerAdapter<customer_wdl_t> customerwdl;
WiredTigerAdapter<history_t> history;
WiredTigerAdapter<neworder_t> neworder;
WiredTigerAdapter<order_t> order;
WiredTigerAdapter<order_wdc_t> order_wdc;
WiredTigerAdapter<orderline_t> orderline;
WiredTigerAdapter<item_t> item;
WiredTigerAdapter<stock_t> stock;
#include "tpcc_workload.hpp"
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("WiredTiger TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   warehouseCount = FLAGS_tpcc_warehouse_count;
   // -------------------------------------------------------------------------------------
   WiredTigerDB wiredtiger_db;
   wiredtiger_db.prepareThread();
   warehouse = WiredTigerAdapter<warehouse_t>(wiredtiger_db);
   district = WiredTigerAdapter<district_t>(wiredtiger_db);
   customer = WiredTigerAdapter<customer_t>(wiredtiger_db);
   customerwdl = WiredTigerAdapter<customer_wdl_t>(wiredtiger_db);
   history = WiredTigerAdapter<history_t>(wiredtiger_db);
   neworder = WiredTigerAdapter<neworder_t>(wiredtiger_db);
   order = WiredTigerAdapter<order_t>(wiredtiger_db);
   order_wdc = WiredTigerAdapter<order_wdc_t>(wiredtiger_db);
   orderline = WiredTigerAdapter<orderline_t>(wiredtiger_db);
   item = WiredTigerAdapter<item_t>(wiredtiger_db);
   stock = WiredTigerAdapter<stock_t>(wiredtiger_db);
   // -------------------------------------------------------------------------------------
   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   loadItem();
   loadWarehouse();
   for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&]() {
         wiredtiger_db.prepareThread();
         while (true) {
            const u32 w_id = g_w_id++;
            if (w_id > FLAGS_tpcc_warehouse_count) {
               return;
            }
            jumpmuTry()
            {
               // wiredtiger_db.startTX();
               loadStock(w_id);
               loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  loadCustomer(w_id, d_id);
                  loadOrders(w_id, d_id);
               }
               // wiredtiger_db.commitTX();
            }
            jumpmuCatch() { ensure(false); }
         }
      });
   }
   for (auto& thread : threads) {
      thread.join();
   }
   threads.clear();
   // -------------------------------------------------------------------------------------
   atomic<u64> running_threads_counter = 0;
   atomic<u64> keep_running = true;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         if (FLAGS_pin_threads) {
            leanstore::utils::pinThisThread(t_i);
         }
         wiredtiger_db.prepareThread();
         while (keep_running) {
            jumpmuTry()
            {
               wiredtiger_db.startTX();
               Integer w_id;
               if (FLAGS_tpcc_warehouse_affinity) {
                  w_id = t_i + 1;
               } else {
                  w_id = urand(1, FLAGS_tpcc_warehouse_count);
               }
               tx(w_id);
               wiredtiger_db.commitTX();
               thread_committed[t_i]++;
            }
            jumpmuCatch() { thread_aborted[t_i]++; }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   threads.emplace_back([&]() {
      running_threads_counter++;
      u64 time = 0;
      cout << "t,tag,tx_committed,tx_aborted" << endl;
      while (keep_running) {
         cout << time++ << "," << FLAGS_tag << ",";
         u64 total_committed = 0, total_aborted = 0;
         for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
            total_committed += thread_committed[t_i].exchange(0);
            total_aborted += thread_aborted[t_i].exchange(0);
         }
         cout << total_committed << "," << total_aborted << endl;
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
