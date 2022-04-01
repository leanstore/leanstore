#include "../shared/LMDBAdapter.hpp"
#include "TPCCWorkload.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Misc.hpp"
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
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_uint64(ch_a_delay_sec, 0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_header, true, "");
// -------------------------------------------------------------------------------------
thread_local lmdb::txn LMDB::txn = nullptr;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("LMDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LMDB lm_db;
   LMDBAdapter<warehouse_t> warehouse(lm_db, "warehouse");
   LMDBAdapter<district_t> district(lm_db, "district");
   LMDBAdapter<customer_t> customer(lm_db, "customer");
   LMDBAdapter<customer_wdl_t> customerwdl(lm_db, "customer_wdl");
   LMDBAdapter<history_t> history(lm_db, "history");
   LMDBAdapter<neworder_t> neworder(lm_db, "neworder");
   LMDBAdapter<order_t> order(lm_db, "order");
   LMDBAdapter<order_wdc_t> order_wdc(lm_db, "order_wdc");
   LMDBAdapter<orderline_t> orderline(lm_db, "orderline");
   LMDBAdapter<item_t> item(lm_db, "item");
   LMDBAdapter<stock_t> stock(lm_db, "stock");
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   TPCCWorkload<LMDBAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                  FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove, should_tpcc_driver_handle_isolation_anomalies,
                                  FLAGS_tpcc_warehouse_affinity);
   // -------------------------------------------------------------------------------------
   lm_db.startTX();
   std::vector<thread> threads;
   tpcc.loadItem();
   tpcc.loadWarehouse();
   for (u32 w_id = 1; w_id <= FLAGS_tpcc_warehouse_count; w_id++) {
      tpcc.loadStock(w_id);
      tpcc.loadDistrinct(w_id);
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         tpcc.loadCustomer(w_id, d_id);
         tpcc.loadOrders(w_id, d_id);
      }
   }
   lm_db.commitTX();
   // -------------------------------------------------------------------------------------
   atomic<u64> running_threads_counter = 0;
   atomic<u64> keep_running = true;
   std::atomic<u64> thread_committed[FLAGS_worker_threads];
   std::atomic<u64> thread_aborted[FLAGS_worker_threads];
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_ch_a_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         if (FLAGS_pin_threads) {
            leanstore::utils::pinThisThread(t_i);
         }
         lm_db.startTX();
         tpcc.prepare();
         lm_db.commitTX();
         // -------------------------------------------------------------------------------------
         if (FLAGS_ch_a_delay_sec) {
            sleep(FLAGS_ch_a_delay_sec);
         }
         // -------------------------------------------------------------------------------------
         while (keep_running) {
            jumpmuTry()
            {
               lm_db.startTX(true);
               for (u64 i = 0; i < FLAGS_ch_a_rounds; i++) {
                  tpcc.analyticalQuery(FLAGS_ch_a_query);
               }
               lm_db.commitTX();
               thread_committed[t_i]++;
            }
            jumpmuCatch() { thread_aborted[t_i]++; }
         }
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   for (u64 t_i = FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         if (FLAGS_pin_threads) {
            leanstore::utils::pinThisThread(t_i);
         }
         lm_db.startTX();
         tpcc.prepare();
         lm_db.commitTX();
         while (keep_running) {
            jumpmuTry()
            {
               Integer w_id;
               if (FLAGS_tpcc_warehouse_affinity) {
                  w_id = t_i + 1;
               } else {
                  w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
               }
               // -------------------------------------------------------------------------------------
               std::tuple<s32, bool> tx_info = tpcc.getRandomTXInfo();
               lm_db.startTX(std::get<1>(tx_info));
               tpcc.execTX(w_id, std::get<0>(tx_info));
               lm_db.commitTX();
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
      if (FLAGS_print_header) {
         cout << "t,tag,olap_committed,olap_aborted,oltp_committed,oltp_aborted" << endl;
      }
      while (keep_running) {
         cout << time++ << "," << FLAGS_tag << ",";
         u64 total_committed = 0, total_aborted = 0;
         for (u64 t_i = 0; t_i < FLAGS_ch_a_threads; t_i++) {
            total_committed += thread_committed[t_i].exchange(0);
            total_aborted += thread_aborted[t_i].exchange(0);
         }
         cout << total_committed << "," << total_aborted << ",";
         total_committed = 0;
         total_aborted = 0;
         // -------------------------------------------------------------------------------------
         for (u64 t_i = FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
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
