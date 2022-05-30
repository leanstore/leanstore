#include "../shared/RocksDBAdapter.hpp"
#include "TPCCWorkload.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include <rocksdb/db.h>

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
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_string(rocks_db, "none", "none/pessimistic/optimistic");
// -------------------------------------------------------------------------------------
thread_local rocksdb::Transaction* RocksDB::txn = nullptr;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("RocksDB TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   RocksDB::DB_TYPE type;
   if (FLAGS_rocks_db == "none") {
      type = RocksDB::DB_TYPE::DB;
   } else if (FLAGS_rocks_db == "pessimistic") {
      type = RocksDB::DB_TYPE::TransactionDB;
   } else if (FLAGS_rocks_db == "optimistic") {
      // TODO: still WIP
      UNREACHABLE();
      type = RocksDB::DB_TYPE::OptimisticDB;
   } else {
      UNREACHABLE();
   }
   RocksDB rocks_db(type);
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
                                     FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove, true);
   std::vector<thread> threads;
   std::atomic<u32> g_w_id = 1;
   rocks_db.startTX();
   tpcc.loadItem();
   tpcc.loadWarehouse();
   rocks_db.commitTX();
   for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      threads.emplace_back([&]() {
         while (true) {
            const u32 w_id = g_w_id++;
            if (w_id > FLAGS_tpcc_warehouse_count) {
               return;
            }
            jumpmuTry()
            {
               rocks_db.startTX();
               tpcc.loadStock(w_id);
               tpcc.loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  tpcc.loadCustomer(w_id, d_id);
                  tpcc.loadOrders(w_id, d_id);
               }
               rocks_db.commitTX();
            }
            jumpmuCatch() { UNREACHABLE(); }
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
   for (u64 t_i = 0; t_i < FLAGS_ch_a_threads; t_i++) {
      thread_committed[t_i] = 0;
      thread_aborted[t_i] = 0;
      // -------------------------------------------------------------------------------------
      threads.emplace_back([&, t_i]() {
         running_threads_counter++;
         if (FLAGS_pin_threads) {
            leanstore::utils::pinThisThread(t_i);
         }
         tpcc.prepare();
         while (keep_running) {
            jumpmuTry()
            {
               rocks_db.startTX();
               for (u64 i = 0; i < FLAGS_ch_a_rounds; i++) {
                  tpcc.analyticalQuery();
               }
               rocks_db.commitTX();
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
         tpcc.prepare();
         while (keep_running) {
            jumpmuTry()
            {
               Integer w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
               rocks_db.startTX();
               tpcc.tx(w_id);
               rocks_db.commitTX();
               thread_committed[t_i]++;
            }
            jumpmuCatch() { thread_aborted[t_i]++; }
            running_threads_counter--;
         }
      });
   }
   // -------------------------------------------------------------------------------------
   threads.emplace_back([&]() {
      running_threads_counter++;
      while (keep_running) {
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
