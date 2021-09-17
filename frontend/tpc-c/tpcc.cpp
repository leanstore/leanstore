#include "../shared/LeanStoreAdapter.hpp"
#include "Schema.hpp"
#include "TPCCWorkload.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_int64(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_bool(tpcc_cross_warehouses, true, "");
DEFINE_uint64(tpcc_analytical_weight, 0, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   assert(FLAGS_tpcc_warehouse_count > 0);
   LeanStore::addS64Flag("TPC_SCALE", &FLAGS_tpcc_warehouse_count);
   // -------------------------------------------------------------------------------------
   // Check arguments
   ensure(FLAGS_ch_a_threads < FLAGS_worker_threads);
   ensure(!FLAGS_tpcc_warehouse_affinity || FLAGS_tpcc_warehouse_count >= FLAGS_worker_threads);
   // -------------------------------------------------------------------------------------
   LeanStore db;
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
   auto& crm = db.getCRManager();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() {
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
   });
   // -------------------------------------------------------------------------------------
   db.registerConfigEntry("tpcc_warehouse_count", FLAGS_tpcc_warehouse_count);
   db.registerConfigEntry("tpcc_warehouse_affinity", FLAGS_tpcc_warehouse_affinity);
   db.registerConfigEntry("ch_a_threads", FLAGS_ch_a_threads);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   // -------------------------------------------------------------------------------------
   const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   TPCCWorkload<LeanStoreAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                       FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove,
                                       should_tpcc_driver_handle_isolation_anomalies, FLAGS_tpcc_cross_warehouses);
   // -------------------------------------------------------------------------------------
   if (!FLAGS_recover) {
      cout << "Loading TPC-C" << endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX();
         tpcc.loadItem();
         tpcc.loadWarehouse();
         cr::Worker::my().commitTX();
      });
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX();
               tpcc.loadStock(w_id);
               tpcc.loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  tpcc.loadCustomer(w_id, d_id);
                  tpcc.loadOrders(w_id, d_id);
               }
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
   }
   // -------------------------------------------------------------------------------------
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "TPC-C loaded - consumed space in GiB = " << gib << endl;
   crm.scheduleJobSync(0, [&]() { cout << "Warehouse pages = " << warehouse.btree->countPages() << endl; });
   // -------------------------------------------------------------------------------------
   atomic<u64> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
   db.startProfilingThread();
   u64 tx_per_thread[FLAGS_worker_threads];
   // -------------------------------------------------------------------------------------
   for (u64 t_i = 0; t_i < FLAGS_ch_a_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         tpcc.prepare();
         volatile u64 tx_acc = 0;
         const leanstore::TX_MODE tx_mode = FLAGS_olap_mode ? leanstore::TX_MODE::OLAP : leanstore::TX_MODE::OLTP;
         cr::Worker::my().startTX(tx_mode, isolation_level);
         cr::Worker::my().commitTX();
         while (keep_running) {
            jumpmuTry()
            {
               cr::Worker::my().startTX(tx_mode, isolation_level);
               if (FLAGS_tmp5) {
                  for (u64 i = 0; i <= 10; i++)
                     if (i != 5)
                        leanstore::cr::Worker::my().relations_cut_from_snapshot.add(i);
               }
               for (u64 i = 0; i < FLAGS_ch_a_rounds; i++) {
                  tpcc.analyticalQuery(FLAGS_ch_a_query);
               }
               cr::Worker::my().commitTX();
               tx_acc++;
            }
            jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
         }
         cr::Worker::my().shutdown();
         // -------------------------------------------------------------------------------------
         tx_per_thread[t_i] = tx_acc;
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
   for (u64 t_i = FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         tpcc.prepare();
         volatile u64 tx_acc = 0;
         while (keep_running) {
            jumpmuTry()
            {
               cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, isolation_level);
               u32 w_id;
               if (FLAGS_tpcc_warehouse_affinity) {
                  w_id = t_i + 1;
               } else {
                  w_id = tpcc.urand(1, FLAGS_tpcc_warehouse_count);
               }
               tpcc.tx(w_id);
               if (FLAGS_tpcc_abort_pct && tpcc.urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                  cr::Worker::my().abortTX();
               } else {
                  cr::Worker::my().commitTX();
               }
               WorkerCounters::myCounters().tx++;
               tx_acc++;
            }
            jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
         }
         cr::Worker::my().shutdown();
         // -------------------------------------------------------------------------------------
         tx_per_thread[t_i] = tx_acc;
         running_threads_counter--;
      });
   }
   // -------------------------------------------------------------------------------------
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
      crm.joinAll();
   }
   cout << endl;
   {
      u64 total = 0;
      for (u64 t_i = FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
         total += tx_per_thread[t_i];
         cout << tx_per_thread[t_i] << ",";
      }
      cout << endl;
      cout << "TPC-C = " << total << endl;
      total = 0;
      for (u64 t_i = 0; t_i < FLAGS_ch_a_threads; t_i++) {
         total += tx_per_thread[t_i];
         cout << tx_per_thread[t_i] << ",";
      }
      cout << "CH = " << total << endl;
   }
   // -------------------------------------------------------------------------------------
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
   return 0;
}
