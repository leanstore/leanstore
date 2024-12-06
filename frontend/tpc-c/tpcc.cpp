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
DEFINE_bool(tpcc_verify, false, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_uint64(tpcc_analytical_weight, 0, "");
DEFINE_uint64(ch_a_threads, 0, "CH analytical threads");
DEFINE_uint64(ch_a_rounds, 1, "");
DEFINE_uint64(ch_a_query, 2, "");
DEFINE_uint64(ch_a_start_delay_sec, 0, "");
DEFINE_uint64(ch_a_process_delay_sec, 0, "");
DEFINE_bool(ch_a_infinite, false, "");
DEFINE_bool(ch_a_once, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
void prepareWorker(leanstore::LeanStore& db);
void finishWorker(leanstore::LeanStore& db);
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
   db.registerConfigEntry("ch_a_rounds", FLAGS_ch_a_rounds);
   db.registerConfigEntry("ch_a_query", FLAGS_ch_a_query);
   db.registerConfigEntry("ch_a_start_delay_sec", FLAGS_ch_a_start_delay_sec);
   db.registerConfigEntry("ch_a_process_delay_sec", FLAGS_ch_a_process_delay_sec);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);
   // set creator_threads to worker_threads if == 0
   FLAGS_creator_threads = FLAGS_creator_threads ? FLAGS_creator_threads : FLAGS_worker_threads;
   // -------------------------------------------------------------------------------------
   leanstore::TX_ISOLATION_LEVEL isolation_level = leanstore::parseIsolationLevel(FLAGS_isolation_level);
   // -------------------------------------------------------------------------------------
   const bool should_tpcc_driver_handle_isolation_anomalies = isolation_level < leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   TPCCWorkload<LeanStoreAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock,
                                       FLAGS_order_wdc_index, FLAGS_tpcc_warehouse_count, FLAGS_tpcc_remove,
                                       should_tpcc_driver_handle_isolation_anomalies, FLAGS_tpcc_warehouse_affinity);
   // -------------------------------------------------------------------------------------
   if (!FLAGS_recover) {
      cout << "Loading TPC-C" << endl;
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpcc.loadItem();
         tpcc.loadWarehouse();
         cr::Worker::my().commitTX();
      });
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_creator_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  finishWorker(db);
                  return;
               }
               cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
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
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_verify) {
         cout << "Verifying TPC-C" << endl;
         crm.scheduleJobSync(0, [&]() {
            cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
            tpcc.verifyItems();
            cr::Worker::my().commitTX();
         });
         g_w_id = 1;
         for (u32 t_i = 0; t_i < FLAGS_creator_threads; t_i++) {
            crm.scheduleJobAsync(t_i, [&]() {
               while (true) {
                  u32 w_id = g_w_id++;
                  if (w_id > FLAGS_tpcc_warehouse_count) {
                     return;
                  }
                  cr::Worker::my().startTX(leanstore::TX_MODE::OLTP);
                  tpcc.verifyWarehouse(w_id);
                  cr::Worker::my().commitTX();
               }
            });
            crm.joinAll();
         }
      }
   }
   // -------------------------------------------------------------------------------------
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "TPC-C loaded - consumed space in GiB = " << gib << endl;
   crm.scheduleJobSync(0, [&]() { cout << "Warehouse pages = " << warehouse.btree->countPages() << endl; });
   // -------------------------------------------------------------------------------------
   if(FLAGS_run_until_tx!=0 || FLAGS_run_for_seconds!=0) {
      atomic<u64> keep_running = true;
      atomic<u64> running_threads_counter = 0;
      vector<thread> threads;
      auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
      db.startProfilingThread();
      u64 tx_per_thread[FLAGS_worker_threads];
      // -------------------------------------------------------------------------------------
      for (u64 t_i = FLAGS_worker_threads - FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            prepareWorker(db);
            volatile u64 tx_acc = 0;
            const leanstore::TX_MODE tx_mode = leanstore::TX_MODE::OLAP;
            cr::Worker::my().startTX(tx_mode, isolation_level);
            cr::Worker::my().commitTX();
            // -------------------------------------------------------------------------------------
            if (FLAGS_ch_a_start_delay_sec) {
               cr::Worker::my().cc.switchToReadCommittedMode();
               sleep(FLAGS_ch_a_start_delay_sec);
               cr::Worker::my().cc.switchToSnapshotIsolationMode();
            }
            // -------------------------------------------------------------------------------------
            while (keep_running) {
               jumpmuTry()
               {
                  cr::Worker::my().startTX(tx_mode, isolation_level);
                  {
                     if (FLAGS_ch_a_process_delay_sec) {
                        sleep(FLAGS_ch_a_process_delay_sec);
                     }
                     {
                        JMUW<utils::Timer> timer(CRCounters::myCounters().cc_ms_olap_tx);
                        if (FLAGS_ch_a_rounds) {
                           for (u64 i = 0; i < FLAGS_ch_a_rounds; i++) {
                              tpcc.analyticalQuery(FLAGS_ch_a_query);
                           }
                        } else {
                           while (keep_running) {
                              tpcc.analyticalQuery(FLAGS_ch_a_query);
                           }
                        }
                     }
                     cr::Worker::my().commitTX();
                  }
                  WorkerCounters::myCounters().olap_tx++;
                  tx_acc = tx_acc + 1;
               }
               jumpmuCatch()
               {
                  WorkerCounters::myCounters().olap_tx_abort++;
               }
               if (FLAGS_ch_a_once) {
                  cr::Worker::my().shutdown();
                  sleep(2);
                  break;
               }
            }
            cr::Worker::my().shutdown();
            // -------------------------------------------------------------------------------------
            tx_per_thread[t_i] = tx_acc;
            finishWorker(db);
            running_threads_counter--;
         });
      }
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_worker_threads - FLAGS_ch_a_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            prepareWorker(db);
            volatile u64 tx_acc = 0;
            if (FLAGS_tmp4 && t_i == 0) {
               while (keep_running) {
                  jumpmuTry()
                  {
                     cr::Worker::my().startTX(leanstore::TX_MODE::OLTP, leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION);
                     UpdateDescriptorGenerator1(warehouse_update_descriptor, warehouse_t, w_ytd);
                     warehouse.update1(
                         {1}, [&](warehouse_t& rec) { rec.w_ytd += 1; }, warehouse_update_descriptor);
                     cr::Worker::my().commitTX();
                     WorkerCounters::myCounters().tx++;
                  }
                  jumpmuCatch()
                  {
                     WorkerCounters::myCounters().tx_abort++;
                  }
               }
            } else {
               while (keep_running) {
                  utils::Timer timer(CRCounters::myCounters().cc_ms_oltp_tx);
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
                     tx_acc = tx_acc + 1;
                  }
                  jumpmuCatch()
                  {
                     WorkerCounters::myCounters().tx_abort++;
                  }
               }
            }
            cr::Worker::my().shutdown();
            // -------------------------------------------------------------------------------------
            tx_per_thread[t_i] = tx_acc;
            finishWorker(db);
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
         }
         crm.joinAll();
      }
      cout << endl;
      {
         u64 total = 0;
         for (u64 t_i = 0; t_i < FLAGS_worker_threads - FLAGS_ch_a_threads; t_i++) {
            total += tx_per_thread[t_i];
            cout << tx_per_thread[t_i] << ",";
         }
         cout << endl;
         cout << "TPC-C = " << total << endl;
         total = 0;
         for (u64 t_i = FLAGS_worker_threads - FLAGS_ch_a_threads; t_i < FLAGS_worker_threads; t_i++) {
            total += tx_per_thread[t_i];
            cout << tx_per_thread[t_i] << ",";
         }
         cout << "CH = " << total << endl;
      }
      // -------------------------------------------------------------------------------------
   }
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
   return 0;
}

void prepareWorker(leanstore::LeanStore& db){
   Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
   leanstore::WorkerCounters::myCounters().variable_for_workload =
      std::stol(db.recover("tpcc-h-variable-" + std::to_string(t_id), "0"));;

}

void finishWorker(leanstore::LeanStore& db){
   Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
   db.persist("tpcc-h-variable-" + std::to_string(t_id), std::to_string(leanstore::WorkerCounters::myCounters().variable_for_workload));
}
