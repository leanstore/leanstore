#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tpcc/config.h"
#include "benchmark/tpcc/workload.h"
#include "leanstore/leanstore.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore TPC-C");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  PerfController ctrl;
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore
  auto db   = std::make_unique<leanstore::LeanStore>();
  auto tpcc = std::make_unique<tpcc::TPCCWorkload<LeanStoreAdapter>>(
    FLAGS_tpcc_warehouse_count, true, true, true, static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, *db);
  tpcc->customer.ToggleAppendBiasMode(true);
  tpcc->history.ToggleAppendBiasMode(true);
  tpcc->order.ToggleAppendBiasMode(true);
  tpcc->orderline.ToggleAppendBiasMode(true);
  tpcc->item.ToggleAppendBiasMode(true);
  tpcc->stock.ToggleAppendBiasMode(true);

  // TPC-C loader
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    tpcc->LoadItem();
    tpcc->LoadWarehouse();
    db->CommitTransaction();
  });
  for (Integer w_id = 1; w_id <= static_cast<Integer>(FLAGS_tpcc_warehouse_count); w_id++) {
    LOG_DEBUG("Prepare for warehouse %d", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      tpcc->InitializeThread();
      db->StartTransaction();
      tpcc->LoadStock(w_id);
      tpcc->LoadDistrinct(w_id);
      for (Integer d_id = 1; d_id <= tpcc->D_PER_WH; d_id++) {
        tpcc->LoadCustomer(w_id, d_id);
        tpcc->LoadOrder(w_id, d_id);
      }
      db->CommitTransaction();
    });
    LOG_DEBUG("Prepare warehouse %d successfully", w_id);
  }
  db->worker_pool.JoinAll();
  LOG_INFO("Space used: %.4f GB", db->AllocatedSize());
  auto initial_wal_size = db->WALSize();

#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    auto w_cnt = tpcc->warehouse.Count();
    auto d_cnt = tpcc->district.Count();
    LOG_DEBUG("Warehouse count: %lu - District count: %lu", w_cnt, d_cnt);
    assert(w_cnt == FLAGS_tpcc_warehouse_count);
    assert(d_cnt == FLAGS_tpcc_warehouse_count * tpcc->D_PER_WH);

    auto c_cnt       = tpcc->customer.Count();
    auto c_index_cnt = tpcc->customer_wdc.Count();
    LOG_DEBUG("Customer count: %lu - Customer's index count: %lu", c_cnt, c_index_cnt);
    assert(c_cnt == c_index_cnt);

    auto o_cnt       = tpcc->order.Count();
    auto o_index_cnt = tpcc->order_wdc.Count();
    LOG_DEBUG("Order count: %lu - Order's index count: %lu", o_cnt, o_index_cnt);
    assert(o_cnt == o_index_cnt);
    db->CommitTransaction();
  });
#endif

  // TPC-C execution
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  for (auto t_id = 0U; t_id < FLAGS_worker_count; t_id++) {
    db->worker_pool.ScheduleAsyncJob(t_id, [&, thread_id = t_id]() {
      tpcc->InitializeThread();

      while (keep_running.load()) {
        int w_id = (FLAGS_tpcc_warehouse_affinity) ? (thread_id % FLAGS_tpcc_warehouse_count) + 1
                                                   : UniformRand(1, FLAGS_tpcc_warehouse_count);
        db->StartTransaction(tpcc->NextTransactionArrivalTime([&]() { db->CheckDuringIdle(); }));
        tpcc->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tpcc_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Space used: %.4f GB - WAL size: %.4f GB", db->AllocatedSize(), db->WALSize() - initial_wal_size);
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
}