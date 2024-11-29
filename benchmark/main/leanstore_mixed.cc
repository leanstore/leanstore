#include "benchmark/tpcc/config.h"
#include "benchmark/tpcc/workload.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

DEFINE_uint32(mixed_ycsb_ratio, 50,
              "Proportion of workers running YCSB benchmark."
              "The rest runs TPC-C");
DEFINE_uint32(mixed_group_size, 4,
              "The logical group size which we split workers into"
              "Within this group, mixed_ycsb_ratio % will run YCSB, and 100 - mixed_ycsb_ratio will run TPC-C");
DEFINE_uint32(mixed_exec_seconds, 20, "Execution time");

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Mix TPC-C & YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);
  // Statistics
  PerfEvent e;
  PerfController ctrl;
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore, TPC-C, and YCSB
  auto db = std::make_unique<leanstore::LeanStore>();

  // Initialize TPC-C
  auto tpcc = std::make_unique<tpcc::TPCCWorkload<LeanStoreAdapter>>(
    FLAGS_tpcc_warehouse_count, true, true, true, static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, *db);
  tpcc->customer.ToggleAppendBiasMode(true);
  tpcc->history.ToggleAppendBiasMode(true);
  tpcc->order.ToggleAppendBiasMode(true);
  tpcc->orderline.ToggleAppendBiasMode(true);
  tpcc->item.ToggleAppendBiasMode(true);
  tpcc->stock.ToggleAppendBiasMode(true);

  // Initialize YCSB
  ycsb::WorkerLocalPayloads payloads(FLAGS_worker_count);
  for (auto &payload : payloads) { payload.reset(new uint8_t[FLAGS_ycsb_payload_size]()); }
  auto ycsb =
    std::make_unique<ycsb::YCSBKeyValue>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                         static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, payloads, *db);

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

  // YCSB loader
  std::atomic<UInteger> w_id_loader = 0;
  LOG_DEBUG("Prepare YCSB initial data");
  tbb::parallel_for(tbb::blocked_range<Integer>(1, FLAGS_ycsb_record_count + 1),
                    [&](const tbb::blocked_range<Integer> &range) {
                      auto w_id = (++w_id_loader) % FLAGS_worker_count;
                      db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id, range]() {
                        db->StartTransaction();
                        ycsb->LoadInitialData(w_id, range);
                        db->CommitTransaction();
                      });
                    });
  db->worker_pool.JoinAll();
  LOG_INFO("Space used: %.4f GB", db->AllocatedSize());

  // Main benchmark
  std::atomic<bool> keep_running(true);
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  // Execution -- we split workers into group of 4
  for (auto w_id = 0U; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id]() {
      while (keep_running.load()) {
        db->StartTransaction();
        if (w_id % FLAGS_mixed_group_size <=
            (FLAGS_mixed_ycsb_ratio * static_cast<double>(FLAGS_mixed_group_size) / 100)) {
          ycsb->ExecuteTransaction(w_id);
        } else {
          int wh_id = (FLAGS_tpcc_warehouse_affinity) ? (w_id % FLAGS_tpcc_warehouse_count) + 1
                                                      : UniformRand(1, FLAGS_tpcc_warehouse_count);
          tpcc->ExecuteTransaction(wh_id);
        }
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_mixed_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Total completed txn %lu - Space used: %.4f GB - WAL size: %.4f GB",
           leanstore::statistics::total_committed_txn.load(), db->AllocatedSize(), db->WALSize());
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
}