#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"
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
  gflags::SetUsageMessage("Leanstore YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(ycsb::SUPPORTED_PAYLOAD_SIZE), std::end(ycsb::SUPPORTED_PAYLOAD_SIZE),
                FLAGS_ycsb_payload_size) == std::end(ycsb::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN("Payload size %lu not supported, check ycsb::SUPPORTED_PAYLOAD_SIZE", FLAGS_ycsb_payload_size);
    return 0;
  }

  // Flags correction
  if (!FLAGS_ycsb_random_payload) { FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size; }

  // Setup worker-local payload
  ycsb::WorkerLocalPayloads payloads(FLAGS_worker_count);
  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE)) uint8_t[FLAGS_ycsb_max_payload_size]());
  }

  // Setup env
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);
  leanstore::RegisterSEGFAULTHandler();
  PerfEvent e;
  PerfController ctrl;

  // Initialize LeanStore
  auto db = std::make_unique<leanstore::LeanStore>();
  std::unique_ptr<ycsb::YCSBWorkloadInterface> ycsb;

  if (FLAGS_ycsb_payload_size == ycsb::BLOB_NORMAL_PAYLOAD) {
    ycsb =
      std::make_unique<ycsb::YCSBKeyValue>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                           static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, payloads, *db);
  } else {
    ycsb =
      std::make_unique<ycsb::YCSBBlobState>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, true,
                                            static_cast<double>(FLAGS_txn_rate) / FLAGS_worker_count, payloads, *db);
  }

  // YCSB loader
  std::atomic<UInteger> w_id_loader = 0;
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
#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    LOG_DEBUG("Record count: %lu", ycsb->CountEntries());
    assert(ycsb->CountEntries() == FLAGS_ycsb_record_count);
    db->CommitTransaction();
  });
#endif

  // YCSB profiling
  std::atomic<bool> keep_running(true);
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  // YCSB execution
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id]() {
      while (keep_running.load()) {
        db->StartTransaction(ycsb->NextTransactionArrivalTime([&]() { db->CheckDuringIdle(); }));
        ycsb->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Space used: %.4f GB - WAL size: %.4f GB", db->AllocatedSize(), db->WALSize());
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
}
