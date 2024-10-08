#include "benchmark/adapters/wiredtiger_adapter.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <ranges>
#include <string>
#include <thread>
#include <vector>

using KeyValueWT =
  ycsb::YCSBWorkloadNoBlobRep<WiredTigerAdapter, ycsb::Relation<Varchar<ycsb::BLOB_NORMAL_PAYLOAD>, 0>>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("WiredTiger YCSB");
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
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE))
                    uint8_t[std::max(FLAGS_ycsb_max_payload_size, GLOBAL_BLOCK_SIZE)]());
  }

  // Setup env
  PerfEvent e;
  PerfController ctrl;

  // Initialize WiredTigerDB
  auto db = WiredTigerDB();
  db.PrepareThread();
  auto ycsb = std::make_unique<KeyValueWT>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                           payloads, db);

  // YCSB loader
  db.StartTransaction();
  ycsb->LoadInitialData(0, tbb::blocked_range<Integer>(1, FLAGS_ycsb_record_count + 1));
  db.CommitTransaction();
#ifdef DEBUG
  db.StartTransaction();
  LOG_DEBUG("Record count: %lu", ycsb->CountEntries());
  assert(ycsb->CountEntries() == FLAGS_ycsb_record_count);
  db.CommitTransaction();
#endif

  // YCSB profiling
  std::atomic<bool> keep_running(true);
  std::vector<std::thread> threads;
  std::atomic<uint64_t> completed_txn(0);
  threads.emplace_back(db.StartProfilingThread("wiredtiger", keep_running, completed_txn));

  // YCSB execution
  ctrl.StartPerfRuntime();
  e.startCounters();
  db.latencies.resize(FLAGS_worker_count, {});
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    threads.emplace_back([&, tid = w_id]() {
      db.PrepareThread();
      while (keep_running.load()) {
        auto start_time = tsctime::ReadTSC();
        db.StartTransaction(ycsb->NextTransactionArrivalTime([&]() {}));
        ycsb->ExecuteTransaction(tid);
        db.CommitTransaction();
        db.Report(tid, start_time);
        completed_txn++;
      }
      db.CloseSession();
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  e.stopCounters();
  for (auto &t : threads) { t.join(); }
  e.printReport(std::cout, db.total_txn_completed.load());
  db.LatencyEvaluation();
}
