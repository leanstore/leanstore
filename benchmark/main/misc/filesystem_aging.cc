#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/shared_schema.h"
#include "benchmark/ycsb/workload.h"

#include "gflags/gflags.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"

#include <iostream>
#include <vector>

using BlobRelation = leanstore::schema::BlobRelation<0>;

DEFINE_uint32(alloc_ratio, 80, "Allocation ratio");

/**
 * @brief 80% BLOB allocation, 20% Deletion
 * Size randomly from 4MB - 100MB
 *
 * Virtual 64GB, buffer size 32GB as well
 */
auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("LeanStore Aging");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Setup worker-local payload
  ycsb::WorkerLocalPayloads payloads(FLAGS_worker_count);
  if (!FLAGS_ycsb_random_payload) { FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size; }
  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE)) uint8_t[FLAGS_ycsb_max_payload_size]());
  }

  // Environment
  PerfEvent e;
  PerfController ctrl;
  leanstore::RegisterSEGFAULTHandler();

  // Initialization
  remove(FLAGS_db_path.c_str());
  auto db       = std::make_unique<FilesystemAsDB>(FLAGS_db_path, FLAGS_fs_enable_fsync, true);
  auto relation = FilesystemAdapter<benchmark::FileRelation<0, 120>>(db.get());

  // profiling
  std::atomic<bool> keep_running(true);
  std::atomic<uint64_t> completed_txn = 0;
  std::vector<std::thread> threads;

  // statistic & profiling
  threads.emplace_back(db->StartProfilingThread(keep_running, completed_txn));
  ctrl.StartPerfRuntime();
  e.startCounters();

  // Execution
  std::atomic<uint64_t> uid = 1;
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    threads.emplace_back([&, w_id]() {
      while (keep_running.load()) {
        auto is_alloc = RandomGenerator::GetRandU64(0, 100) <= FLAGS_alloc_ratio;
        if (is_alloc) {
          // Generate key and payload
          auto r_key      = uid.fetch_add(1);
          auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
          RandomGenerator::GetRandRepetitiveString(payloads[w_id].get(), 100UL, payload_sz);
          relation.InsertRawPayload({r_key}, {payloads[w_id].get(), payload_sz});
        } else {
          // Random key to be deleted
          auto r_key = RandomGenerator::GetRandU64(1, uid.load());
          relation.Erase({r_key});
        }
        completed_txn++;
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;
  e.stopCounters();
  ctrl.StopPerfRuntime();

  for (auto &t : threads) { t.join(); }
  e.printReport(std::cout, db->total_txn_completed.load());
}