#include "benchmark/adapters/config.h"
#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/ycsb/workload.h"

#include "gflags/gflags.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"

#include <iostream>
#include <vector>

using BlobRelation = leanstore::schema::BlobRelation<0>;

DEFINE_uint32(alloc_ratio, 80, "Allocation ratio");
DEFINE_bool(with_tail_extent, true, "Whether to enable tail extent or not");

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

  // LeanStore & Workload initialization
  auto db       = std::make_unique<leanstore::LeanStore>();
  auto relation = LeanStoreAdapter<BlobRelation>(*db);

  // profiling
  std::atomic<bool> keep_running(true);
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  // Execution
  std::atomic<uint64_t> uid = 1;

  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id]() {
      uint8_t blob_rep[ycsb::MAX_BLOB_REPRESENT_SIZE];
      uint64_t blob_rep_size = 0;

      while (keep_running.load()) {
        db->StartTransaction();

        auto is_alloc = RandomGenerator::GetRandU64(0, 100) <= FLAGS_alloc_ratio;
        if (is_alloc) {
          // Generate key and payload
          auto r_key      = uid.fetch_add(1);
          auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
          RandomGenerator::GetRandRepetitiveString(payloads[w_id].get(), 100UL, payload_sz);

          // Register BLOB
          auto new_blob_rep = relation.RegisterBlob({payloads[w_id].get(), payload_sz}, {}, !FLAGS_with_tail_extent);
          relation.InsertRawPayload({r_key}, new_blob_rep);
        } else {
          // Random key to be deleted
          auto r_key = RandomGenerator::GetRandU64(1, uid.load());
          auto found = relation.LookUp({r_key}, [&](const auto &rec) {
            blob_rep_size = rec.PayloadSize();
            std::memcpy(blob_rep, rec.payload, rec.PayloadSize());
          });

          // Delete BLOB
          if (found) {
            relation.RemoveBlob(blob_rep);
            relation.Erase({r_key});
          }
        }

        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Total completed txn %lu - Space used: %.4f GB - WAL size: %.4f GB",
           leanstore::statistics::total_txn_completed.load(), db->AllocatedSize(), db->WALSize());
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_txn_completed);
}