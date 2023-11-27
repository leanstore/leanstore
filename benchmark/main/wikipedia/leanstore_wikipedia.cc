#include "benchmark/wikipedia/config.h"
#include "benchmark/wikipedia/workload.h"
#include "leanstore/leanstore.h"

#include "gflags/gflags.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <iostream>
#include <vector>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Wikipedia Read-Only");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Environment
  PerfEvent e;
  PerfController ctrl;
  leanstore::RegisterSEGFAULTHandler();

  // LeanStore & Workload initialization
  auto db = std::make_unique<leanstore::LeanStore>();
  auto wiki =
    std::make_unique<wiki::WikipediaReadOnly<LeanStoreAdapter, leanstore::schema::InrowBlobRelation<0>>>(true, *db);
  wiki->relation.ToggleAppendBiasMode(true);

  // Setup env
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Wikipedia loader
  LOG_INFO("Start loading initial data");
  auto time_start                   = std::chrono::high_resolution_clock::now();
  std::atomic<UInteger> w_id_loader = 0;
  tbb::parallel_for(tbb::blocked_range<UInteger>(1, wiki->RequiredNumberOfEntries() + 1),
                    [&](const tbb::blocked_range<UInteger> &range) {
                      auto w_id = (++w_id_loader) % FLAGS_worker_count;
                      db->worker_pool.ScheduleAsyncJob(w_id, [&, range]() {
                        db->StartTransaction();
                        wiki->LoadInitialData(range);
                        db->CommitTransaction();
                      });
                    });
  db->worker_pool.JoinAll();
  auto time_end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Complete loading. Off-row records: %u - In-row records: %u", wiki->offrow_count.load(),
           wiki->inrow_count.load());
  LOG_INFO("Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->AllocatedSize());
#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    LOG_DEBUG("Record count: %lu", wiki->CountEntries());
    assert(wiki->CountEntries() == wiki->RequiredNumberOfEntries());
    db->CommitTransaction();
  });
#endif

  // Drop cache before the experiment
  if (FLAGS_wiki_clear_cache_before_expr) { db->DropCache(); }

  // Wikipedia Read-only Workload
  std::atomic<bool> keep_running(true);
  ctrl.StartPerfRuntime();
  db->StartProfilingThread();
  e.startCounters();

  // YCSB execution
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&]() {
      while (keep_running.load()) {
        db->StartTransaction();
        wiki->ExecuteReadOnlyTxn();
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wiki_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Total completed txn %lu - Space used: %.4f GB - WAL size: %.4f GB",
           leanstore::statistics::total_txn_completed.load(), db->AllocatedSize(), db->WALSize());
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_txn_completed);
}