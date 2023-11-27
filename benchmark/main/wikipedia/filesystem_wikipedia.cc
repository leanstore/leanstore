#include "benchmark/adapters/config.h"
#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/wikipedia/config.h"
#include "benchmark/wikipedia/workload.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("FileSystem Wikipedia");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  PerfController ctrl;

  // Environment
  remove(FLAGS_db_path.c_str());
  auto db   = std::make_unique<FilesystemAsDB>(FLAGS_db_path, FLAGS_fs_enable_fsync);
  auto wiki = std::make_unique<wiki::WikipediaReadOnly<FilesystemAdapter, wiki::FileRelation>>(false, db.get());

  // YCSB loader
  LOG_INFO("Start loading initial data");
  auto time_start = std::chrono::high_resolution_clock::now();
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, 10);
  tbb::parallel_for(tbb::blocked_range<UInteger>(1, wiki->RequiredNumberOfEntries() + 1),
                    [&](const tbb::blocked_range<UInteger> &range) { wiki->LoadInitialDataWithoutBlobRep(range); });
  auto time_end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Complete loading. Off-row records: %u - In-row records: %u", wiki->offrow_count.load(),
           wiki->inrow_count.load());
  LOG_INFO("Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->DatabaseSize());
#ifdef DEBUG
  LOG_DEBUG("Record count: %lu", wiki->CountEntries());
  assert(wiki->CountEntries() == wiki->RequiredNumberOfEntries());
  assert(wiki->CountEntries() == wiki->inrow_count.load());
#endif

  // Drop cache before the experiment
  if (FLAGS_wiki_clear_cache_before_expr) { db->DropCache(); }

  // Execution
  std::atomic<bool> keep_running(true);
  std::atomic<uint64_t> completed_txn = 0;
  std::vector<std::thread> threads;
  PerfEvent e;

  // statistic thread
  threads.emplace_back(db->StartProfilingThread(keep_running, completed_txn));

  // execution
  e.startCounters();
  ctrl.StartPerfRuntime();
  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&]() {
      while (keep_running.load()) {
        wiki->ExecuteTxnNoBlobRepresent();
        completed_txn++;
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wiki_exec_seconds));
  keep_running = false;
  e.stopCounters();
  ctrl.StopPerfRuntime();

  for (auto &t : threads) { t.join(); }
  e.printReport(std::cout, db->total_txn_completed);
}