#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/gitclone/workload.h"
#include "benchmark/utils/misc.h"

#include "gflags/gflags.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"

#include <iostream>
#include <vector>

/**
 * @brief The Max Payload is statically retrieved from the trace files
 */
using AdapterClass = gitclone::GitCloneWorkload<FilesystemAdapter, gitclone::SystemFileRelation,
                                                gitclone::TemplateFileRelation, gitclone::ObjectFileRelation>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Filesystem Git Clone");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Environment
  PerfEvent e;
  PerfController ctrl;
  SetStackSize(24);

  // LeanStore & Workload initialization
  auto db   = std::make_unique<FilesystemAsDB>(FLAGS_db_path, FLAGS_fs_enable_fsync);
  auto repo = std::make_unique<AdapterClass>(false, db.get());

  // Load initial data
  LOG_INFO("Start loading initial data");
  auto time_start = std::chrono::high_resolution_clock::now();
  repo->LoadDataWithoutBlobRep();
  auto time_end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Complete loading. Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->DatabaseSize());

  // Start benchmarking
  LOG_INFO("Start experiment");
  e.startCounters();
  ctrl.StartPerfRuntime();
  time_start = std::chrono::high_resolution_clock::now();
  repo->InitializationPhaseWithoutBlobRep();
  repo->MainPhaseWithoutBlobRep();
  time_end = std::chrono::high_resolution_clock::now();

  // Benchmark report
  LOG_INFO("Complete benchmark. Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->DatabaseSize());
  e.stopCounters();
  ctrl.StopPerfRuntime();
  uint64_t no_entries = repo->CountObjects();
  e.printReport(std::cout, no_entries);
}
