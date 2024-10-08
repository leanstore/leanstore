#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/gitclone/workload.h"

#include "gflags/gflags.h"
#include "share_headers/perf_event.h"

#include <iostream>

using AdapterClass =
  gitclone::GitCloneWorkload<LeanStoreAdapter, leanstore::schema::InrowBlobRelation<0>,
                             leanstore::schema::InrowBlobRelation<1>, leanstore::schema::InrowBlobRelation<2>>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Git Clone");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Environment
  PerfEvent e;
  leanstore::RegisterSEGFAULTHandler();

  // LeanStore & Workload initialization
  auto db   = std::make_unique<leanstore::LeanStore>();
  auto repo = std::make_unique<AdapterClass>(true, *db);
  repo->system_files.ToggleAppendBiasMode(true);
  repo->template_files.ToggleAppendBiasMode(true);
  repo->objects.ToggleAppendBiasMode(true);

  // Load initial data
  LOG_INFO("Start loading initial data");
  auto time_start = std::chrono::high_resolution_clock::now();
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    repo->LoadInitialData();
    db->CommitTransaction();
  });
  auto time_end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Complete loading. Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->AllocatedSize());

  // Start benchmarking
  LOG_INFO("Start experiment");
  e.startCounters();
  time_start = std::chrono::high_resolution_clock::now();
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    repo->InitializationPhase();
    repo->MainPhase();
    db->CommitTransaction();
  });
  time_end = std::chrono::high_resolution_clock::now();

  // Benchmark report
  LOG_INFO("Complete benchmark. Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->AllocatedSize());
  e.stopCounters();
  uint64_t no_entries = 0;
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    no_entries = repo->CountObjects();
    db->CommitTransaction();
  });
  e.printReport(std::cout, no_entries);
}