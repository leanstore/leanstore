#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tatp/workload.h"
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
  gflags::SetUsageMessage("Leanstore TATP Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  PerfController ctrl;
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore
  auto db   = std::make_unique<leanstore::LeanStore>();
  auto tatp = std::make_unique<tatp::TATPWorkload<LeanStoreAdapter>>(FLAGS_tatp_subcriber_count, *db);
  tatp->subcriber.ToggleAppendBiasMode(true);

  // Load initial data
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    tatp->LoadSubcribers();
    db->CommitTransaction();
  });
  for (auto s_id = 1UL; s_id <= FLAGS_tatp_subcriber_count; s_id++) {
    db->worker_pool.ScheduleAsyncJob(s_id % FLAGS_worker_count, [&, s_id]() {
      db->StartTransaction();
      tatp->LoadSubcriberData(s_id);
      db->CommitTransaction();
    });
    if (s_id % 100000 == 0) { LOG_DEBUG("Complete %lu subcribers", s_id); }
  }
  db->worker_pool.JoinAll();
  LOG_INFO("Space used: %.4f GB", db->AllocatedSize());
  auto initial_wal_size = db->WALSize();

#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    auto s_cnt       = tatp->subcriber.Count();
    auto s_index_cnt = tatp->subcriber_nbr.Count();
    LOG_DEBUG("Subcriber count: %lu - Subcriber index count: %lu", s_cnt, s_index_cnt);
    assert(s_cnt == FLAGS_tatp_subcriber_count);
    assert(s_cnt == s_index_cnt);

    auto ai_cnt = tatp->access_info.Count();
    auto sf_cnt = tatp->special_facility.Count();
    LOG_DEBUG("Access info count: %lu - Special facility count: %lu", ai_cnt, sf_cnt);
    assert((s_cnt * 2 <= ai_cnt) && (s_cnt * 3 >= ai_cnt));
    assert((s_cnt * 2 <= sf_cnt) && (s_cnt * 3 >= sf_cnt));

    auto cf_cnt = tatp->call_forwarding.Count();
    LOG_DEBUG("Call forwarding count: %lu", cf_cnt);
    assert((sf_cnt <= cf_cnt) && (cf_cnt <= sf_cnt * 2));
    db->CommitTransaction();
  });
#endif

  // TPC-C execution
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    db->worker_pool.ScheduleAsyncJob(t_id, [&]() {
      while (keep_running.load()) {
        db->StartTransaction();
        tatp->ExecuteTransaction();
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tatp_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Space used: %.4f GB - WAL size: %.4f GB", db->AllocatedSize(), db->WALSize() - initial_wal_size);
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_committed_txn);
}