#include "benchmark/filebench/webserver/config.h"
#include "benchmark/filebench/webserver/workload.h"
#include "leanstore/leanstore.h"

#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Webserver");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto db        = std::make_unique<leanstore::LeanStore>();
  auto webserver = std::make_unique<filebench::webserver::WebserverWorkload<
    LeanStoreAdapter, filebench::webserver::BlobStateRelation<0>, filebench::webserver::BlobStateRelation<1>>>(
    FLAGS_webserver_no_files, *db);

  // WebServer loader
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    webserver->LoadLogFiles();
    db->CommitTransaction();
  });

  std::atomic<UInteger> w_id_loader = 0;
  tbb::parallel_for(tbb::blocked_range<Integer>(1, FLAGS_webserver_no_files + 1),
                    [&](const tbb::blocked_range<Integer> &range) {
                      auto w_id = (++w_id_loader) % FLAGS_worker_count;
                      db->worker_pool.ScheduleAsyncJob(w_id, [&, range]() {
                        db->StartTransaction();
                        webserver->LoadHTMLData(range);
                        db->CommitTransaction();
                      });
                    });
  db->worker_pool.JoinAll();
  LOG_INFO("Space used: %.4f GB", db->AllocatedSize());
#ifdef DEBUG
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    LOG_DEBUG("File count: %lu - Log files count: %lu", webserver->html_relation.Count(),
              webserver->log_relation.Count());
    assert(webserver->html_relation.Count() == static_cast<u64>(FLAGS_webserver_no_files));
    assert(webserver->log_relation.Count() == FLAGS_worker_count);
    db->CommitTransaction();
  });
#endif

  // WebServer execution
  std::atomic<bool> keep_running(true);
  db->StartProfilingThread();

  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    db->worker_pool.ScheduleAsyncJob(t_id, [&]() {
      webserver->InitializeThread();

      while (keep_running.load()) {
        db->StartTransaction();
        webserver->ExecuteTransaction();
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_webserver_exec_seconds));
  keep_running = false;
  db->Shutdown();
  LOG_INFO("Space used: %.4f GB - WAL size: %.4f GB", db->AllocatedSize(), db->WALSize());
}
