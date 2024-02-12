#include "benchmark/adapters/filesystem_adapter.h"
#include "benchmark/utils/shared_schema.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

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

using YCSB120B = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 120>>;
using YCSB4K   = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 4096>>;
using YCSB100K = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 102400>>;
using YCSB1M   = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 1048576>>;
using YCSB10M  = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 10485760>>;
using YCSB1G   = ycsb::YCSBWorkloadNoBlobRep<FilesystemAdapter, benchmark::FileRelation<0, 1073741824>>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("FileSystem YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(ycsb::SUPPORTED_PAYLOAD_SIZE), std::end(ycsb::SUPPORTED_PAYLOAD_SIZE),
                FLAGS_ycsb_payload_size) == std::end(ycsb::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN("Payload size %lu not supported, check ycsb::SUPPORTED_PAYLOAD_SIZE", FLAGS_ycsb_payload_size);
    return 0;
  }

  // Correct Flags & initialize worker-local payloads
  if (!FLAGS_ycsb_random_payload) { FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size; }
  ycsb::WorkerLocalPayloads payloads(FLAGS_worker_count);
  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE)) uint8_t[FLAGS_ycsb_max_payload_size]());
  }

  // Init Filesystem and YCSB
  SetStackSize(24);
  remove(FLAGS_db_path.c_str());
  auto db = std::make_unique<FilesystemAsDB>(FLAGS_db_path, FLAGS_fs_enable_fsync);
  std::unique_ptr<ycsb::YCSBWorkloadInterface> ycsb;
  switch (FLAGS_ycsb_max_payload_size) {
    case ycsb::BLOB_NORMAL_PAYLOAD:
      ycsb = std::make_unique<YCSB120B>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                        payloads, db.get());
      break;
    case 4096:
      ycsb = std::make_unique<YCSB4K>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                      payloads, db.get());
      break;
    case 102400:
      ycsb = std::make_unique<YCSB100K>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                        payloads, db.get());
      break;
    case 1048576:
      ycsb = std::make_unique<YCSB1M>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                      payloads, db.get());
      break;
    case 10485760:
      ycsb = std::make_unique<YCSB10M>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                       payloads, db.get());
      break;
    case 1073741824:
      ycsb = std::make_unique<YCSB1G>(FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta, false,
                                      payloads, db.get());
      break;
    default: UnreachableCode();
  }

  // YCSB loader
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);
  tbb::parallel_for(tbb::blocked_range<Integer>(1, FLAGS_ycsb_record_count + 1),
                    [&](const tbb::blocked_range<Integer> &range) {
                      auto w_id = rand() % FLAGS_worker_count;
                      ycsb->LoadInitialData(w_id, range);
                    });
  LOG_INFO("Space used: %.4f GB", db->DatabaseSize());
#ifdef DEBUG
  LOG_DEBUG("Record count: %lu", ycsb->CountEntries());
  assert(ycsb->CountEntries() == FLAGS_ycsb_record_count);
#endif

  // YCSB execution
  PerfEvent e;
  PerfController ctrl;
  std::atomic<bool> keep_running(true);
  std::atomic<uint64_t> completed_txn = 0;
  std::vector<std::thread> threads;

  // statistic thread
  threads.emplace_back(db->StartProfilingThread(keep_running, completed_txn));

  // execution
  ctrl.StartPerfRuntime();
  e.startCounters();
  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&, t_id]() {
      while (keep_running.load()) {
        ycsb->ExecuteTransaction(t_id);
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