#include "benchmark/adapters/sql_databases.h"
#include "benchmark/utils/misc.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "fmt/core.h"
#include "share_headers/logger.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <iterator>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("SQLite3 YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(ycsb::SUPPORTED_PAYLOAD_SIZE), std::end(ycsb::SUPPORTED_PAYLOAD_SIZE),
                FLAGS_ycsb_payload_size) == std::end(ycsb::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN("Payload size %lu not supported, check ycsb::SUPPORTED_PAYLOAD_SIZE", FLAGS_ycsb_payload_size);
    return 0;
  }

  // Flags correction
  if (!FLAGS_ycsb_random_payload) { FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size; }

  // Init SQLite
  auto db = std::make_unique<SQLiteDB>(FLAGS_db_path);
  db->ui << "DROP TABLE IF EXISTS YCSB_TABLE;";
  db->ui << fmt::format("CREATE TABLE YCSB_TABLE (my_key INTEGER PRIMARY KEY, my_payload VARCHAR({}));",
                        FLAGS_ycsb_max_payload_size);

  // YCSB loader
  LOG_INFO("Start loading initial data");
  std::vector<uint8_t> payload(FLAGS_ycsb_max_payload_size);
  db->StartTransaction();
  for (auto key = 1UL; key <= FLAGS_ycsb_record_count; key++) {
    auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
    RandomGenerator::GetRandRepetitiveString(payload.data(), 100UL, payload_sz);
    db->ui << "INSERT INTO YCSB_TABLE (my_key, my_payload) VALUES (?, ?)" << key << payload;
  }
  db->CommitTransaction();
  LOG_INFO("Space used: %.4f GB", db->DatabaseSize());

  // Execution
  ZipfGenerator zipf_generator(FLAGS_ycsb_zipf_theta, FLAGS_ycsb_record_count);
  std::atomic<bool> keep_running(true);
  std::atomic<uint64_t> completed_txn(0);
  std::vector<std::thread> threads;
  PerfEvent e;
  PerfController ctrl;

  // statistic thread
  db->latencies.resize(FLAGS_worker_count, {});
  threads.emplace_back(db->StartProfilingThread("sqlite", keep_running, completed_txn));

  // YCSB benchmark
  e.startCounters();
  ctrl.StartPerfRuntime();
  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&, tid = t_id]() {
      std::vector<uint8_t> payload(FLAGS_ycsb_max_payload_size);

      while (keep_running.load()) {
        auto access_key = static_cast<UInteger>(zipf_generator.Rand());

        auto start_time = tsctime::ReadTSC();
        if (RandomGenerator::GetRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
          db->StartTransaction();
          payload.clear();
          db->ui << "SELECT * FROM YCSB_TABLE WHERE my_key = ?;" << access_key >>
            [&]([[maybe_unused]] uint64_t my_key, std::vector<uint8_t> my_payload) {
              assert((my_payload.size() >= FLAGS_ycsb_payload_size) &&
                     (my_payload.size() <= FLAGS_ycsb_max_payload_size));
              payload.assign(my_payload.begin(), my_payload.end());
            };
        } else {
          db->StartTransaction(true);
          auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
          payload.resize(payload_sz);
          RandomGenerator::GetRandRepetitiveString(payload.data(), 100UL, payload_sz);
          db->ui << "UPDATE YCSB_TABLE SET my_payload = ? WHERE my_key = ?;" << payload << access_key;
        }
        db->CommitTransaction();
        db->Report(tid, start_time);

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
  db->LatencyEvaluation();
}
