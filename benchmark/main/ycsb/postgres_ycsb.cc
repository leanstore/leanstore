#include "benchmark/adapters/sql_databases.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "fmt/core.h"
#include "share_headers/logger.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <iterator>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Postgres YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(ycsb::SUPPORTED_PAYLOAD_SIZE), std::end(ycsb::SUPPORTED_PAYLOAD_SIZE),
                FLAGS_ycsb_payload_size) == std::end(ycsb::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN("Payload size %lu not supported, check ycsb::SUPPORTED_PAYLOAD_SIZE", FLAGS_ycsb_payload_size);
    return 0;
  }

  // Flags correction
  if (!FLAGS_ycsb_random_payload) { FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size; }

  // Init Postgres
  auto db = std::make_unique<PostgresDB>();
  db->StartTransaction();
  db->txn->exec0("DROP TABLE IF EXISTS YCSB_TABLE;");
  db->txn->exec0("CREATE TABLE YCSB_TABLE (my_key INTEGER PRIMARY KEY, my_payload TEXT);");
  db->txn->exec0("ALTER TABLE YCSB_TABLE ALTER COLUMN my_payload SET STORAGE EXTERNAL;");
  db->CommitTransaction();

  // YCSB loader
  LOG_INFO("Start loading initial data");
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);
  tbb::parallel_for(
    tbb::blocked_range<Integer>(1, FLAGS_ycsb_record_count + 1), [&](const tbb::blocked_range<Integer> &range) {
      std::basic_string<std::byte> payload;
      payload.resize(FLAGS_ycsb_max_payload_size, std::byte{0});

      db->StartTransaction();
      for (auto key = range.begin(); key < range.end(); key++) {
        auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
        RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);
        db->txn->exec_params("INSERT INTO YCSB_TABLE (my_key, my_payload) VALUES ($1, $2)", key, payload);
      }
      db->CommitTransaction();
    });
  LOG_INFO("Space used: %.4f GB", db->DatabaseSize());

  // Execution
  ZipfGenerator zipf_generator(FLAGS_ycsb_zipf_theta, FLAGS_ycsb_record_count);
  std::atomic<bool> keep_running(true);
  std::atomic<uint64_t> completed_txn(0);
  std::vector<std::thread> threads;
  PerfEvent e;

  // statistic thread
  threads.emplace_back(db->StartProfilingThread("postgres", keep_running, completed_txn));

  // YCSB benchmark
  e.startCounters();
  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&]() {
      std::basic_string<std::byte> payload;
      payload.resize(FLAGS_ycsb_max_payload_size, std::byte{0});

      while (keep_running.load()) {
        auto access_key = static_cast<UInteger>(zipf_generator.Rand());

        db->StartTransaction();
        if (RandomGenerator::GetRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
          auto res = db->txn->exec_params("SELECT * FROM YCSB_TABLE WHERE my_key = $1", access_key);
        } else {
          auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
          RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);
          db->txn->exec_params("UPDATE YCSB_TABLE SET my_payload = $1 WHERE my_key = $2;", payload, access_key);
        }
        db->CommitTransaction();

        completed_txn++;
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;
  e.stopCounters();

  for (auto &t : threads) { t.join(); }
  e.printReport(std::cout, db->total_txn_completed.load());
  LOG_INFO("Space used: %.4f GB", db->DatabaseSize());
}
