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
  auto db = std::make_unique<MySQLDB>();
  db->PrepareThread();
  db->StartTransaction();
  std::unique_ptr<sql::Statement> stmt(db->conn->createStatement());
  stmt->execute("DROP TABLE IF EXISTS YCSB_TABLE;");
  if (FLAGS_ycsb_max_payload_size == ycsb::BLOB_NORMAL_PAYLOAD) {
    stmt->execute(fmt::format("CREATE TABLE YCSB_TABLE (my_key INTEGER PRIMARY KEY, my_payload VARCHAR({}));",
                              FLAGS_ycsb_max_payload_size));
  } else {
    stmt->execute("CREATE TABLE YCSB_TABLE (my_key INTEGER PRIMARY KEY, my_payload MEDIUMBLOB);");
  }
  stmt->execute("ALTER TABLE YCSB_TABLE COMPRESSION='None';");
  db->CommitTransaction();

  // YCSB loader
  LOG_INFO("Start loading initial data");
  tbb::global_control c(tbb::global_control::max_allowed_parallelism, FLAGS_worker_count);
  tbb::parallel_for(
    tbb::blocked_range<Integer>(1, FLAGS_ycsb_record_count + 1), [&](const tbb::blocked_range<Integer> &range) {
      std::string payload;
      payload.resize(FLAGS_ycsb_max_payload_size);
      std::unique_ptr<sql::PreparedStatement> pstm = nullptr;

      db->StartTransaction();
      for (auto key = range.begin(); key < range.end(); key++) {
        auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
        RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);
        pstm.reset(db->conn->prepareStatement("INSERT INTO YCSB_TABLE (my_key, my_payload) VALUES (?, ?)"));
        pstm->setUInt64(1, key);
        pstm->setString(2, payload.substr(0, payload_sz));
        pstm->execute();
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

  // // statistic thread
  threads.emplace_back(db->StartProfilingThread("mysql", keep_running, completed_txn));

  // YCSB benchmark
  e.startCounters();
  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&]() {
      // Worker initialization
      db->PrepareThread();
      std::string payload;
      payload.resize(FLAGS_ycsb_max_payload_size);
      std::unique_ptr<sql::PreparedStatement> pstm = nullptr;

      // Main workload
      while (keep_running.load()) {
        auto access_key = static_cast<UInteger>(zipf_generator.Rand());

        db->StartTransaction();
        if (RandomGenerator::GetRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
          pstm.reset(db->conn->prepareStatement("SELECT * FROM YCSB_TABLE WHERE my_key = ?;"));
          pstm->setUInt64(1, access_key);
          auto res = pstm->executeQuery();
          res->next();
        } else {
          auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
          RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);
          pstm.reset(db->conn->prepareStatement("UPDATE YCSB_TABLE SET my_payload = ? WHERE my_key = ?;"));
          pstm->setUInt64(2, access_key);
          pstm->setString(1, payload.substr(0, payload_sz));
          pstm->execute();
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
}
