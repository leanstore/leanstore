#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/filebench/webserver/config.h"
#include "benchmark/filebench/webserver/schema.h"
#include "benchmark/utils/rand.h"

#include "share_headers/config.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <random>

#define RELATION_LOADS_BLOB(relation, key, likely_grow)                           \
  ({                                                                              \
    auto file_size = GetFileSize();                                               \
    uint8_t payload[file_size];                                                   \
    RandomGenerator::GetRandString(payload, file_size);                           \
    auto r_span = (relation).RegisterBlob({payload, file_size}, {}, likely_grow); \
    (relation).InsertRawPayload({(key)}, r_span);                                 \
  })

namespace filebench::webserver {

/**
 * @brief The FileBench - WebServer implementation
 */
template <template <typename> class AdapterType, class HTMLRelation, class LogRelation>
struct WebserverWorkload {
  AdapterType<HTMLRelation> html_relation;
  AdapterType<LogRelation> log_relation;

  UInteger no_files;
  std::random_device generator;
  std::gamma_distribution<> rand;

  // Run-time env
  inline static thread_local uint8_t ws_payload[MAX_FILE_SIZE];
  inline static thread_local Integer ws_thread_id         = 0;
  inline static std::atomic<Integer> ws_thread_id_counter = 1;

  template <typename... Params>
  explicit WebserverWorkload(UInteger number_of_html_files, Params &&...params)
      : html_relation(AdapterType<HTMLRelation>(std::forward<Params>(params)...)),
        log_relation(AdapterType<LogRelation>(std::forward<Params>(params)...)),
        no_files(number_of_html_files) {
    Ensure(FLAGS_webserver_log_entry_size <= MAX_LOG_ENTRY_SIZE);
    rand = std::gamma_distribution<>(GAMMA_DIST_GAMMA, MEAN_FILE_SIZE / GAMMA_DIST_GAMMA);
  }

  void InitializeThread() {
    if (ws_thread_id > 0) { return; }
    ws_thread_id = ws_thread_id_counter++;
  }

  void LoadHTMLData(const tbb::blocked_range<Integer> &range) {
    for (auto key = range.begin(); key < range.end(); key++) { RELATION_LOADS_BLOB(html_relation, key, false); }
  }

  void LoadLogFiles() {
    for (auto t_id = 1; t_id <= static_cast<int>(FLAGS_worker_count); t_id++) {
      RELATION_LOADS_BLOB(log_relation, t_id, true);
    }
  }

  /**
   * @brief Web server transaction
   *      1. Read FLAGS_webserver_file_read_per_txn HTML files
   *      2. Append new log entry to the log file
   */
  void ExecuteTransaction() {
    // 1. Read HTML files
    uint8_t blob_rep[MAX_BLOB_REP_SIZE];
    uint64_t blob_rep_size = 0;

    for (u32 i = 0; i < FLAGS_webserver_file_read_per_txn; i++) {
      auto access_key = UniformRand(1, no_files);
      auto found      = html_relation.LookUp({access_key}, [&](const HTMLRelation &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<HTMLRelation &>(rec).payload.Data(), blob_rep_size);
      });
      Ensure(found);

      html_relation.LoadBlob(
        blob_rep,
        [&](std::span<const u8> blob_data) {
          Ensure(blob_data.size() <= MAX_FILE_SIZE);
          std::memcpy(ws_payload, blob_data.data(), blob_data.size());
        },
        false);
    }

    // 2. Append Log entry
    Ensure(ws_thread_id > 0);
    RandomGenerator::GetRandString(ws_payload, FLAGS_webserver_log_entry_size);
    log_relation.LookUp({ws_thread_id}, [&](const LogRelation &rec) {
      blob_rep_size = rec.PayloadSize();
      std::memcpy(blob_rep, const_cast<LogRelation &>(rec).payload.Data(), blob_rep_size);
    });
    log_relation.RegisterBlob({ws_payload, FLAGS_webserver_log_entry_size}, {blob_rep, blob_rep_size}, true);
  }

  // -------------------------------------------------------------------------------------
 private:
  auto GetFileSize() -> uint32_t {
    return static_cast<uint32_t>(std::round(rand(generator))) % (MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
  }
};

}  // namespace filebench::webserver