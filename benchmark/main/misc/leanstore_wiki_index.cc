#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/rand.h"
#include "benchmark/wikipedia/config.h"
#include "benchmark/wikipedia/schema.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "yyjson.h"

#include <fstream>
#include <iostream>
#include <vector>

using BlobRelation = leanstore::schema::BlobRelation<0>;

/**
 * @brief Exec cmd: ./benchmark/LeanStore_IndexWiki -worker_count=1 -bm_virtual_gb=128 -bm_physical_gb=32
 * -db_path=/dev/nvme0n1p3 -blob_logging_variant={1,2}
 *
 * *IMPORTANT*: Please remove all short strings (which is a corner case in this LeanStore's BLOB impl)
 * FOr the evaluation, all strings whose size is < 100 bytes are removed
 */
auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Wikipedia Read-Only");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  PerfController ctrl;

  // Environment
  PerfEvent e;
  leanstore::RegisterSEGFAULTHandler();

  // LeanStore and indexes initialization
  auto db            = std::make_unique<leanstore::LeanStore>();
  auto wiki_relation = LeanStoreAdapter<BlobRelation>(*db);
  auto bh_index      = LeanStoreAdapter<wiki::BlobStateIndex>(*db);
  auto prefix_index  = LeanStoreAdapter<wiki::BlobPrefixIndex>(*db);
  wiki_relation.ToggleAppendBiasMode(true);
  bh_index.SetComparisonOperator(leanstore::ComparisonOperator::BLOB_HANDLER);

  // Load all data
  std::string line;
  std::vector<std::string> articles;
  std::ifstream articles_file(FLAGS_wiki_articles_path);
  LOG_INFO("Start loading initial data");
  auto time_start = std::chrono::high_resolution_clock::now();
  while (std::getline(articles_file, line)) {
    auto doc  = yyjson_read(line.data(), line.size(), 0);
    auto root = yyjson_doc_get_root(doc);
    auto text = yyjson_obj_get(root, "text");
    auto blob = std::span<uint8_t>(reinterpret_cast<uint8_t *>(const_cast<char *>(yyjson_get_str(text))),
                                   static_cast<uint64_t>(yyjson_get_len(text)));
    articles.emplace_back(yyjson_get_str(text), static_cast<uint64_t>(yyjson_get_len(text)));
    wiki_relation.MiniTransactionWrapper([&]() {
      auto blob_rep = wiki_relation.RegisterBlob(blob, {}, false);
      wiki_relation.InsertRawPayload({articles.size()}, blob_rep);
    });
  }
  auto time_end = std::chrono::high_resolution_clock::now();
  wiki_relation.MiniTransactionWrapper([&]() {
    LOG_DEBUG("Record count: %lu - No articles: %lu", wiki_relation.Count(), articles.size());
    assert(wiki_relation.Count() == articles.size());
  });
  LOG_INFO("Complete loading. Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), db->AllocatedSize());

  /**
   * @brief Blob Handler indexing
   *
   * Statistics:
   * height == 4
   * Number of leaf nodes 21709
   * Avg prefix len of leaf nodes 0.00
   * Avg prefix len of inner nodes 0.00
   * Avg child cnt of inner nodes 24.88
   * Avg size of inner nodes 2928.67 bytes
   * Avg inner slot key_length 92.1496
   */
  if (FLAGS_blob_indexing_variant == 1) {
    // Index construction
    LOG_INFO("Start building Blob Handler Index");
    time_start = std::chrono::high_resolution_clock::now();
    wiki_relation.MiniTransactionWrapper([&]() {
      wiki_relation.Scan({0}, [&](const BlobRelation::Key &key, const BlobRelation &record) {
        auto bh = reinterpret_cast<const leanstore::BlobState *>(record.payload);
        wiki::BlobStateIndex::Key bh_key;
        std::memcpy(&bh_key, bh, bh->MallocSize());
        bh_index.Insert(bh_key, {key.my_key});

        // For Debugging
        if (FLAGS_wiki_index_evaluate_deduplication) {
          auto id   = key.my_key - 1;
          auto blob = std::span<uint8_t>(reinterpret_cast<uint8_t *>(articles[id].data()), articles[id].size());
          [[maybe_unused]] auto found = bh_index.LookUpBlob(blob, [&](const auto &rec) {
            if (key.my_key != rec.article_id) { LOG_WARN("Duplication: Line %lu and %lu", key.my_key, rec.article_id); }
          });
          assert(found);
        }
        return true;
      });
    });

    time_end            = std::chrono::high_resolution_clock::now();
    float bh_index_size = 0;
    bh_index.MiniTransactionWrapper([&]() { bh_index_size = bh_index.RelationSize(); });
    LOG_INFO("Blob Handler Index build complete. Time: %lu ms - Space used: %.4f MB",
             std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), bh_index_size);
  }

  /**
   * @brief Prefix indexing
   *
   * Statistics:
   * height == 4
   * Number of leaf nodes 186961
   * Avg prefix len of leaf nodes 9.73
   * Avg prefix len of inner nodes 6.31
   * Avg child cnt of inner nodes 117.34
   * Avg size of inner nodes 2874.59 bytes
   * Avg inner slot key_length 5.6235
   */
  if (FLAGS_blob_indexing_variant == 2) {
    auto rejection = 0;
    // Index construction
    LOG_INFO("Start building Prefix Index");
    time_start = std::chrono::high_resolution_clock::now();
    wiki_relation.MiniTransactionWrapper([&]() {
      wiki_relation.Scan({0}, [&](const BlobRelation::Key &key, const BlobRelation &record) {
        auto blob_handler = reinterpret_cast<const leanstore::BlobState *>(record.payload);
        db->LoadBlob(
          blob_handler,
          [&](std::span<const uint8_t> blob) {
            wiki::BlobPrefixIndex::Key prefix_key;
            prefix_key.prefix_length = std::min(blob.size(), wiki::INDEX_PREFIX_LENGTH);
            std::memcpy(prefix_key.value, blob.data(), prefix_key.prefix_length);
            if (FLAGS_wiki_index_evaluate_deduplication) {
              if (prefix_index.LookUp(prefix_key, []([[maybe_unused]] const auto &rec) {})) { rejection++; }
            }
            wiki::BlobPrefixIndex payload;
            payload.article_id = key.my_key;
            leanstore::BlobState::CalculateSHA256(payload.digest, blob);
            prefix_index.Insert(prefix_key, payload);
          },
          true);
        return true;
      });
    });
    time_end                = std::chrono::high_resolution_clock::now();
    float prefix_index_size = 0;
    prefix_index.MiniTransactionWrapper([&]() { prefix_index_size = prefix_index.RelationSize(); });
    LOG_INFO("Prefix Index build complete. Time: %lu ms - Space used: %.4f MB",
             std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count(), prefix_index_size);
    if (FLAGS_wiki_index_evaluate_deduplication) { LOG_INFO("Prefix Index: Reject %u records", rejection); }
  }

  // Random Index Look-up Workload
  std::atomic<bool> keep_running(true);
  ctrl.StartPerfRuntime();
  db->StartProfilingThread();
  e.startCounters();

  // Main execution
  auto not_found = 0UL;
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&]() {
      wiki::BlobPrefixIndex::Key prefix_key;
      bool found = false;

      while (keep_running.load()) {
        auto id   = RandomGenerator::GetRandU64(0, articles.size());
        auto blob = std::span<uint8_t>(reinterpret_cast<uint8_t *>(articles[id].data()), articles[id].size());

        db->StartTransaction();
        switch (FLAGS_blob_indexing_variant) {
          case 1:
            found = bh_index.LookUpBlob(blob, [&](const auto &rec) { id = rec.article_id; });
            Ensure(found);
            break;
          case 2:
            found = false;
            uint8_t digest[wiki::SHA2_LENGTH];
            prefix_key.prefix_length = std::min(blob.size(), wiki::INDEX_PREFIX_LENGTH);
            std::memcpy(prefix_key.value, blob.data(), prefix_key.prefix_length);
            /**
             * @brief Only index the prefix of the requested doc,
             *  call extra SHA-256 to validate if the found doc is what we we are looking for
             *  45% percentile are bigger than wiki::INDEX_PREFIX_LENGTH
             *  Average size of object > wiki::INDEX_PREFIX_LENGTH: 4393 bytes
             */
            if (blob.size() > wiki::INDEX_PREFIX_LENGTH) {
              // Calculate SHA-256 for equality lookup
              leanstore::BlobState::CalculateSHA256(digest, blob);
            }
            prefix_index.LookUp(prefix_key, [&](const auto &rec) {
              id    = rec.article_id;
              found = true;
              if (blob.size() > wiki::INDEX_PREFIX_LENGTH) {
                found = (std::memcmp(rec.digest, digest, wiki::SHA2_LENGTH) == 0);
              }
            });
            // It's possible that a row is rejected because of duplicated prefix,
            //  so we shouldn't expect Ensure(found) here
            if (!found) { not_found++; }
            break;
          default: UnreachableCode();
        }
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_wiki_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Total completed txn %lu - Space used: %.4f GB - WAL size: %.4f GB - Not found items %lu",
           leanstore::statistics::total_txn_completed.load(), db->AllocatedSize(), db->WALSize(), not_found);
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_txn_completed);
}