#include "leanstore/leanstore.h"
#include "leanstore/config.h"
#include "storage/btree/tree.h"

#include "fmt/core.h"
#include "share_headers/mem_usage.h"

#include <sys/sysinfo.h>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <new>

namespace leanstore {

LeanStore::LeanStore()
    : fp_manager(std::make_unique<storage::FreePageManager>()),
      buffer_pool(std::make_unique<buffer::BufferManager>(is_running, fp_manager.get())),
      log_manager(std::make_unique<recovery::LogManager>(is_running)),
      transaction_manager(std::make_unique<transaction::TransactionManager>(buffer_pool.get(), log_manager.get())),
      blob_manager(std::make_unique<storage::blob::BlobManager>(buffer_pool.get())),
      worker_pool(is_running) {
  // Necessary dirty works
  worker_pool.ScheduleSyncJob(0, [&]() { buffer_pool->AllocMetadataPage(); });
  all_buffer_pools.push_back(buffer_pool.get());
  // Group-commit
  StartGroupCommitThread();
  //  Page provider threads
  if (FLAGS_page_provider_thread > 0) { buffer_pool->RunPageProviderThreads(); }
}

LeanStore::~LeanStore() {
  Shutdown();
  start_profiling = false;
}

void LeanStore::Shutdown() {
  worker_pool.Stop();
  Ensure(is_running == false);
  for (size_t w_id = 0; w_id <= FLAGS_worker_count; w_id++) {
    struct exmap_action_params params = {
      .interface = static_cast<u16>(w_id),
      .iov_len   = 0,
      .opcode    = EXMAP_OP_RM_SD,
      .flags     = 0,
    };
    ioctl(buffer_pool->exmapfd_, EXMAP_IOCTL_ACTION, &params);
  }
  /** It's possible that these two special threads are already completed before calling join */
  if (group_committer.joinable()) { group_committer.join(); }
  if (stat_collector.joinable()) { stat_collector.join(); }
}

// -------------------------------------------------------------------------------------
void LeanStore::RegisterTable(const std::type_index &relation) {
  assert(indexes.find(relation) == indexes.end());
  assert(FLAGS_worker_count > 0);
  worker_pool.ScheduleSyncJob(0, [&]() {
    transaction_manager->StartTransaction(leanstore::transaction::Transaction::Type::SYSTEM);
    indexes.try_emplace(relation, std::make_unique<storage::BTree>(buffer_pool.get(), false));
    CommitTransaction();
  });
}

auto LeanStore::RetrieveIndex(const std::type_index &relation) -> KVInterface * {
  assert(indexes.find(relation) != indexes.end());
  return indexes.at(relation).get();
}

void LeanStore::StartTransaction(bool read_only, Transaction::Mode tx_mode, const std::string &tx_isolation_level) {
  transaction_manager->StartTransaction(Transaction::Type::USER,
                                        transaction::TransactionManager::ParseIsolationLevel(tx_isolation_level),
                                        tx_mode, read_only);
}

void LeanStore::CommitTransaction() {
  transaction_manager->CommitTransaction();
  blob_manager->UnloadAllBlobs();
}

void LeanStore::AbortTransaction() {
  transaction_manager->AbortTransaction();
  blob_manager->UnloadAllBlobs();
}

// -------------------------------------------------------------------------------------
auto LeanStore::AllocatedSize() -> float { return static_cast<float>(buffer_pool->alloc_cnt_.load() * PAGE_SIZE) / GB; }

auto LeanStore::WALSize() -> float {
  auto [_, db_offset_limit] = buffer_pool->GetWalInfo();
  auto wal_size_in_bytes    = gct->w_offset_ - db_offset_limit;
  return static_cast<float>(wal_size_in_bytes) / GB;
}

auto LeanStore::DBSize() -> float {
  return static_cast<float>((buffer_pool->alloc_cnt_.load() - statistics::storage::free_size) * PAGE_SIZE) / GB;
}

void LeanStore::DropCache() {
  worker_pool.ScheduleSyncJob(0, [&]() {
    LOG_INFO("Dropping the cache");
    while (buffer_pool->physical_used_cnt_ > 0) { buffer_pool->Evict(); }
    LOG_INFO("Complete cleaning the cache. Buffer size: %lu", buffer_pool->physical_used_cnt_.load());
  });
}

// -------------------------------------------------------------------------------------
void LeanStore::StartGroupCommitThread() {
  group_committer = std::thread([&]() {
    worker_thread_id = FLAGS_worker_count;
    gct = std::make_unique<recovery::GroupCommitExecutor>(buffer_pool.get(), log_manager.get(), is_running);
    gct->StartExecution();
    std::printf("Halt LeanStore's GroupCommit thread\n");
  });
}

void LeanStore::StartProfilingThread() {
  start_profiling = true;
  stat_collector  = std::thread([&]() {
    pthread_setname_np(pthread_self(), "stats_collector");
    std::printf("ts,tx,bm_rmb,bm_wmb,bm_evict,blob_io_mb,logio_mb,db_size,alloc_size,mem_mb\n");
    float r_mb;
    float w_mb;
    u64 e_cnt;
    u64 cnt = 0;

    while (is_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      // Progress stats
      auto progress = statistics::txn_processed.exchange(0);
      statistics::total_txn_completed += progress;
      // Buffer stats
      r_mb          = static_cast<float>(statistics::buffer::read_cnt.exchange(0) * PAGE_SIZE) / MB;
      w_mb          = static_cast<float>(statistics::buffer::write_cnt.exchange(0) * PAGE_SIZE) / MB;
      e_cnt         = statistics::buffer::evict_cnt.exchange(0);
      auto db_sz    = DBSize();
      auto alloc_sz = AllocatedSize();
      // Group-commit stats
      auto log_write = static_cast<float>(statistics::recovery::gct_write_bytes.exchange(0)) / MB;
      auto blob_io   = static_cast<float>(statistics::blob::blob_logging_io.exchange(0)) / MB;
      // System stats
      auto mem_mb = static_cast<float>(getPeakRSS()) / MB;
      // Output
      std::printf("%lu,%lu,%.4f,%.4f,%lu,%.2f,%.4f,%.4f,%.4f,%.4f\n", cnt++, progress, r_mb, w_mb, e_cnt, blob_io,
                   log_write, db_sz, alloc_sz, mem_mb);
    }
    std::printf("Halt LeanStore's Profiling thread\n");
  });
}

// -------------------------------------------------------------------------------------
auto LeanStore::CreateNewBlob(std::span<const u8> blob_payload, BlobState *prev_blob, bool likely_grow)
  -> std::span<const u8> {
  if (blob_payload.empty()) { throw leanstore::ex::GenericException("Blob payload shouldn't be empty"); }
  // TODO(Duy): Compress blob_payload before calling AllocateBlob
  auto blob_hd = blob_manager->AllocateBlob(blob_payload, prev_blob, likely_grow);
  if (FLAGS_blob_logging_variant == -1) {
    auto &current_txn = transaction::TransactionManager::active_txn;
    current_txn.LogBlob(blob_payload);
  }
  return std::span{reinterpret_cast<u8 *>(blob_hd), blob_hd->MallocSize()};
}

void LeanStore::LoadBlob(const BlobState *blob_t, const storage::blob::BlobCallbackFunc &read_cb, bool partial_load) {
  if (partial_load) {
    blob_manager->LoadBlob(blob_t, PAGE_SIZE, read_cb);
  } else {
    blob_manager->LoadBlob(blob_t, blob_t->blob_size, read_cb);
  }
}

void LeanStore::RemoveBlob(BlobState *blob_t) { blob_manager->RemoveBlob(blob_t); }

auto LeanStore::RetrieveComparisonFunc(ComparisonOperator cmp_op) -> ComparisonLambda {
  switch (cmp_op) {
    case ComparisonOperator::BLOB_LOOKUP:
      return {cmp_op, [blob_man = blob_manager.get()](const void *a, const void *b, [[maybe_unused]] size_t) {
                return blob_man->BlobStateCompareWithString(a, b);
              }};
    case ComparisonOperator::BLOB_HANDLER:
      return {cmp_op, [blob_man = blob_manager.get()](const void *a, const void *b, [[maybe_unused]] size_t) {
                return blob_man->BlobStateComparison(a, b);
              }};
    default: return {cmp_op, std::memcmp};
  }
}

}  // namespace leanstore
