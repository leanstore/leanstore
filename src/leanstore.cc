#include "leanstore/leanstore.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "storage/btree/tree.h"

#include "share_headers/mem_usage.h"
#include "share_headers/time.h"

#include <sys/sysinfo.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <ranges>
#include <span>

namespace leanstore {

u32 LeanStore::signature = 0;
std::vector<buffer::BufferManager *> LeanStore::all_buffer_pools;
thread_local wid_t LeanStore::worker_thread_id = -1;  // The ID of current worker (min wid is 0)

LeanStore::LeanStore()
    : buffer_pool(std::make_unique<buffer::BufferManager>(is_running)),
      log_manager(std::make_unique<recovery::LogManager>(is_running)),
      transaction_manager(std::make_unique<transaction::TransactionManager>(buffer_pool.get(), log_manager.get())),
      blob_manager(std::make_unique<storage::blob::BlobManager>(buffer_pool.get())),
      recovery(std::make_unique<recovery::RecoveryManager>(buffer_pool.get())),
      worker_pool(is_running,
                  [&]() {
                    buffer_pool->ConstructLocalRing();
                    log_manager->LocalLogWorker().backend.Connect();
                  }),
      gen(2, FLAGS_wal_max_idle_time_us * 1000) {
  // Validate and fix flags
  Ensure(FLAGS_txn_commit_group_size > 0);
  Ensure(FLAGS_worker_count % FLAGS_wal_stealing_group_size == 0);
  Ensure(FLAGS_worker_count % FLAGS_txn_commit_group_size == 0);
  Ensure(FLAGS_wal_stealing_group_size <= FLAGS_worker_count);
  Ensure(FLAGS_txn_commit_group_size <= FLAGS_worker_count);
  if (FLAGS_txn_commit_variant == transaction::CommitProtocol::AUTONOMOUS_COMMIT) {
    Ensure(FLAGS_worker_count % FLAGS_txn_commit_group_size == 0);
  } else {
    FLAGS_txn_commit_group_size = FLAGS_worker_count;
  }
#ifdef DEBUG
  spdlog::set_level(spdlog::level::debug);
#endif

  // Necessary dirty works
  WarningMessage("Current TSC_PER_NS setting: " + std::to_string(tsctime::TSC_PER_NS));
  log_manager->InitializeCommitExecutor(buffer_pool.get(), is_running);
  worker_pool.ScheduleSyncJob(0, [&]() { buffer_pool->AllocMetadataPage(); });
  LeanStore::all_buffer_pools.push_back(buffer_pool.get());

  // If recovery is required, initialize the recovery manager and then execute it
  if (FLAGS_wal_enable_recovery) {
    FLAGS_wal_enable    = false;  // HACK: disable WAL subsystem temporarily during recovery
    auto recovery_start = tsctime::ReadTSC();
    recovery->RecoveryPreparation();
    LeanStore::signature       = recovery->SignatureOfPrevSession();
    FLAGS_wal_recovery_threads = std::min(FLAGS_wal_recovery_threads, FLAGS_worker_count);

    /**
     * @brief Three-phase recovery
     * - 1st: Log materialization
     * - 2nd: Analysis: determine winner and loser
     * - 3rd: Redo logs
     * Not support UNDO at the moment
     *
     * After recovery, continue appending log with previous session, i.e., reuse the same MASTER_RECORD block
     */
    // 1st phase
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
      worker_pool.ScheduleAsyncJob(idx, [&]() {
        auto last_empty_offset = recovery->MaterializeLogs();
        UpdateMin(log_manager->WALOffset(), last_empty_offset);
      });
    }
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }
    auto recovery_p1 = tsctime::ReadTSC();
    spdlog::info("Last valid offset: {}", log_manager->WALOffset().load() / MB);

    // 2nd phase
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
      worker_pool.ScheduleAsyncJob(idx, [&]() { recovery->Analysis(); });
    }
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }
    auto recovery_p2 = tsctime::ReadTSC();

    // 3rd phase
    worker_pool.ScheduleSyncJob(0, [&]() { recovery->PrepareRedoEnv(); });
    if (!FLAGS_wal_instant_recovery) {
      for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
        worker_pool.ScheduleAsyncJob(idx, [&]() {
          transaction_manager->StartTransaction(leanstore::transaction::Transaction::Type::SYSTEM);
          recovery->Redo();
          CommitTransaction();
        });
      }
      for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }
    }

    // recovery end
    auto recovery_end = tsctime::ReadTSC();
    spdlog::info("Recovery time: phase 1 {:.4f} us, phase 2 {:.4f} us, phase 3 {:.4f} us",
                 tsctime::TscDifferenceNs(recovery_start, recovery_p1) / 1000.0L,
                 tsctime::TscDifferenceNs(recovery_p1, recovery_p2) / 1000.0L,
                 tsctime::TscDifferenceNs(recovery_p2, recovery_end) / 1000.0L);
    FLAGS_wal_enable = true;
  } else {
    // Start a fresh session
    log_manager->WriteMasterRecord();
  }

  // Group commit

  if ((FLAGS_txn_commit_variant != transaction::CommitProtocol::BASELINE_COMMIT) &&
      (FLAGS_txn_commit_variant != transaction::CommitProtocol::AUTONOMOUS_COMMIT)) {
    group_committer = std::thread([&]() {
      pthread_setname_np(pthread_self(), "group_committer");
      worker_thread_id = FLAGS_worker_count;
      log_manager->CommitExecutor(0).StartExecution();
      std::printf("Halt LeanStore's background GroupCommit thread\n");
    });
  }

  // Page provider threads
  if (FLAGS_page_provider_thread > 0) { buffer_pool->RunPageProviderThreads(); }
}

LeanStore::~LeanStore() {
  Shutdown();
  start_profiling = false;
}

void LeanStore::Shutdown() {
  worker_pool.Stop();
  Ensure(is_running == false);
  if (group_committer.joinable()) { group_committer.join(); }
  if (stat_collector.joinable()) {
    stat_collector.join();
    // Print transaction latency
    if (FLAGS_txn_debug) {
      spdlog::info("Start measuring latency");
      auto wcnt = FLAGS_worker_count;
      if ((FLAGS_txn_commit_variant != transaction::CommitProtocol::BASELINE_COMMIT) &&
          (FLAGS_txn_commit_variant != transaction::CommitProtocol::AUTONOMOUS_COMMIT)) {
        wcnt++;
      }
      for (auto idx = 1U; idx < wcnt; idx++) {
        std::ranges::copy(statistics::txn_latency[idx], std::back_inserter(statistics::txn_latency[0]));
        std::ranges::copy(statistics::rfa_txn_latency[idx], std::back_inserter(statistics::rfa_txn_latency[0]));
        std::ranges::copy(statistics::lat_inc_wait[idx], std::back_inserter(statistics::lat_inc_wait[0]));
        std::ranges::copy(statistics::txn_queue[idx], std::back_inserter(statistics::txn_queue[0]));
        std::ranges::copy(statistics::txn_exec[idx], std::back_inserter(statistics::txn_exec[0]));
        std::ranges::copy(statistics::io_latency[idx], std::back_inserter(statistics::io_latency[0]));
        std::ranges::copy(statistics::txn_per_round[idx], std::back_inserter(statistics::txn_per_round[0]));
      }
      spdlog::info("# data points: {}", statistics::txn_latency[0].size() + statistics::rfa_txn_latency[0].size());
      spdlog::info("Start evaluating latency data");
      std::sort(statistics::txn_latency[0].begin(), statistics::txn_latency[0].end());
      std::sort(statistics::rfa_txn_latency[0].begin(), statistics::rfa_txn_latency[0].end());
      if (!statistics::rfa_txn_latency[0].empty()) {
        WriteSequenceToFile(statistics::rfa_txn_latency[0], 1000, "rfa_latency.txt");
        auto nth = statistics::rfa_txn_latency[0].size() * 999 / 1000;
        spdlog::info("RFA transaction statistics:\n\tAvgLatency({:.4f} us)\n\t99.9thLatency({} us)",
                     Average(statistics::rfa_txn_latency[0]) / 1000UL,
                     static_cast<float>(statistics::rfa_txn_latency[0][nth - 1]) / 1000UL);
      }
      if (!statistics::txn_latency[0].empty()) {
        WriteSequenceToFile(statistics::txn_latency[0], 1000, "normal_latency.txt");
        auto nth = statistics::txn_latency[0].size() * 999 / 1000;
        spdlog::info("Normal transaction statistics:\n\tAvgLatency({:.4f} us)\n\t99.9thLatency({} us)",
                     Average(statistics::txn_latency[0]) / 1000UL,
                     static_cast<float>(statistics::txn_latency[0][nth - 1]) / 1000UL);
      }
      spdlog::info(
        "Statistics:\n\tAvgExecTime({:.4f} us)\n\tAvgQueue({:.4f} us)\n\tAvgLatencyInclWait({:.4f} us)\n\t"
        "AvgIOLatency({:.4f} us)\n\t"
        "AvgTxnPerCommitRound({:.4f} txns)\n\t99.9thTxnPerRound({} txns)\n\t99.99thTxnPerRound({} txns)",
        Average(statistics::txn_exec[0]) / 1000UL, Average(statistics::txn_queue[0]) / 1000UL,
        Average(statistics::lat_inc_wait[0]) / 1000UL,
        Average(statistics::io_latency[0]) / 1000UL,
        Average(statistics::txn_per_round[0]),
        Percentile(statistics::txn_per_round[0], 99.9), Percentile(statistics::txn_per_round[0], 99.99));
      std::vector<timestamp_t> summary;
      std::merge(statistics::rfa_txn_latency[0].begin(), statistics::rfa_txn_latency[0].end(),
                 statistics::txn_latency[0].begin(), statistics::txn_latency[0].end(), std::back_inserter(summary));
      WriteSequenceToFile(summary, 1000, "latency.txt");
    }
  }
}

void LeanStore::CheckDuringIdle() {
  if ((FLAGS_txn_commit_variant == transaction::CommitProtocol::WORKERS_WRITE_LOG) ||
      (FLAGS_txn_commit_variant == transaction::CommitProtocol::AUTONOMOUS_COMMIT)) {
    assert(FLAGS_wal_max_idle_time_us > 0);
    /**
     * @brief A probabilistic model to decide whether to write logs during IDLE
     *
     * With original WILO, there are two scenarios that it can't handle:
     * - 1 write txn then a sequence of read-only transactions
     * - 1 write txn then system workers become idle for a long time
     *
     * To tackle these two issues, we can force WILO to write logs during system idle --
     *  see PoissonScheduler::Wait() for your information
     * However, if we force write every time, it's possible that:
     * - the free time is not enough for 1 write, i.e., idle for 5us, but 1 write causes at least 20us
     * - we write log excessively, worsening write amplification and decrease throughput
     *
     * To fix this, we probabilistically trigger force write according to the maximum allowed idle time
     */
    auto avg_idle_time = Average(statistics::worker_idle_ns[worker_thread_id]);
    auto eval_value    = gen.NoElements() - gen.Rand();
    if (avg_idle_time >= eval_value) {
      auto &logger = log_manager->LocalLogWorker();
      if (FLAGS_wal_variant != LoggingVariant::VECTOR) {
        logger.last_unharden_commit_ts = transaction_manager->AddBarrierTransaction();
        logger.PublicCommitTS();
      }
      logger.WorkerStealsLog(true);
      log_manager->TriggerGroupCommit(worker_thread_id / FLAGS_txn_commit_group_size);
    }
  }
}

// -------------------------------------------------------------------------------------
void LeanStore::RegisterTable(const std::type_index &relation, uint32_t relation_idx) {
  assert(indexes.find(relation) == indexes.end());
  assert(FLAGS_worker_count > 0);
  if (FLAGS_wal_enable_recovery) { Ensure(recovery->HasRecovered(METADATA_PAGE_ID)); }
  worker_pool.ScheduleSyncJob(0, [&]() {
    transaction_manager->StartTransaction(leanstore::transaction::Transaction::Type::SYSTEM);
    indexes.try_emplace(relation, std::make_unique<storage::BTree>(buffer_pool.get(), recovery.get(), relation_idx));
    CommitTransaction();
  });
}

auto LeanStore::RetrieveIndex(const std::type_index &relation) -> KVInterface * {
  assert(indexes.find(relation) != indexes.end());
  return indexes.at(relation).get();
}

void LeanStore::StartTransaction(timestamp_t txn_arrival_time, Transaction::Mode tx_mode,
                                 const std::string &tx_isolation_level) {
  transaction_manager->StartTransaction(Transaction::Type::USER, txn_arrival_time,
                                        transaction::TransactionManager::ParseIsolationLevel(tx_isolation_level),
                                        tx_mode);
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
  auto capacity           = StorageCapacity(FLAGS_db_path.c_str());
  auto current_wal_offset = log_manager->WALOffset().load();
  return static_cast<float>(capacity - current_wal_offset) / GB;
}

auto LeanStore::DBSize() -> float {
  return static_cast<float>((buffer_pool->alloc_cnt_.load() - statistics::storage::free_size) * PAGE_SIZE) / GB;
}

void LeanStore::DropCache() {
  worker_pool.ScheduleSyncJob(0, [&]() {
    spdlog::info("Dropping the cache");
    while (buffer_pool->physical_used_cnt_ > 0) { buffer_pool->Evict(); }
    spdlog::info("Complete cleaning the cache. Buffer size: {}", buffer_pool->physical_used_cnt_.load());
  });
}

// -------------------------------------------------------------------------------------

void LeanStore::StartProfilingThread() {
  start_profiling = true;
  stat_collector  = std::thread([&]() {
    pthread_setname_np(pthread_self(), "stats_collector");
    std::printf(
      "ts,tx,normal,rfa,commit_rounds,bm_rmb,bm_wmb,bm_evict,log_sz_mb,logio_mb,log_flush_cnt,"
       "gct_p1_us,gct_p2_us,gct_p3_us,db_size\n");
    auto cnt           = 0UL;
    auto completed_txn = 0UL;
    auto commit_exec   = 0UL;
    auto commit_rounds = 0UL;

    while (is_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (!start_profiling_latency) { start_profiling_latency = true; }
      // Progress stats
      auto rounds   = 0UL;
      auto progress = 0UL;
      // Txn type start
      auto normal_txn = 0UL;
      auto rfa_txn    = 0UL;
      for (auto idx = 0U; idx <= FLAGS_worker_count; idx++) {
        rounds += statistics::commit_rounds[idx].exchange(0);
        progress += statistics::txn_processed[idx].exchange(0);
        normal_txn += statistics::precommited_txn_processed[idx].exchange(0);
        rfa_txn += statistics::precommited_rfa_txn_processed[idx].exchange(0);
      }
      completed_txn += normal_txn + rfa_txn;
      if (!FLAGS_wal_enable) { progress = normal_txn + rfa_txn; }
      statistics::total_committed_txn += progress;
      commit_rounds += rounds;
      // System stats
      auto r_mb  = static_cast<float>(statistics::buffer::read_cnt.exchange(0) * PAGE_SIZE) / MB;
      auto w_mb  = static_cast<float>(statistics::buffer::write_cnt.exchange(0) * PAGE_SIZE) / MB;
      auto e_cnt = statistics::buffer::evict_cnt.exchange(0);
      auto db_sz = DBSize();
      // Group commit stats
      auto log_sz        = 0.0F;
      auto log_write     = 0.0F;
      auto log_flush_cnt = 0UL;
      auto p1_us         = 0UL;
      auto p2_us         = 0UL;
      auto p3_us         = 0UL;
      for (auto idx = 0U; idx <= FLAGS_worker_count; idx++) {
        log_sz += static_cast<float>(statistics::recovery::real_log_bytes[idx].exchange(0)) / MB;
        log_write += static_cast<float>(statistics::recovery::written_log_bytes[idx].exchange(0)) / MB;
        log_flush_cnt += statistics::log_flush_cnt[idx].exchange(0);
        p1_us += statistics::recovery::gct_phase_1_ns[idx].exchange(0) / 1000;
        p2_us += statistics::recovery::gct_phase_2_ns[idx].exchange(0) / 1000;
        p3_us += statistics::recovery::gct_phase_3_ns[idx].exchange(0) / 1000;
      }
      commit_exec += p1_us + p2_us + p3_us;
      // Output
      std::printf("%lu,%lu,%lu,%lu,%lu,%.4f,%.4f,%lu,%.4f,%.4f,%lu,%lu,%lu,%lu,%.4f\n",
                  cnt++, progress, normal_txn,
                   rfa_txn, rounds, r_mb, w_mb, e_cnt, log_sz, log_write, log_flush_cnt, p1_us, p2_us, p3_us, db_sz);
    }
    spdlog::info("Transaction statistics: # completed txns: {} - # committed txns: {}", completed_txn,
                  statistics::total_committed_txn.load());
    spdlog::info("AvgGroupCommitTime: {:.4f}us - No rounds {} - Txn per round {:.4f}",
                  static_cast<double>(commit_exec) / commit_rounds, commit_rounds,
                  static_cast<double>(statistics::total_committed_txn) / commit_rounds);
    spdlog::info("Halt LeanStore's Profiling thread");
  });
}

// -------------------------------------------------------------------------------------
auto LeanStore::CreateNewBlob(std::span<const u8> blob_payload, BlobState *prev_blob, bool likely_grow)
  -> std::span<const u8> {
  Ensure(FLAGS_blob_enable);
  if (blob_payload.empty()) { throw leanstore::ex::GenericException("Blob payload shouldn't be empty"); }
  auto blob_hd = blob_manager->AllocateBlob(blob_payload, prev_blob, likely_grow);
  return std::span{reinterpret_cast<u8 *>(blob_hd), blob_hd->MallocSize()};
}

void LeanStore::LoadBlob(const BlobState *blob_t, const storage::blob::BlobCallbackFunc &read_cb, bool partial_load) {
  Ensure(FLAGS_blob_enable);
  if (partial_load) {
    blob_manager->LoadBlob(blob_t, PAGE_SIZE, read_cb);
  } else {
    blob_manager->LoadBlob(blob_t, blob_t->blob_size, read_cb);
  }
}

void LeanStore::RemoveBlob(BlobState *blob_t) {
  Ensure(FLAGS_blob_enable);
  blob_manager->RemoveBlob(blob_t);
}

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
