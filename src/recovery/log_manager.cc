#include "recovery/log_manager.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/dependency_closure.h"
#include "sync/hybrid_guard.h"

#include "share_headers/time.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <new>

using leanstore::transaction::CommitProtocol;

namespace leanstore::recovery {

u64 LogManager::stealing_border = 0;

LogManager::LogManager(std::atomic<bool> &is_running)
    : no_commit_executor_((FLAGS_txn_commit_variant == CommitProtocol::AUTONOMOUS_COMMIT)
                            ? FLAGS_worker_count / FLAGS_txn_commit_group_size
                            : 1),
      commit_latches_(no_commit_executor_),
      worker_write_batch_size_(FLAGS_wal_batch_write_kb * KB),
      wal_block_size_(FLAGS_wal_block_size_mb * MB),
      w_state_(FLAGS_worker_count),
      commit_state_(FLAGS_worker_count) {
  LogManager::stealing_border = std::max(BitLength(FLAGS_wal_stealing_group_size) - 1, 1);

  /* Initialize log I/O */
  wal_fd_ = open(FLAGS_db_path.c_str(), O_WRONLY | O_DIRECT, S_IRWXU);
  Ensure(wal_fd_ > 0);
  w_offset_ = StorageCapacity(FLAGS_db_path.c_str());

  /* Initialize local log workers */
  logger_ = static_cast<LogWorker *>(calloc(FLAGS_worker_count, sizeof(LogWorker)));
  for (auto idx = 0U; idx < FLAGS_worker_count; idx++) {
    new (&logger_[idx]) LogWorker(is_running, this);
    logger_[idx].w_state = &(w_state_[idx]);
  }

  /* Initialize group commit context */
  gc_ = static_cast<GroupCommitExecutor *>(calloc(no_commit_executor_, sizeof(GroupCommitExecutor)));
}

LogManager::~LogManager() {
  for (auto idx = 0U; idx < FLAGS_worker_count; idx++) { logger_[idx].~LogWorker(); }
  free(logger_);
  for (auto idx = 0U; idx < no_commit_executor_; idx++) { gc_[idx].~GroupCommitExecutor(); }
  free(gc_);
}

auto LogManager::LocalLogWorker() -> LogWorker & {
  assert(LeanStore::worker_thread_id < FLAGS_worker_count);
  return logger_[LeanStore::worker_thread_id];
}

auto LogManager::WALOffset() -> std::atomic<u64> & { return w_offset_; }

void LogManager::InitializeCommitExecutor(buffer::BufferManager *buffer, std::atomic<bool> &is_running) {
  for (auto idx = 0U; idx < no_commit_executor_; idx++) {
    auto start_wid = idx * FLAGS_txn_commit_group_size;
    auto end_wid   = std::min(FLAGS_worker_count, start_wid + FLAGS_txn_commit_group_size);
    spdlog::info("Group commit {}: worker [{}..{}]", idx, start_wid, end_wid - 1);
    new (&gc_[idx]) GroupCommitExecutor(buffer, this, start_wid, end_wid, is_running);
  }
}

auto LogManager::CommitExecutor(u32 commit_idx) -> GroupCommitExecutor & { return gc_[commit_idx]; }

auto LogManager::NumberOfCommitExecutor() -> u32 { return no_commit_executor_; }

void LogManager::TriggerGroupCommit(u32 commit_idx) {
  if (commit_latches_[commit_idx].TryLockExclusive()) {
    sync::HybridGuard guard(&(commit_latches_[commit_idx]), sync::GuardMode::ADOPT_EXCLUSIVE);
    gc_[commit_idx].ExecuteOneRound();
  }
}

/**
 * @brief VECTOR variant: advance the stable prefix of every worker
 * `durable` holds each worker's durable commit timestamp.
 * The new stable prefixes are the largest durable prefixes which are closed under dependencies.
 * Waiting instead for each dependency's worker to acknowledge it would deadlock on a cycle of dependencies
 */
void LogManager::AdvanceStablePrefix(std::span<const WorkerConsistentState> durable) {
  std::unique_lock guard(stable_mutex_, std::try_to_lock);
  if (!guard.owns_lock()) { return; }

  /* Only this function pops from `pending`, and push_back() keeps references to the other elements valid */
  auto prefix = std::vector<std::vector<const PendingCommit *>>(FLAGS_worker_count);
  for (auto w_i = 0U; w_i < FLAGS_worker_count; w_i++) {
    auto &stable = logger_[w_i].stable;
    std::scoped_lock lock(stable.pending_mutex);
    for (const auto &commit : stable.pending) {
      if (commit.commit_ts > durable[w_i].precommitted_tx_commit_ts) { break; }
      prefix[w_i].push_back(&commit);
    }
  }

  CutToClosedPrefixes(std::span{prefix}, [&](wid_t w_i) { return logger_[w_i].stable.gsn.load(); });

  auto min_stable_gsn = std::numeric_limits<timestamp_t>::max();
  for (auto w_i = 0U; w_i < FLAGS_worker_count; w_i++) {
    auto &stable = logger_[w_i].stable;
    if (!prefix[w_i].empty()) {
      stable.gsn.store(prefix[w_i].back()->commit_gsn);
      stable.commit_ts.store(prefix[w_i].back()->commit_ts);
      std::scoped_lock lock(stable.pending_mutex);
      stable.pending.erase(stable.pending.begin(), stable.pending.begin() + std::ssize(prefix[w_i]));
    }
    min_stable_gsn = std::min(min_stable_gsn, stable.gsn.load());
  }
  UpdateMax(global_min_gsn_stable, min_stable_gsn);
}

/**
 * @brief Write a dummy log entry (MasterRecord) here, which contains newest signature,
 *  used to differentiate between previous LeanStore session and current session.
 */
void LogManager::WriteMasterRecord() {
  /* Generate MASTER_RECORD log entry */
  auto start_tsctime           = tsctime::ReadTSC();
  LeanStore::signature         = ComputeCRC(reinterpret_cast<u8 *>(&start_tsctime), sizeof(u64));
  auto buffer                  = AlignedPtr<BLK_BLOCK_SIZE>(BLK_BLOCK_SIZE);
  auto master_record           = new (buffer.get()) LogMetaEntry();
  master_record->type          = LogEntry::Type::MASTER_RECORD;
  master_record->size          = sizeof(LogMetaEntry);
  master_record->rc.gsn        = 1;
  master_record->rc.chunk_size = BLK_BLOCK_SIZE;
  master_record->rc.signature  = LeanStore::signature;
  if (FLAGS_wal_debug) { master_record->ComputeChksum(); }

  /* Persist master record */
  [[maybe_unused]] auto cnt = pwrite(wal_fd_, buffer.get(), BLK_BLOCK_SIZE, w_offset_.load() - BLK_BLOCK_SIZE);
  assert(cnt == BLK_BLOCK_SIZE);
  if (FLAGS_wal_fsync) { fdatasync(wal_fd_); }
  w_offset_ -= BLK_BLOCK_SIZE;
}

}  // namespace leanstore::recovery