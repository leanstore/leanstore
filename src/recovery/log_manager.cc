#include "recovery/log_manager.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "sync/hybrid_guard.h"

#include "share_headers/time.h"

#include <new>

using leanstore::transaction::CommitProtocol;

namespace leanstore::recovery {

LogManager::LogManager(std::atomic<bool> &is_running)
    : no_commit_executor_((FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL)
                            ? FLAGS_worker_count / FLAGS_txn_commit_group_size
                            : 1),
      commit_latches_(no_commit_executor_),
      worker_write_batch_size_(FLAGS_wal_batch_write_kb * KB),
      w_state_(FLAGS_worker_count),
      commit_state_(FLAGS_worker_count) {
  /* Initialize log I/O*/
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

  /* Initialize centralized log buffer if required */
  if (FLAGS_wal_centralized_buffer) {
    centralized_buf_ = std::make_unique<LogBuffer>(FLAGS_worker_count * FLAGS_wal_buffer_size_mb * MB, &is_running);
  }
}

LogManager::~LogManager() {
  for (auto idx = 0U; idx < FLAGS_worker_count; idx++) { logger_[idx].~LogWorker(); }
  free(logger_);
  for (auto idx = 0U; idx < no_commit_executor_; idx++) { gc_[idx].~GroupCommitExecutor(); }
  free(gc_);
}

auto LogManager::LocalLogWorker() -> LogWorker & {
  assert(worker_thread_id < FLAGS_worker_count);
  return logger_[worker_thread_id];
}

auto LogManager::WALOffset() -> std::atomic<u64> & { return w_offset_; }

void LogManager::InitializeCommitExecutor(buffer::BufferManager *buffer, std::atomic<bool> &is_running) {
  for (auto idx = 0U; idx < no_commit_executor_; idx++) {
    auto start_wid = idx * FLAGS_txn_commit_group_size;
    auto end_wid   = std::min(FLAGS_worker_count, start_wid + FLAGS_txn_commit_group_size);
    LOG_DEBUG("Group commit %u: worker [%u..%u]", idx, start_wid, end_wid - 1);
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

}  // namespace leanstore::recovery