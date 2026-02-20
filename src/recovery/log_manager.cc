#include "recovery/log_manager.h"
#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "sync/hybrid_guard.h"

#include "share_headers/time.h"

#include <memory>
#include <new>

namespace leanstore::recovery {

u64 LogManager::stealing_border = 0;

LogManager::LogManager(std::atomic<bool> &is_running)
    : no_commit_executor_(FLAGS_worker_count / FLAGS_txn_commit_group_size),
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