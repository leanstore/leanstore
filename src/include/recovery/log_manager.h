#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "recovery/group_commit.h"
#include "recovery/log_entry.h"
#include "recovery/log_worker.h"
#include "sync/epoch_handler.h"

#include "gtest/gtest_prod.h"

#include <atomic>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::transaction {
class TransactionManager;
}

namespace leanstore::recovery {

class GroupCommitExecutor;

class LogManager {
 public:
  /* The GSN up to which all logs from all workers have been flushed */
  inline static std::atomic<timestamp_t> global_min_gsn_flushed = 0;
  /* Increment the workers' GSN to this value periodically to prevent local GSN from skewing and undermining RFA */
  inline static std::atomic<timestamp_t> global_sync_to_this_gsn = 0;

  explicit LogManager(std::atomic<bool> &is_running);
  ~LogManager();

  /* Child components: Local logger and group commit*/
  auto NumberOfCommitExecutor() -> u32;
  auto CommitExecutor(u32 commit_idx) -> GroupCommitExecutor &;
  auto LocalLogWorker() -> LogWorker &;

  /* Utilities */
  void InitializeCommitExecutor(buffer::BufferManager *buffer, std::atomic<bool> &is_running);
  void TriggerGroupCommit(u32 commit_idx);
  auto WALOffset() -> std::atomic<u64> &;

 private:
  friend class transaction::TransactionManager;
  friend class GroupCommitExecutor;
  friend struct LogWorker;
  friend struct LogBuffer;
  FRIEND_TEST(TestLogManager, CentralizedLogging);

  /* I/O for the log */
  int wal_fd_;
  std::atomic<u64> w_offset_;

  /* Centralized log buffer */
  std::unique_ptr<LogBuffer> centralized_buf_;
  sync::HybridLatch buffer_latch_;

  /* Local logger/log group and the group commit executor */
  LogWorker *logger_;
  GroupCommitExecutor *gc_;
  u32 no_commit_executor_;

  /* For baseline commit & WILO steal, whoever worker acquires X-latch can run group commit */
  std::vector<sync::HybridLatch> commit_latches_;

  /* Logger environments */
  const u64 worker_write_batch_size_; /* For WILO, whenever a worker filled this number, it triggers log write */
  std::vector<WorkerConsistentState> w_state_;
  std::vector<WorkerConsistentState> commit_state_;
};

}  // namespace leanstore::recovery
