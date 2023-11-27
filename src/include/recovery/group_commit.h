#pragma once

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "recovery/log_worker.h"

#include "gtest/gtest_prod.h"
#include "liburing.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_set>

namespace leanstore::recovery {

class LogManager;

class GroupCommitExecutor {
 public:
  GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager,
                      std::atomic<bool> &bg_threads_keep_running);
  ~GroupCommitExecutor() = default;

  /* Main exposed API */
  void StartExecution();

  /* Utilities APIs */
  void PrepareWrite(u8 *src, size_t size, size_t offset);
  void InitializeRound();
  void CompleteRound();

  /* Distributed logging 3 phases */
  void PhaseOne();
  void PhaseTwo();
  void PhaseThree();
  void PhaseFour();
  void PhaseFive();

 private:
  friend class leanstore::LeanStore;
  FRIEND_TEST(TestGroupCommit, BasicOperation);
  FRIEND_TEST(TestGroupCommitBlob, BlobSupportVariant);
  using timepoint_t = decltype(std::chrono::high_resolution_clock::now());

  /* Only run one round */
  void ExecuteOneRound();

  /* Large Page async utilities */
  void PrepareLargePageWrite(transaction::Transaction &txn);
  void CompleteTxnBlobs(transaction::Transaction &txn);

  /* IO Uring hot submit*/
  void HotUringSubmit();

  /* Env */
  buffer::BufferManager *buffer_;
  std::atomic<bool> *keep_running_;
  bool has_blob_;

  /* IO interfaces */
  int wal_fd_;
  size_t w_offset_;
  std::vector<std::tuple<pageid_t, u64, pageid_t, u64>> lp_req_{};  // Vector of [Dirty large page, extent]

  /* io_uring properties */
  struct io_uring ring_;
  u32 submitted_io_cnt_{0};

  /* GSN & timestamp management*/
  logid_t min_all_workers_gsn_;  // For Remote Flush Avoidance
  logid_t max_all_workers_gsn_;  // Sync all workers to this point
  timestamp_t min_hardened_commit_ts_;

  /* Log workers management */
  LogWorker *all_loggers_;
  std::unordered_set<pageid_t> already_prep_;  // List of prepared extents
  std::unordered_set<pageid_t> completed_lp_;  // List of completed large pages (not extents) committed to disk
  std::vector<size_t> ready_to_commit_cut_;
  std::vector<size_t> ready_to_commit_rfa_cut_;
  std::vector<LogWorker::WorkerConsistentState> worker_states_;

  /* Statistics */
  timepoint_t phase_1_begin_;
  timepoint_t phase_1_end_;
  timepoint_t phase_2_begin_;
  timepoint_t phase_2_end_;
  timepoint_t phase_3_begin_;
  timepoint_t phase_3_end_;
  timepoint_t phase_4_begin_;
  timepoint_t phase_4_end_;
  timepoint_t phase_5_begin_;
  timepoint_t phase_5_end_;
  u64 round_{0};
  u64 completed_txn_;
};

}  // namespace leanstore::recovery