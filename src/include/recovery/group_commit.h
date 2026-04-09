#pragma once

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "recovery/log_worker.h"

#include "gtest/gtest_prod.h"
#include "liburing.h"
#include "roaring/roaring.hh"

#include <atomic>
#include <functional>
#include <memory>

namespace leanstore::recovery {

class LogManager;

class GroupCommitExecutor {
 public:
  static constexpr u32 GROUP_COMMIT_QD = 16;

  GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager, u32 start_wid, u32 end_wid,
                      std::atomic<bool> &keep_running);
  ~GroupCommitExecutor() = default;

  /* Main API */
  void StartExecution();
  void ExecuteOneRound();
  void InitializeRound();
  void CompleteRound();

  /* Commit processing in 4 phases */
  void PhaseOne();
  void PhaseTwo();
  void PhaseThree();

 private:
  friend class leanstore::LeanStore;
  friend class LogManager;

  FRIEND_TEST(TestGroupCommit, BasicOperation);
  FRIEND_TEST(TestGroupCommit, BlobSupportVariant);

  /* Private utilities */
  void Fsync();
  void PrepareWrite(u8 *src, size_t size, size_t offset);
  void CollectConsistentState(wid_t w_i);
  void CollectPrecommittedQueue(wid_t w_i);

  /* Per-txn utilities */
  void CompleteTransaction(transaction::SerializableTransaction &txn);
  void PrepareLargePageWrite(const transaction::SerializableTransaction &txn);
  auto SatisfyCommitConditions(wid_t w_i, const transaction::SerializableTransaction &txn) -> bool;

  /* Env */
  buffer::BufferManager *buffer_;
  std::atomic<bool> *keep_running_;

  /* io_uring properties */
  struct io_uring ring_;
  u32 submitted_io_cnt_{0};

  /* GSN & timestamp management*/
  timestamp_t min_all_workers_gsn_;  // For Remote Flush Avoidance
  timestamp_t max_all_workers_gsn_;  // Sync all workers to this point
  timestamp_t min_hardened_commit_ts_;

  /* Log workers management */
  LogManager *log_manager_;
  const u64 start_logger_id_;  // The worker id of the 1st worker
  const u64 end_logger_id_;    // The worker id of the last worker in the commit group
  std::vector<size_t> ready_to_commit_cut_;
  std::vector<size_t> ready_to_commit_rfa_cut_;
  std::vector<WorkerConsistentState> worker_states_;

  /* Extent management */
  std::vector<std::tuple<pageid_t, u64, pageid_t>> lp_req_;  // Vector of [Dirty large page, extent_start_pid]
  roaring::Roaring64Map already_prep_;                       // List of prepared extents
  roaring::Roaring64Map completed_lp_;  // List of completed large pages (not extents) committed to disk

  /* Statistics */
  timestamp_t phase_1_begin_;
  timestamp_t phase_2_begin_;
  timestamp_t phase_3_begin_;
  timestamp_t phase_3_end_;
  u64 completed_txn_;
  u64 committed_txn_;
  u64 aborted_txn_;
};

}  // namespace leanstore::recovery