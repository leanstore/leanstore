#include "recovery/group_commit.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "transaction/transaction_manager.h"

#include "share_headers/time.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <tuple>

using leanstore::transaction::CommitProtocol;

#define PREPARE_LARGE_PAGE_WRITE(pre_queue, pre_queue_rfa)                      \
  ({                                                                            \
    (pre_queue).LoopElement(ready_to_commit_cut_[w_i], [&](auto &txn) {         \
      PrepareLargePageWrite(txn);                                               \
      return true;                                                              \
    });                                                                         \
    (pre_queue_rfa).LoopElement(ready_to_commit_rfa_cut_[w_i], [&](auto &txn) { \
      PrepareLargePageWrite(txn);                                               \
      return true;                                                              \
    });                                                                         \
  })

/**
 * @brief In decentralized logging, there are two conditions for commit operations:
 * - The transaction didn't see any uncommitted content (from a possible dependency)
 * - Dependent transactions are hardened, i.e., their logs are persistent
 */
#define COMPLETE_NORMAL_QUEUE(pre_queue)                                                                     \
  ({                                                                                                         \
    auto barrier_txn = 0UL;                                                                                  \
    auto tx_i        = (pre_queue).LoopElement(ready_to_commit_cut_[w_i], [&](auto &txn) {                   \
      if (txn.max_observed_gsn <= min_all_workers_gsn_ && txn.commit_ts <= min_hardened_commit_ts_) { \
        if (txn.state == transaction::Transaction::State::BARRIER) {                                  \
          barrier_txn++;                                                                              \
        } else {                                                                                      \
          CompleteTransaction(txn);                                                                   \
        }                                                                                             \
        return true;                                                                                  \
      }                                                                                               \
      return false;                                                                                   \
    });                                                                                               \
    if (tx_i > 0) {                                                                                          \
      (pre_queue).Erase(tx_i);                                                                               \
      completed_txn_ += tx_i - barrier_txn;                                                                  \
    }                                                                                                        \
  })

#define COMPLETE_RFA_QUEUE(pre_queue_rfa)                                                          \
  ({                                                                                               \
    auto barrier_txn = 0UL;                                                                        \
    auto tx_i        = (pre_queue_rfa).LoopElement(ready_to_commit_rfa_cut_[w_i], [&](auto &txn) { \
      if (txn.commit_ts <= worker_states_[w_i].precommitted_tx_commit_ts) [[likely]] {      \
        if (txn.state == transaction::Transaction::State::BARRIER) {                        \
          barrier_txn++;                                                                    \
        } else {                                                                            \
          CompleteTransaction(txn);                                                         \
        }                                                                                   \
        return true;                                                                        \
      }                                                                                     \
      return false;                                                                         \
    });                                                                                     \
    if (tx_i > 0) {                                                                                \
      (pre_queue_rfa).Erase(tx_i);                                                                 \
      completed_txn_ += tx_i - barrier_txn;                                                        \
    }                                                                                              \
  })

namespace leanstore::recovery {

GroupCommitExecutor::GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager, u32 start_wid,
                                         u32 end_wid, std::atomic<bool> &keep_running)
    : buffer_(buffer),
      keep_running_(&keep_running),
      log_manager_(log_manager),
      start_logger_id_(start_wid),
      end_logger_id_(end_wid),
      ready_to_commit_cut_(FLAGS_worker_count),
      ready_to_commit_rfa_cut_(FLAGS_worker_count),
      worker_states_(FLAGS_worker_count) {
  int ret = io_uring_queue_init(GROUP_COMMIT_QD, &ring_, 0);
  if (ret != 0) { throw std::runtime_error("GroupCommit: io_uring_queue_init error"); }
}

/**
 * @brief Reset GroupCommitExecutor's state every round
 */
void GroupCommitExecutor::InitializeRound() {
  already_prepared_ = true;
  lp_req_.clear();
  already_prep_.clear();
  submitted_io_cnt_       = 0;
  completed_txn_          = 0;
  min_all_workers_gsn_    = std::numeric_limits<timestamp_t>::max();
  max_all_workers_gsn_    = 0;
  min_hardened_commit_ts_ = std::numeric_limits<timestamp_t>::max();
}

void GroupCommitExecutor::CompleteRound() {
  // Update RFA env
  already_prepared_ = false;
  UpdateMax(LogManager::global_min_gsn_flushed, min_all_workers_gsn_);
  UpdateMax(LogManager::global_sync_to_this_gsn, max_all_workers_gsn_);

  // Update statistics
  if (start_profiling) {
    statistics::txn_processed[worker_thread_id] += completed_txn_;
    statistics::commit_rounds[worker_thread_id]++;
    statistics::txn_per_round[worker_thread_id].emplace_back(completed_txn_);
    statistics::recovery::gct_phase_1_ns[worker_thread_id] += tsctime::TscDifferenceNs(phase_1_begin_, phase_2_begin_);
    statistics::recovery::gct_phase_2_ns[worker_thread_id] += tsctime::TscDifferenceNs(phase_2_begin_, phase_3_begin_);
    statistics::recovery::gct_phase_3_ns[worker_thread_id] += tsctime::TscDifferenceNs(phase_3_begin_, phase_4_begin_);
    statistics::recovery::gct_phase_4_ns[worker_thread_id] += tsctime::TscDifferenceNs(phase_4_begin_, phase_4_end_);
  }
}

void GroupCommitExecutor::PhaseOneWithoutThisGroup(wid_t gr_start_wid, wid_t gr_end_wid) {
  assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);
  if (keep_running_->load()) {
    InitializeRound();
    avoid_start_wid_ = gr_start_wid;
    avoid_end_wid_   = gr_end_wid;
    /* Retrieve a consistent copy of all loggers' state for RFA operations */
    for (auto w_i = 0UL; w_i < avoid_start_wid_; w_i++) { CollectConsistentState(w_i); }
    for (u32 w_i = avoid_end_wid_; w_i < FLAGS_worker_count; w_i++) { CollectConsistentState(w_i); }
  }
}

/**
 * @brief Phase description:
 *
 * - Phase one: Write the current log buffer to the storage
 * - Phase two: Flush (fdatasync) all logs to disk and acknowledge all log entries are flushed to the storage
 * - Phase three: Write all BLOBs of transactions to the storage
 * - Phase four: Flush those BLOBs and mark them ok for evicted
 * - Phase five: Notify which transactions are committed according to RFA
 */
void GroupCommitExecutor::ExecuteOneRound() {
  if (keep_running_->load()) {
    if (!already_prepared_) {
      InitializeRound();
      phase_1_begin_ = tsctime::ReadTSC();
      PhaseOne();
    } else {
      phase_1_begin_ = tsctime::ReadTSC();
      for (auto w_i = avoid_start_wid_; w_i < avoid_end_wid_; w_i++) { CollectConsistentState(w_i); }
    }
    phase_2_begin_ = tsctime::ReadTSC();
    PhaseTwo();
    phase_3_begin_ = tsctime::ReadTSC();
    if (FLAGS_blob_enable) { PhaseThree(); }
    phase_4_begin_ = tsctime::ReadTSC();
    PhaseFour();
    phase_4_end_ = tsctime::ReadTSC();
    CompleteRound();
  }
}

void GroupCommitExecutor::StartExecution() {
  while (keep_running_->load()) {
    try {
      ExecuteOneRound();
    } catch (...) {
      /**
       * @brief The Group-commit thread is detached,
       *  which means LeanStore may be stopped & deallocated while the Group commit is still running.
       * Therefore, the group commit may cause SEGFAULT while LeanStore is deallocating,
       *  and this try-catch block is to silent that run-time problem
       */
      return;
    }
  }
}

// -------------------------------------------------------------------------------------

void GroupCommitExecutor::PhaseOne() {
  /* Retrieve a consistent copy of all loggers' state for RFA operations */
  for (auto w_i = 0UL; w_i < FLAGS_worker_count; w_i++) { CollectConsistentState(w_i); }

  if (FLAGS_wal_centralized_buffer) {
    assert(FLAGS_txn_commit_variant == CommitProtocol::FLUSH_PIPELINING ||
           log_manager_->commit_latches_[0].LockState() == sync::HybridLatch::EXCLUSIVE);
    auto &centralized_buffer = *log_manager_->centralized_buf_;
    centralized_buffer.WriteLogBuffer(centralized_buffer.wal_cursor, BLK_BLOCK_SIZE, [&](u8 *buffer, u64 len) {
      auto offset = log_manager_->w_offset_.fetch_sub(len);
      auto ret    = pwrite(log_manager_->wal_fd_, buffer, len, offset - len);
      Ensure(ret == static_cast<int>(len));
    });
    centralized_buffer.write_cursor = centralized_buffer.wal_cursor;
    return;
  }

  /* Persist all WAL to SSD using async IO */
  if ((FLAGS_txn_commit_variant != CommitProtocol::WORKERS_WRITE_LOG) &&
      (FLAGS_txn_commit_variant != CommitProtocol::WILO_STEAL)) {
    for (auto w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
      log_manager_->logger_[w_i].log_buffer.WriteLogBuffer(worker_states_[w_i].last_wal_cursor, BLK_BLOCK_SIZE,
                                                           [&](u8 *buffer, u64 len) {
                                                             auto offset = log_manager_->w_offset_.fetch_sub(len);
                                                             PrepareWrite(buffer, len, offset - len);
                                                           });
    }
  }
}

void GroupCommitExecutor::PhaseTwo() {
  if (submitted_io_cnt_ > 0) {
    UringSubmit(&ring_, submitted_io_cnt_);
    submitted_io_cnt_ = 0;
  }
  if (FLAGS_wal_fsync) { Fsync(); }

  /* Acknowledge all log entries of other workers are flushed to the storage */
  if ((FLAGS_txn_commit_variant != CommitProtocol::WORKERS_WRITE_LOG) &&
      (FLAGS_txn_commit_variant != CommitProtocol::WILO_STEAL) && (!FLAGS_wal_centralized_buffer)) {
    for (size_t w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
      log_manager_->logger_[w_i].log_buffer.write_cursor.store(worker_states_[w_i].last_wal_cursor,
                                                               std::memory_order_release);
    }
  }
}

/**
 * @brief LargePage phase - writing and persisting all extents of transactions
 */
void GroupCommitExecutor::PhaseThree() {
  Ensure(FLAGS_blob_enable);
  for (size_t w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
    auto &logger = log_manager_->logger_[w_i];
    /**
     * @brief Persist all BLOBs for all transactions
     */
    if (FLAGS_txn_queue_size_mb > 0) {
      PREPARE_LARGE_PAGE_WRITE(logger.precommitted_queue, logger.precommitted_queue_rfa);
    } else {
      PREPARE_LARGE_PAGE_WRITE(logger.slow_pre_queue, logger.slow_pre_queue_rfa);
    }
  }

  if (!lp_req_.empty()) {
    Ensure(submitted_io_cnt_ > 0);
    UringSubmit(&ring_, submitted_io_cnt_);
    submitted_io_cnt_ = 0;
    if (FLAGS_wal_fsync) { Fsync(); }

    for (auto [start_pid, pg_cnt, extent_pid] : lp_req_) {
      if (!completed_lp_.contains(start_pid)) {
        completed_lp_.add(start_pid);
        buffer_->EvictExtent(extent_pid);
      }
    }
  }
}

void GroupCommitExecutor::PhaseFour() {
  for (size_t w_i = start_logger_id_; w_i < end_logger_id_; w_i++) {
    auto &logger = log_manager_->logger_[w_i];
    if (FLAGS_txn_queue_size_mb > 0) {
      COMPLETE_NORMAL_QUEUE(logger.precommitted_queue);
      COMPLETE_RFA_QUEUE(logger.precommitted_queue_rfa);
    } else {
      COMPLETE_NORMAL_QUEUE(logger.slow_pre_queue);
      COMPLETE_RFA_QUEUE(logger.slow_pre_queue_rfa);
    }
  }
}

// -------------------------------------------------------------------------------------

void GroupCommitExecutor::Fsync() { fdatasync(log_manager_->wal_fd_); }

void GroupCommitExecutor::CollectConsistentState(wid_t w_i) {
  auto &logger = log_manager_->logger_[w_i];
  if (FLAGS_txn_queue_size_mb > 0) {
    ready_to_commit_cut_[w_i]     = logger.precommitted_queue.SizeApprox();
    ready_to_commit_rfa_cut_[w_i] = logger.precommitted_queue_rfa.SizeApprox();
  } else {
    ready_to_commit_cut_[w_i]     = logger.slow_pre_queue.SizeApprox();
    ready_to_commit_rfa_cut_[w_i] = logger.slow_pre_queue_rfa.SizeApprox();
  }

  if (FLAGS_txn_commit_variant != CommitProtocol::WILO_STEAL) {
    worker_states_[w_i].Clone(log_manager_->w_state_[w_i]);
  } else {
    worker_states_[w_i].Clone(log_manager_->commit_state_[w_i]);
  }
  min_all_workers_gsn_ = std::min<timestamp_t>(min_all_workers_gsn_, worker_states_[w_i].last_gsn);
  max_all_workers_gsn_ = std::max<timestamp_t>(max_all_workers_gsn_, worker_states_[w_i].last_gsn);
  min_hardened_commit_ts_ =
    std::min<timestamp_t>(min_hardened_commit_ts_, worker_states_[w_i].precommitted_tx_commit_ts);
}

void GroupCommitExecutor::PrepareWrite(u8 *src, size_t size, size_t offset) {
  Ensure(u64(src) % BLK_BLOCK_SIZE == 0);
  Ensure(size % BLK_BLOCK_SIZE == 0);
  Ensure(offset % BLK_BLOCK_SIZE == 0);
  auto sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    UringSubmit(&ring_, submitted_io_cnt_);
    sqe = io_uring_get_sqe(&ring_);
    Ensure(sqe != nullptr);
    submitted_io_cnt_ = 0;
  }
  submitted_io_cnt_++;
  io_uring_prep_write(sqe, log_manager_->wal_fd_, src, size, offset);
}

template <class T>
void GroupCommitExecutor::PrepareLargePageWrite(T &txn) {
  Ensure(txn.ToFlushedLargePages().size() == txn.ToEvictedExtents().size());

  for (auto &[pid, pg_cnt] : txn.ToFlushedLargePages()) {
    if (!already_prep_.contains(pid)) {
      already_prep_.add(pid);
      if (FLAGS_blob_normal_buffer_pool) {
        for (auto id = pid; id < pid + pg_cnt; id++) {
          PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(id)), PAGE_SIZE, id * PAGE_SIZE);
        }
      } else {
        PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(pid)), pg_cnt * PAGE_SIZE, pid * PAGE_SIZE);
      }
    }
  }

  /* Update large-page requirements for txn commit */
  for (size_t idx = 0; idx < txn.ToFlushedLargePages().size(); idx++) {
    auto &[pid, pg_cnt] = txn.ToFlushedLargePages()[idx];
    auto &extent_pid    = txn.ToEvictedExtents()[idx];
    if (start_profiling) { statistics::blob::blob_logging_io += pg_cnt * PAGE_SIZE; }
    lp_req_.emplace_back(pid, pg_cnt, extent_pid);
  }
}

template <class T>
void GroupCommitExecutor::CompleteTransaction(T &txn) {
  Ensure(txn.state == transaction::Transaction::State::READY_TO_COMMIT);
  if (FLAGS_blob_enable) {
    for (auto &lp : txn.ToFlushedLargePages()) { completed_lp_.remove(lp.start_pid); }
    buffer_->FreeStorageManager()->PublicFreeExtents(txn.ToFreeExtents());
  }
  transaction::TransactionManager::DurableCommit(txn, phase_2_begin_);
}

template void GroupCommitExecutor::PrepareLargePageWrite<transaction::Transaction>(transaction::Transaction &);
template void GroupCommitExecutor::PrepareLargePageWrite<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &);
template void GroupCommitExecutor::CompleteTransaction<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &);
template void GroupCommitExecutor::CompleteTransaction<transaction::Transaction>(transaction::Transaction &);

}  // namespace leanstore::recovery
