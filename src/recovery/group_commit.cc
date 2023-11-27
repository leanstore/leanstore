#include "recovery/group_commit.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <tuple>

namespace leanstore::recovery {

GroupCommitExecutor::GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager,
                                         std::atomic<bool> &bg_threads_keep_running)
    : buffer_(buffer),
      keep_running_(&bg_threads_keep_running),
      all_loggers_(log_manager->logger_),
      ready_to_commit_cut_(FLAGS_worker_count),
      ready_to_commit_rfa_cut_(FLAGS_worker_count),
      worker_states_(FLAGS_worker_count) {
  pthread_setname_np(pthread_self(), "group_committer");
  std::tie(wal_fd_, w_offset_) = buffer_->GetWalInfo();
  int ret                      = io_uring_queue_init(FLAGS_wal_max_qd, &ring_, IORING_SETUP_SINGLE_ISSUER);
  if (ret != 0) { throw std::runtime_error("GroupCommit: io_uring_queue_init error"); }
}

/**
 * @brief Reset GroupCommitExecutor's state every round
 */
void GroupCommitExecutor::InitializeRound() {
  round_++;
  lp_req_.clear();
  already_prep_.clear();
  has_blob_               = false;
  submitted_io_cnt_       = 0;
  completed_txn_          = 0;
  min_all_workers_gsn_    = std::numeric_limits<logid_t>::max();
  max_all_workers_gsn_    = 0;
  min_hardened_commit_ts_ = std::numeric_limits<timestamp_t>::max();
}

void GroupCommitExecutor::CompleteRound() {
  // Update RFA env
  assert(LogManager::global_min_gsn_flushed.load() <= min_all_workers_gsn_);
  LogManager::global_min_gsn_flushed.store(min_all_workers_gsn_, std::memory_order_release);
  LogManager::global_sync_to_this_gsn.store(max_all_workers_gsn_, std::memory_order_release);

  // Update statistics
  if (start_profiling) {
    statistics::txn_processed += completed_txn_;
    statistics::recovery::gct_phase_1_ms +=
      std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end_ - phase_1_begin_).count();
    statistics::recovery::gct_phase_2_ms +=
      (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end_ - phase_2_begin_).count());
    statistics::recovery::gct_phase_3_ms +=
      (std::chrono::duration_cast<std::chrono::microseconds>(phase_3_end_ - phase_3_begin_).count());
  }
}

void GroupCommitExecutor::PrepareWrite(u8 *src, size_t size, size_t offset) {
  Ensure(u64(src) % BLK_BLOCK_SIZE == 0);
  Ensure(size % BLK_BLOCK_SIZE == 0);
  Ensure(offset % BLK_BLOCK_SIZE == 0);
  auto sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    HotUringSubmit();
    sqe = io_uring_get_sqe(&ring_);
    Ensure(sqe != nullptr);
  }
  submitted_io_cnt_++;
  io_uring_prep_write(sqe, wal_fd_, src, size, offset);
}

void GroupCommitExecutor::ExecuteOneRound() {
  assert(keep_running_->load());

  InitializeRound();
  phase_1_begin_ = std::chrono::high_resolution_clock::now();
  PhaseOne();
  phase_2_begin_ = phase_1_end_ = std::chrono::high_resolution_clock::now();
  PhaseTwo();
  phase_2_end_ = phase_3_begin_ = std::chrono::high_resolution_clock::now();
  PhaseThree();
  phase_3_end_ = phase_4_begin_ = std::chrono::high_resolution_clock::now();
  PhaseFour();
  phase_4_end_ = phase_5_begin_ = std::chrono::high_resolution_clock::now();
  PhaseFive();
  phase_5_end_ = std::chrono::high_resolution_clock::now();
  CompleteRound();
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
/**
 * @brief 1st Phase:
 * - Retrieve state of all workers for RFA operations
 * - Write WAL logs to WAL file using AsyncIO
 * - Write Blob to main DB file
 */
void GroupCommitExecutor::PhaseOne() {
  for (size_t w_i = 0; w_i < FLAGS_worker_count; w_i++) {
    auto &logger = all_loggers_[w_i];
    {
      /**
       * Retrieve a consistent copy of all loggers' state for RFA operations
       */
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        ready_to_commit_cut_[w_i]     = logger.precommitted_queue.size();
        ready_to_commit_rfa_cut_[w_i] = logger.precommitted_queue_rfa.size();
      }

      worker_states_[w_i].Clone(logger.w_state);
      min_all_workers_gsn_ = std::min<logid_t>(min_all_workers_gsn_, worker_states_[w_i].last_gsn);
      max_all_workers_gsn_ = std::max<logid_t>(max_all_workers_gsn_, worker_states_[w_i].last_gsn);
      min_hardened_commit_ts_ =
        std::min<timestamp_t>(min_hardened_commit_ts_, worker_states_[w_i].precommitted_tx_commit_ts);
    }
    /**
     * Persist all WAL to SSD using async IO
     */
    auto wal_gct_cursor = logger.gct_cursor.load();
    if (worker_states_[w_i].w_written_offset > wal_gct_cursor) {
      const auto lower_offset = DownAlign(wal_gct_cursor);
      const auto upper_offset = UpAlign(worker_states_[w_i].w_written_offset);
      Ensure(upper_offset <= LogWorker::WAL_BUFFER_SIZE);
      auto size_aligned = upper_offset - lower_offset;
      PrepareWrite(logger.wal_buffer + lower_offset, size_aligned, w_offset_);
      w_offset_ += size_aligned;
      if (start_profiling) { statistics::recovery::gct_write_bytes += size_aligned; }
    } else if (worker_states_[w_i].w_written_offset < wal_gct_cursor) {
      {
        const u64 lower_offset = DownAlign(wal_gct_cursor);
        const u64 upper_offset = LogWorker::WAL_BUFFER_SIZE;
        auto size_aligned      = upper_offset - lower_offset;
        PrepareWrite(logger.wal_buffer + lower_offset, size_aligned, w_offset_);
        w_offset_ += size_aligned;
        if (start_profiling) { statistics::recovery::gct_write_bytes += size_aligned; }
      }
      {
        const u64 lower_offset  = 0;
        const u64 upper_offset  = UpAlign(worker_states_[w_i].w_written_offset);
        const auto size_aligned = upper_offset - lower_offset;
        PrepareWrite(logger.wal_buffer, size_aligned, w_offset_);
        w_offset_ += size_aligned;
        if (start_profiling) { statistics::recovery::gct_write_bytes += size_aligned; }
      }
    }
  }
}

/**
 * @brief IO-uring prevents users from creating extra submission queue when the ring's capacity is full
 *      Please check https://man7.org/linux/man-pages/man3/io_uring_get_sqe.3.html - io_uring_get_sqe() return nullptr
 *      Therefore, when this case happens, we must submit the current ring before registering new submission queue
 */
void GroupCommitExecutor::HotUringSubmit() {
  if (submitted_io_cnt_ > 0) {
    auto ret = io_uring_submit_and_wait(&ring_, submitted_io_cnt_);
    io_uring_cq_advance(&ring_, submitted_io_cnt_);
    Ensure(ret == static_cast<int>(submitted_io_cnt_));
    submitted_io_cnt_ = 0;
  }
}

/**
 * @brief Flush all logs to disk
 */
void GroupCommitExecutor::PhaseTwo() {
  HotUringSubmit();

  if (FLAGS_wal_fsync) { fdatasync(wal_fd_); }
}

void GroupCommitExecutor::PhaseThree() {
  for (size_t w_i = 0; w_i < FLAGS_worker_count; w_i++) {
    auto &logger = all_loggers_[w_i];
    /**
     * @brief Persist all BLOBs for all transactions
     */
    if (FLAGS_blob_logging_variant >= 0) {
      for (size_t tx_i = 0; tx_i < ready_to_commit_cut_[w_i]; tx_i++) {
        PrepareLargePageWrite(logger.precommitted_queue[tx_i]);
      }
      for (size_t tx_i = 0; tx_i < ready_to_commit_rfa_cut_[w_i]; tx_i++) {
        PrepareLargePageWrite(logger.precommitted_queue_rfa[tx_i]);
      }
    }
  }
}

void GroupCommitExecutor::PhaseFour() {
  if (has_blob_) {
    HotUringSubmit();
    if (FLAGS_wal_fsync) { fdatasync(wal_fd_); }

    if (FLAGS_blob_logging_variant >= 0) {
      for (auto [start_pid, pg_cnt, extent_pid, extent_sz] : lp_req_) {
        if (!completed_lp_.contains(start_pid)) {
          completed_lp_.insert(start_pid);
          buffer_->EvictExtent(extent_pid, extent_sz);
        }
      }
    }
  }
}

void GroupCommitExecutor::PhaseFive() {
  for (size_t w_i = 0; w_i < FLAGS_worker_count; w_i++) {
    auto &logger = all_loggers_[w_i];

    logger.gct_cursor.store(worker_states_[w_i].w_written_offset, std::memory_order_release);
    // -------------------------------------------------------------------------------------
    // Commit transactions in precommitted_queue
    u64 tx_i = 0;
    for (tx_i = 0; tx_i < ready_to_commit_cut_[w_i] &&
                   logger.precommitted_queue[tx_i].max_observed_gsn_ <= min_all_workers_gsn_ &&
                   logger.precommitted_queue[tx_i].StartTS() <= min_hardened_commit_ts_;
         tx_i++) {
      CompleteTxnBlobs(logger.precommitted_queue[tx_i]);
      logger.precommitted_queue[tx_i].state_ = transaction::Transaction::State::COMMITTED;
      buffer_->GetFreePageManager()->PublicFreeRanges(logger.precommitted_queue[tx_i].ToFreeExtents());
    }
    Ensure(tx_i <= ready_to_commit_cut_[w_i]);
    if (tx_i > 0) {
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        logger.precommitted_queue.erase(logger.precommitted_queue.begin(), logger.precommitted_queue.begin() + tx_i);
      }
      completed_txn_ += tx_i;
    }
    // -------------------------------------------------------------------------------------
    // Commit transactions in precommitted_queue_rfa
    for (tx_i = 0; tx_i < ready_to_commit_rfa_cut_[w_i]; tx_i++) {
      CompleteTxnBlobs(logger.precommitted_queue_rfa[tx_i]);
      logger.precommitted_queue_rfa[tx_i].state_ = transaction::Transaction::State::COMMITTED;
      buffer_->GetFreePageManager()->PublicFreeRanges(logger.precommitted_queue_rfa[tx_i].ToFreeExtents());
    }
    if (tx_i > 0) {
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        logger.precommitted_queue_rfa.erase(logger.precommitted_queue_rfa.begin(),
                                            logger.precommitted_queue_rfa.begin() + tx_i);
      }
      completed_txn_ += tx_i;
    }
  }
}

// -------------------------------------------------------------------------------------
void GroupCommitExecutor::PrepareLargePageWrite(transaction::Transaction &txn) {
  assert(FLAGS_blob_logging_variant >= 0);
  Ensure(txn.ToFlushedLargePages().size() == txn.ToEvictedExtents().size());
  if (txn.ToFlushedLargePages().size() > 0) { has_blob_ = true; }

  for (auto &[pid, pg_cnt] : txn.ToFlushedLargePages()) {
    if (!already_prep_.contains(pid)) {
      already_prep_.emplace(pid);
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
    auto &[pid, pg_cnt]           = txn.ToFlushedLargePages()[idx];
    auto &[extent_pid, extent_sz] = txn.ToEvictedExtents()[idx];
    if (start_profiling) { statistics::blob::blob_logging_io += pg_cnt * PAGE_SIZE; }
    lp_req_.emplace_back(pid, pg_cnt, extent_pid, extent_sz);
  }
}

void GroupCommitExecutor::CompleteTxnBlobs(transaction::Transaction &txn) {
  if (FLAGS_blob_logging_variant >= 0) {
    for (auto &lp : txn.ToFlushedLargePages()) { completed_lp_.erase(lp.start_pid); }
  }
}

}  // namespace leanstore::recovery
