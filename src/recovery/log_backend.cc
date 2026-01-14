#include "recovery/log_backend.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"

#include "share_headers/time.h"

namespace leanstore::recovery {

LogBackend::LogBackend(LogManager *log_manager) : manager_(log_manager) {}

void LogBackend::Connect() {
  int ret = io_uring_queue_init(LOGGING_AIO_QD, &ring_, storage::LibaioInterface::uring_flags);
  if (ret != 0) { throw ex::EnsureFailed("LogWorker: io_uring_queue_init error " + std::to_string(ret)); }
}

/**
 * @brief For autonomous commit protocol only.
 * Current worker synchronously write its local log buffer to the storage
 */
void LogBackend::SyncAppend(u8 *buffer, u64 size, const std::function<void()> &async_fn) {
  u64 start_time = 0;
  if (FLAGS_txn_debug && start_profiling_latency) { start_time = tsctime::ReadTSC(); }
  auto offset = LogFlushOffset(size);
  auto sqe    = io_uring_get_sqe(&ring_);
  assert(sqe != nullptr);
  io_uring_prep_write(sqe, manager_->wal_fd_, buffer, size, offset - size);
  UringSubmit(&ring_, 1, async_fn);
  if (FLAGS_txn_debug && start_profiling_latency && start_time > 0) {
    auto end_time = tsctime::ReadTSC();
    statistics::io_latency[LeanStore::worker_thread_id].emplace_back(tsctime::TscDifferenceNs(start_time, end_time));
  }
  if (FLAGS_txn_debug) { statistics::log_flush_cnt[LeanStore::worker_thread_id]++; }
}

auto LogBackend::LogFlushOffset(u64 chunk_size) -> u64 {
  assert((log_block_.start_offset <= log_block_.end_offset) && (chunk_size % BLK_BLOCK_SIZE == 0));
  if (log_block_.end_offset - log_block_.start_offset < chunk_size) {
    log_block_.end_offset   = manager_->w_offset_.fetch_sub(manager_->wal_block_size_, std::memory_order_release);
    log_block_.start_offset = log_block_.end_offset - manager_->wal_block_size_;
  }
  Ensure(log_block_.end_offset - log_block_.start_offset >= chunk_size);
  auto ret_offset = log_block_.end_offset;
  log_block_.end_offset -= chunk_size;
  return ret_offset;
}

}  // namespace leanstore::recovery