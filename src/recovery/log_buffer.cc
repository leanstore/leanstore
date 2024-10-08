#include "recovery/log_buffer.h"
#include "common/exceptions.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "sync/hybrid_guard.h"
#include "transaction/transaction.h"

using leanstore::transaction::CommitProtocol;

namespace leanstore::recovery {

LogBuffer::LogBuffer(u64 buffer_size, std::atomic<bool> *db_is_running) : is_running(db_is_running) {
  wal_buffer = static_cast<u8 *>(aligned_alloc(BLK_BLOCK_SIZE, buffer_size));
}

LogBuffer::~LogBuffer() { free(wal_buffer); }

// For desc, check log_buffer.h
auto LogBuffer::TotalFreeSpace() -> u64 {
  auto curr_write_cursor = write_cursor.load();
  if (curr_write_cursor == wal_cursor) { return FLAGS_wal_buffer_size_mb * MB; }
  if (wal_cursor > curr_write_cursor) { return curr_write_cursor + (FLAGS_wal_buffer_size_mb * MB - wal_cursor); }
  // wal_cursor < curr_write_cursor
  return curr_write_cursor - wal_cursor;
}

auto LogBuffer::ContiguousFreeSpaceForNewEntry() -> u64 {
  const auto curr_write_cursor = write_cursor.load();
  // circulate the wal_cursor to the beginning and insert the whole entry
  return (wal_cursor < curr_write_cursor) ? curr_write_cursor - wal_cursor : FLAGS_wal_buffer_size_mb * MB - wal_cursor;
}

void LogBuffer::EnsureEnoughSpace(LogWorker *owner, u64 requested_size) {
  auto prepared_space_for_new_entry = requested_size + CR_ENTRY_SIZE;
  if ((FLAGS_wal_buffer_size_mb * MB - wal_cursor) < prepared_space_for_new_entry) {
    // the wal buffer doesn't have enough free space for the log
    //  hence we add padding to the end, then insert the entry to the beginning
    prepared_space_for_new_entry += FLAGS_wal_buffer_size_mb * MB - wal_cursor;
  }
  while ((TotalFreeSpace() < prepared_space_for_new_entry) && (is_running->load())) {
    /* For decentralized logging variants, it should be very rarely to be here */
    switch (FLAGS_txn_commit_variant) {
      case ToUnderlying(CommitProtocol::BASELINE_COMMIT): owner->log_manager->TriggerGroupCommit(0); break;
      case ToUnderlying(CommitProtocol::WORKERS_WRITE_LOG): PersistLog(owner, BLK_BLOCK_SIZE); break;
      case ToUnderlying(CommitProtocol::WILO_STEAL):
        PersistLog(owner, BLK_BLOCK_SIZE);

        {
          /* Public its consistent state */
          owner->PublicCommitTS();
          auto &commit_state = owner->log_manager->commit_state_[worker_thread_id];
          commit_state.SyncClone(*owner->w_state);
        }
        break;
      default: AsmYield(); break;
    }
  }
  // Always ensure that we can put one CR entry at the end of the wal_buffer
  if (ContiguousFreeSpaceForNewEntry() < requested_size + CR_ENTRY_SIZE) {
    assert(CR_ENTRY_SIZE <= FLAGS_wal_buffer_size_mb * MB - wal_cursor);
    // the remaining space on wal_buffer is not enough, then we insert padding
    //  in order to insert the new log entry at the beginning of the wal buffer
    auto entry  = new (&wal_buffer[wal_cursor]) LogMetaEntry();
    entry->type = LogEntry::Type::CARRIAGE_RETURN;
    entry->size = FLAGS_wal_buffer_size_mb * MB - wal_cursor;
    if (FLAGS_wal_debug) { entry->ComputeChksum(); }
    wal_cursor = 0;
  }
  assert(ContiguousFreeSpaceForNewEntry() >= requested_size);
  assert(wal_cursor + requested_size + CR_ENTRY_SIZE <= FLAGS_wal_buffer_size_mb * MB);
}

/**
 * @brief For decentralized commit protocol only.
 * Current worker actively write its local log buffer to the storage
 */
void LogBuffer::PersistLog(LogWorker *worker, uint64_t align_size, const std::function<void()> &async_fn) {
  auto *ring      = &(worker->local_ring);
  auto submit_cnt = 0U;
  WriteLogBuffer(wal_cursor, align_size, [&](u8 *buffer, u64 size) {
    auto offset = worker->log_manager->w_offset_.fetch_sub(size, std::memory_order_release);
    auto sqe    = io_uring_get_sqe(ring);
    assert(sqe != nullptr);
    submit_cnt++;
    io_uring_prep_write(sqe, worker->log_manager->wal_fd_, buffer, size, offset - size);
  });
  write_cursor = wal_cursor;
  UringSubmit(ring, submit_cnt, async_fn);
  worker->PublicCommitTS();
  worker->TryCommitRFATxns();
}

void LogBuffer::WriteLogBuffer(u64 wal_cursor, uint64_t align_size, const std::function<void(u8 *, u64)> &write_fn) {
  auto wal_written_cursor = write_cursor.load();

  if (wal_cursor > wal_written_cursor) {
    const auto lower_offset = DownAlign(wal_written_cursor, align_size);
    const auto upper_offset = UpAlign(wal_cursor, align_size);
    auto size_aligned       = upper_offset - lower_offset;
    write_fn(wal_buffer + lower_offset, size_aligned);
    if (start_profiling) {
      statistics::recovery::real_log_bytes[worker_thread_id] += wal_cursor - wal_written_cursor;
      statistics::recovery::written_log_bytes[worker_thread_id] += size_aligned;
    }
  } else if (wal_cursor < wal_written_cursor) {
    {
      const u64 lower_offset = DownAlign(wal_written_cursor, align_size);
      const u64 upper_offset = FLAGS_wal_buffer_size_mb * MB;
      auto size_aligned      = upper_offset - lower_offset;
      write_fn(wal_buffer + lower_offset, size_aligned);
      if (start_profiling) {
        statistics::recovery::real_log_bytes[worker_thread_id] += FLAGS_wal_buffer_size_mb * MB - wal_written_cursor;
        statistics::recovery::written_log_bytes[worker_thread_id] += size_aligned;
      }
    }
    {
      const u64 lower_offset  = 0;
      const u64 upper_offset  = UpAlign(wal_cursor, align_size);
      const auto size_aligned = upper_offset - lower_offset;
      write_fn(wal_buffer, size_aligned);
      if (start_profiling) {
        statistics::recovery::real_log_bytes[worker_thread_id] += wal_cursor;
        statistics::recovery::written_log_bytes[worker_thread_id] += size_aligned;
      }
    }
  }
}

}  // namespace leanstore::recovery