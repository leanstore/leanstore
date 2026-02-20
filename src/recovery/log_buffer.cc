#include "recovery/log_buffer.h"
#include "common/exceptions.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "sync/hybrid_guard.h"
#include "transaction/transaction.h"

namespace leanstore::recovery {

LogBuffer::LogBuffer(u64 buffer_size, std::atomic<bool> *db_is_running) : is_running(db_is_running) {
  wal_buffer = static_cast<u8 *>(AllocHuge(buffer_size));
}

// For desc, check log_buffer.h
auto LogBuffer::TotalFreeSpace() -> u64 {
  auto curr_write_cursor = write_cursor.load();
  if (curr_write_cursor == wal_cursor) { return FLAGS_wal_buffer_size_mb * MB; }
  if (wal_cursor > curr_write_cursor) { return curr_write_cursor + (FLAGS_wal_buffer_size_mb * MB - wal_cursor); }
  return curr_write_cursor - wal_cursor;
}

auto LogBuffer::ContiguousFreeSpaceForNewEntry() -> u64 {
  const auto curr_write_cursor = write_cursor.load();
  // circulate the wal_cursor to the beginning and insert the whole entry
  return (wal_cursor < curr_write_cursor) ? curr_write_cursor - wal_cursor : FLAGS_wal_buffer_size_mb * MB - wal_cursor;
}

void LogBuffer::EnsureEnoughSpace(LogWorker *owner, u64 requested_size) {
  // Beside the CR log entry, every write is possibly coupled with a WRITE_METADATA log entry
  auto prepared_space_for_new_entry = requested_size + CR_ENTRY_SIZE + sizeof(LogMetaEntry);
  if ((FLAGS_wal_buffer_size_mb * MB - wal_cursor) < prepared_space_for_new_entry) {
    // the wal buffer doesn't have enough free space for the log
    //  hence we add padding to the end, then insert the entry to the beginning
    prepared_space_for_new_entry += FLAGS_wal_buffer_size_mb * MB - wal_cursor;
  }
  while ((TotalFreeSpace() < prepared_space_for_new_entry) && (is_running->load())) {
    /* For autonomous commit, it should be very rarely to be here */
    LogFlush(owner, false);
    {
      /* Public its consistent state */
      owner->PublicCommitTS();
      auto &commit_state = owner->log_manager->commit_state_[LeanStore::worker_thread_id];
      commit_state.SyncClone(*owner->w_state);
    }
  }
  // Always ensure that we can put one CR entry at the end of the wal_buffer
  if (ContiguousFreeSpaceForNewEntry() < requested_size + CR_ENTRY_SIZE) {
    assert(CR_ENTRY_SIZE <= FLAGS_wal_buffer_size_mb * MB - wal_cursor);
    // the remaining space on wal_buffer is not enough, then we insert padding
    //  in order to insert the new log entry at the beginning of the wal buffer
    auto entry  = new (&wal_buffer[wal_cursor]) LogEntry();
    entry->type = LogEntry::Type::CARRIAGE_RETURN;
    entry->size = FLAGS_wal_buffer_size_mb * MB - wal_cursor;
    if (FLAGS_wal_debug) { entry->ComputeChksum(); }
    wal_cursor = 0;
  }
  assert(ContiguousFreeSpaceForNewEntry() >= requested_size);
  assert(wal_cursor + requested_size + CR_ENTRY_SIZE <= FLAGS_wal_buffer_size_mb * MB);
}

void LogBuffer::WriteLogBuffer(u64 wal_cursor, uint64_t alignment, const std::function<void(u8 *, u64)> &write_fn) {
  auto wal_written_cursor = write_cursor.load();

  if (wal_cursor > wal_written_cursor) {
    const auto lower_offset = DownAlign(wal_written_cursor, alignment);
    const auto upper_offset = UpAlign(wal_cursor, alignment);
    auto size_aligned       = upper_offset - lower_offset;
    write_fn(wal_buffer + lower_offset, size_aligned);
    if (start_profiling) {
      statistics::recovery::real_log_bytes[LeanStore::worker_thread_id] += wal_cursor - wal_written_cursor;
      statistics::recovery::written_log_bytes[LeanStore::worker_thread_id] += size_aligned;
    }
  } else if (wal_cursor < wal_written_cursor) {
    {
      const u64 lower_offset = DownAlign(wal_written_cursor, alignment);
      const u64 upper_offset = FLAGS_wal_buffer_size_mb * MB;
      auto size_aligned      = upper_offset - lower_offset;
      write_fn(wal_buffer + lower_offset, size_aligned);
      if (start_profiling) {
        statistics::recovery::real_log_bytes[LeanStore::worker_thread_id] +=
          FLAGS_wal_buffer_size_mb * MB - wal_written_cursor;
        statistics::recovery::written_log_bytes[LeanStore::worker_thread_id] += size_aligned;
      }
    }
    {
      const u64 lower_offset  = 0;
      const u64 upper_offset  = UpAlign(wal_cursor, alignment);
      const auto size_aligned = upper_offset - lower_offset;
      write_fn(wal_buffer, size_aligned);
      if (start_profiling) {
        statistics::recovery::real_log_bytes[LeanStore::worker_thread_id] += wal_cursor;
        statistics::recovery::written_log_bytes[LeanStore::worker_thread_id] += size_aligned;
      }
    }
  }
}

/**
 * @brief For decentralized commit protocol only.
 * Current worker actively write its local log buffer to the storage
 */
auto LogBuffer::LogFlush(LogWorker *worker, bool is_async, const std::function<void()> &async_fn) -> u64 {
  auto wal_written_cursor = write_cursor.load();
  auto req_lsn            = 0UL;

  // Clean log buffer, nothing to write
  if (wal_cursor != wal_written_cursor) {
    auto log_size = LogBuffer::DirtyLogSize(wal_cursor, wal_written_cursor) + sizeof(LogMetaEntry);

    /* Copy the dirty logs to the write buffer */
    write_blk.Serialize(
      log_size,
      [&](u8 *fill_ptr, u64 &offset) {
        if (wal_cursor > wal_written_cursor) {
          std::memcpy(fill_ptr, wal_buffer + wal_written_cursor, wal_cursor - wal_written_cursor);
          offset = wal_cursor - wal_written_cursor;
        } else if (wal_cursor < wal_written_cursor) {
          {
            std::memcpy(fill_ptr, wal_buffer + wal_written_cursor, FLAGS_wal_buffer_size_mb * MB - wal_written_cursor);
            offset = FLAGS_wal_buffer_size_mb * MB - wal_written_cursor;
          }
          {
            std::memcpy(fill_ptr + offset, wal_buffer, wal_cursor);
            offset += wal_cursor;
          }
        }
      },
      [worker, is_async, &async_fn, &req_lsn](u8 *buffer, u64 size) {
        worker->backend.SyncAppend(buffer, size, async_fn);
      });
    write_cursor = wal_cursor;
  }
  if (!is_async) { worker->PublicCommitTS(); }
  return req_lsn;
}

}  // namespace leanstore::recovery