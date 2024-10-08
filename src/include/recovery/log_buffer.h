#pragma once

#include "common/typedefs.h"
#include "recovery/log_entry.h"
#include "sync/hybrid_latch.h"

#include <atomic>
#include <functional>
#include <mutex>

namespace leanstore::recovery {

struct LogWorker;

/**
 * Log buffer management
 * The wal_buffer forms a circular queue, and (wal_cursor, write_cursor) act as (first, last)
 *
 * i.e. there are three cases:
 * - wal_cursor == write_cursor: The group committer flushes all the logs of the current buffer
 * - wal_cursor > write_cursor:
 *    Group committer already flushed log stored in [0: write_cursor],
 *      and the logs (write_cursor: wal_cursor] are not persisted yet
 *    In this case, the current log worker can append new logs to
 *      [wal_cursor: WAL_BUFFER_SIZE) and [0: write_cursor]
 * - wal_cursor < write_cursor:
 *    While group committed was flushing the logs,
 *      current log worker already reaches the log limit and has to circulate back
 *    Then, `wal_cursor_circ` will be set to true
 *    In this case, the group committer is allowed to flush all the remaining logs,
 *      i.e. logs stored in (write_cursor: WAL_BUFFER_SIZE],
 *      and then circulate back to the beginning and set `wal_cursor_circ` to false
 *
 * Constraints:
 * - When `wal_cursor_circ` == false: Assert(wal_cursor >= write_cursor);
 * - When `wal_cursor_circ` == true:  Assert(wal_cursor <= write_cursor);
 *    We shouldn't allow wal_cursor to be higher than write_cursor in this case,
 *      otherwise some logs won't be persisted
 *    Note that, the reverse assertions also work,
 *      i.e. if (wal_cursor <= write_cursor) { assert(`wal_cursor_circ` == true) }
 * Note: `wal_cursor_circ` is stored as the most significant bit of write_cursor
 *
 * Improvement:
 * - It's possible to 100% eliminate `wal_cursor_circ` because:
 *  + If wal_cursor >= write_cursor, then wal_cursor's upper limit is WAL_BUFFER_SIZE
 *  + In the opposite case (i.e. wal_cursor < write_cursor), then write_cursor becomes
 *      the new wal_cursor's upper limit.
 *    Only until write_cursor circulate back to the beginning, we fall back into the 1st case
 *
 * Log types:
 * - There are two types of logs, LogMetaEntry & DataLog
 *  + LogMetaEntry stores metadata, i.e. TX_START, TX_COMMIT, ....
 *    size of LogMetaEntry is always fixed
 *  + DataEntry stores the log of data, and its type is always DATA_ENTRY
 *    size of DataEntry varies and is dependent on the logged data
 *
 * Log flow:
 * - When a new transaction starts, insert LogMetaEntry(TX_START) to the wal
 * - During the lifetime of that transaction, insert DataEntry(DATA_ENTRY) to the wal
 * - If the active transaction aborts/commits, insert LogMetaEntry(TX_START/TX_ABORT)
 * - When the wal_buffer (i.e. impl as circular queue) is full (i.e. reaches the end),
 *    LogMetaEntry(CARRIAGE_RETURN) is inserted, then circulates wal_cursor back to the beginning
 */
struct LogBuffer {
  static constexpr u64 CR_ENTRY_SIZE = sizeof(LogMetaEntry);

  /* Buffer management */
  u64 wal_cursor                = 0;  // the worker's current wal cursor on wal_buffer
  std::atomic<u64> write_cursor = 0;  // the latest log cursor already written to the storage
  u8 *wal_buffer;

  /* Env */
  std::atomic<bool> *is_running;

  LogBuffer(u64 buffer_size, std::atomic<bool> *db_is_running);
  ~LogBuffer();

  auto Current() -> u8 * { return &wal_buffer[wal_cursor]; }

  /* Buffer management */
  auto TotalFreeSpace() -> u64;
  auto ContiguousFreeSpaceForNewEntry() -> u64;
  void EnsureEnoughSpace(LogWorker *owner, u64 requested_size);

  /* Log write utilities */
  void PersistLog(LogWorker *owner, uint64_t align_size, const std::function<void()> &async_fn = {});
  void WriteLogBuffer(u64 wal_cursor, uint64_t align_size, const std::function<void(u8 *, u64)> &write_fn);
};

}  // namespace leanstore::recovery