#pragma once

#include "common/typedefs.h"
#include "recovery/log_entry.h"
#include "sync/hybrid_latch.h"
#include "transaction/transaction.h"

#include <atomic>
#include <deque>
#include <functional>
#include <mutex>

namespace leanstore::recovery {

/**
 * Abbreviations:
 *  rfa: Remote flush avoidance
 *  lsn: Log sequence number
 *  gsn: Global sequence number
 *  gct: The Group-Commit thread worker
 */
struct LogWorker {
  inline static constexpr u64 WAL_BUFFER_SIZE = 10 * MB;
  inline static constexpr u64 CR_ENTRY_SIZE   = sizeof(LogMetaEntry);

  // Current active log entry
  std::atomic<bool> *is_running = nullptr;
  LogEntry *active_log          = nullptr;

  // Queue to store all precommitted transactions
  std::deque<transaction::Transaction> precommitted_queue;      // Needs_remote_flush = true
  std::deque<transaction::Transaction> precommitted_queue_rfa;  // Logs can be flushed immediately
  std::mutex precommitted_queue_mutex;

  /**
   * Provide a consistent state of this local worker for the group committer,
   *  which should be as minimal as possible to reduce the contention between two threads
   */
  struct WorkerConsistentState {
    u64 w_written_offset                  = 0;  // w_cursor
    logid_t last_gsn                      = 0;  // w_gsn_clock
    timestamp_t precommitted_tx_commit_ts = 0;
    sync::HybridLatch latch;

    auto Clone(WorkerConsistentState &other) -> WorkerConsistentState &;
  } w_state;

  /**
   * Log entries management
   * The wal_buffer forms a circular queue, and (w_cursor, gct_cursor) act as (first, last)
   * i.e. there are three cases:
   * - w_cursor == gct_cursor: The group committer flushes all the logs of the current buffer
   * - w_cursor > gct_cursor:
   *    Group committer already flushed log stored in [0: gct_cursor],
   *      and the logs (gct_cursor: w_cursor] are not persisted yet
   *    In this case, the current log worker can append new logs to
   *      [w_cursor: WAL_BUFFER_SIZE) and [0: gct_cursor]
   * - w_cursor < gct_cursor:
   *    While group committed was flushing the logs,
   *      current log worker already reaches the log limit and has to circulate back
   *    Then, `w_cursor_circ` will be set to true
   *    In this case, the group committer is allowed to flush all the remaining logs,
   *      i.e. logs stored in (gct_cursor: WAL_BUFFER_SIZE],
   *      and then circulate back to the beginning and set `w_cursor_circ` to false
   *
   * Constraints:
   * - When `w_cursor_circ` == false: Assert(w_cursor >= gct_cursor);
   * - When `w_cursor_circ` == true:  Assert(w_cursor <= gct_cursor);
   *    We shouldn't allow w_cursor to be higher than gct_cursor in this case,
   *      otherwise some logs won't be persisted
   *    Note that, the reverse assertions also work,
   *      i.e. if (w_cursor <= gct_cursor) { assert(`w_cursor_circ` == true) }
   * Note: `w_cursor_circ` is stored as the most significant bit of gct_cursor
   *
   * Improvement:
   * - It's possible to 100% eliminate `w_cursor_circ` because:
   *  + If w_cursor >= gct_cursor, then w_cursor's upper limit is WAL_BUFFER_SIZE
   *  + In the opposite case (i.e. w_cursor < gct_cursor), then gct_cursor becomes
   *      the new w_cursor's upper limit.
   *    Only until gct_cursor circulate back to the beginning, we fall back into the 1st case
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
   *    LogMetaEntry(CARRIAGE_RETURN) is inserted, then circulates w_cursor back to the beginning
   */
  u64 w_cursor                = 0;  // the worker's current wal cursor on wal_buffer
  std::atomic<u64> gct_cursor = 0;  // the gct's cursor on the wal_buffer of this log worker
  logid_t w_lsn_counter       = 0;  // the LSN of this worker
  logid_t w_gsn_clock         = 0;  // local copy of gsn, i.e. w_gsn_clock <= gsn.load()
  logid_t rfa_gsn_flushed     = 0;  // local copy of LogManager.global_min_gsn_flushed
  // the wal buffer of this log worker
  alignas(BLK_BLOCK_SIZE) u8 wal_buffer[WAL_BUFFER_SIZE];

  LogWorker()  = default;
  ~LogWorker() = default;
  void Init(std::atomic<bool> &db_is_running);

  // LogSequenceNumber utilities
  auto GetCurrentGSN() -> logid_t;
  void SetCurrentGSN(logid_t gsn);

  // WorkerConsistentState utilities - public local state info to the group committer
  void PublicWCursor();
  void PublicLocalGSN();
  void PublicCommitTS();

  // Space utilities
  auto TotalFreeSpace() -> u64;
  auto ContiguousFreeSpaceForNewEntry() -> u64;
  void EnsureEnoughSpace(u64 requested_size);

  // Log utilities
  auto ReserveLogMetaEntry() -> LogMetaEntry &;
  auto ReserveDataLog(u64 payload_size, pageid_t pid, indexid_t idx_id) -> DataEntry &;
  auto ReserveBlobLogEntry(u64 payload_size, u16 part_id) -> BlobEntry &;
  auto ReserveFreePageLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreePageEntry &;
  auto ReservePageImageLog(u64 payload_size, pageid_t pid) -> PageImgEntry &;
  void SubmitActiveLogEntry();
  void IterateActiveTxnEntries(const std::function<void(const LogEntry &entry)> &log_cb);
};

static_assert(sizeof(LogWorker) % BLK_BLOCK_SIZE == 0);
static_assert(offsetof(LogWorker, wal_buffer) % BLK_BLOCK_SIZE == 0);

}  // namespace leanstore::recovery
