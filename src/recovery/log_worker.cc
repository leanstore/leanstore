#include "recovery/log_worker.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "recovery/log_entry.h"
#include "sync/hybrid_guard.h"
#include "transaction/transaction_manager.h"

#include <algorithm>
#include <cstring>

using TM = leanstore::transaction::TransactionManager;

namespace leanstore::recovery {

auto LogWorker::WorkerConsistentState::Clone(LogWorker::WorkerConsistentState &other)
  -> LogWorker::WorkerConsistentState & {
  while (true) {
    try {
      sync::HybridGuard guard(&other.latch, sync::GuardMode::OPTIMISTIC);
      last_gsn                  = other.last_gsn;
      w_written_offset          = other.w_written_offset;
      precommitted_tx_commit_ts = other.precommitted_tx_commit_ts;
      break;
    } catch (const sync::RestartException &) {}
  }
  return *this;
}

/**
 * @brief Reset LogWorker's internal state except the WAL Buffer
 */
void LogWorker::Init(std::atomic<bool> &db_is_running) {
  std::memset(reinterpret_cast<u8 *>(this), 0, offsetof(LogWorker, rfa_gsn_flushed));
  is_running             = &db_is_running;
  precommitted_queue     = std::deque<transaction::Transaction>();
  precommitted_queue_rfa = std::deque<transaction::Transaction>();
}

void LogWorker::PublicWCursor() {
  sync::HybridGuard guard(&w_state.latch, sync::GuardMode::EXCLUSIVE);
  w_state.w_written_offset = w_cursor;
}

void LogWorker::PublicLocalGSN() {
  sync::HybridGuard guard(&w_state.latch, sync::GuardMode::EXCLUSIVE);
  w_state.w_written_offset = w_cursor;
  w_state.last_gsn         = w_gsn_clock;
}

void LogWorker::PublicCommitTS() {
  sync::HybridGuard guard(&w_state.latch, sync::GuardMode::EXCLUSIVE);
  w_state.w_written_offset          = w_cursor;
  w_state.precommitted_tx_commit_ts = TM::active_txn.CommitTS();
}

auto LogWorker::GetCurrentGSN() -> logid_t { return w_gsn_clock; }

void LogWorker::SetCurrentGSN(logid_t gsn) { w_gsn_clock = gsn; }

// For desc, check log_worker.h
auto LogWorker::TotalFreeSpace() -> u64 {
  auto gct_cursor_w = gct_cursor.load();
  if (gct_cursor_w == w_cursor) { return WAL_BUFFER_SIZE; }
  if (w_cursor > gct_cursor_w) { return gct_cursor_w + (WAL_BUFFER_SIZE - w_cursor); }
  // w_cursor < gct_cursor_w
  return gct_cursor_w - w_cursor;
}

/**
 * Current available (and should be contiguous because we're appending new log entry)
 *  space in the wal buffer
 */
auto LogWorker::ContiguousFreeSpaceForNewEntry() -> u64 {
  const auto gct_cursor_w = gct_cursor.load();
  // note that it is possible to split the log entry into two parts,
  //  one to append at the end + the other to the head of wal buffer
  // however, doing so is very complicated, so let's avoid that
  // instead, we can circulate the w_cursor to the beginning and insert the whole entry
  return (w_cursor < gct_cursor_w) ? gct_cursor_w - w_cursor : WAL_BUFFER_SIZE - w_cursor;
}

void LogWorker::EnsureEnoughSpace(u64 requested_size) {
  if (FLAGS_wal_enable) {
    auto prepared_space_for_new_entry = requested_size + CR_ENTRY_SIZE;
    if ((WAL_BUFFER_SIZE - w_cursor) < prepared_space_for_new_entry) {
      // the wal buffer doesn't have enough free space for the log
      //  hence we add padding to the end, then insert the entry to the beginning
      prepared_space_for_new_entry += WAL_BUFFER_SIZE - w_cursor;
    }
    while ((TotalFreeSpace() < prepared_space_for_new_entry) && (is_running->load())) { AsmYield(); }
    // Always ensure that we can put one CR entry at the end of the wal_buffer
    if (ContiguousFreeSpaceForNewEntry() < requested_size + CR_ENTRY_SIZE) {
      Ensure(CR_ENTRY_SIZE <= WAL_BUFFER_SIZE - w_cursor);
      // the remaining space on wal_buffer is not enough, then we insert padding
      //  in order to insert the new log entry at the beginning of the wal buffer
      auto entry  = new (&wal_buffer[w_cursor]) LogMetaEntry();
      entry->type = LogEntry::Type::CARRIAGE_RETURN;
      entry->size = WAL_BUFFER_SIZE - w_cursor;
      if (FLAGS_wal_debug) { entry->ComputeChksum(); }
      // reset cursor back to the beginning & public that to group committer
      w_cursor = 0;
      PublicWCursor();
    }
    Ensure(ContiguousFreeSpaceForNewEntry() >= requested_size);
    Ensure(w_cursor + requested_size + CR_ENTRY_SIZE <= WAL_BUFFER_SIZE);
  }
}

auto LogWorker::ReserveLogMetaEntry() -> LogMetaEntry & {
  EnsureEnoughSpace(sizeof(LogMetaEntry));
  active_log = reinterpret_cast<LogEntry *>(&wal_buffer[w_cursor]);
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<LogMetaEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = sizeof(LogMetaEntry);
  return static_cast<LogMetaEntry &>(*active_log);
}

auto LogWorker::ReserveDataLog(u64 payload_size, pageid_t pid, indexid_t idx_id) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  Ensure(total_size <= WAL_BUFFER_SIZE);
  EnsureEnoughSpace(total_size);
  active_log = reinterpret_cast<LogEntry *>(&wal_buffer[w_cursor]);
  // Cast active_log into proper type to update the log entry
  auto log_entry    = reinterpret_cast<DataEntry *>(active_log);
  log_entry->lsn    = w_lsn_counter++;
  log_entry->size   = total_size;
  log_entry->type   = LogEntry::Type::DATA_ENTRY;
  log_entry->pid    = pid;
  log_entry->gsn    = GetCurrentGSN();
  log_entry->idx_id = idx_id;
  return *log_entry;
}

auto LogWorker::ReservePageImageLog(u64 payload_size, pageid_t pid) -> PageImgEntry & {
  Ensure(payload_size < PAGE_SIZE);
  const u64 total_size = sizeof(PageImgEntry) + payload_size;
  Ensure(total_size <= WAL_BUFFER_SIZE);
  EnsureEnoughSpace(total_size);
  active_log = reinterpret_cast<LogEntry *>(&wal_buffer[w_cursor]);
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<PageImgEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = total_size;
  log_entry->type = LogEntry::Type::PAGE_IMG;
  log_entry->pid  = pid;
  log_entry->gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReserveBlobLogEntry(u64 payload_size, u16 part_id) -> BlobEntry & {
  const u64 total_size = sizeof(BlobEntry) + payload_size;
  Ensure(total_size <= WAL_BUFFER_SIZE);
  EnsureEnoughSpace(total_size);
  active_log = reinterpret_cast<LogEntry *>(&wal_buffer[w_cursor]);
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<BlobEntry *>(active_log);
  log_entry->lsn     = w_lsn_counter++;
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::BLOB_ENTRY;
  log_entry->part_id = part_id;
  return *log_entry;
}

auto LogWorker::ReserveFreePageLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreePageEntry & {
  const u64 total_size = sizeof(FreePageEntry);
  Ensure(total_size <= WAL_BUFFER_SIZE);
  EnsureEnoughSpace(total_size);
  active_log = reinterpret_cast<LogEntry *>(&wal_buffer[w_cursor]);
  // Cast active_log into proper type to update the log entry
  auto log_entry       = reinterpret_cast<FreePageEntry *>(active_log);
  log_entry->lsn       = w_lsn_counter++;
  log_entry->size      = total_size;
  log_entry->type      = (to_free) ? LogEntry::Type::FREE_PAGE : LogEntry::Type::REUSE_PAGE;
  log_entry->start_pid = start_pid;
  log_entry->lp_size   = size;
  log_entry->gsn       = GetCurrentGSN();
  return *log_entry;
}

void LogWorker::SubmitActiveLogEntry() {
  if (FLAGS_wal_debug) { active_log->ComputeChksum(); }
  w_cursor += active_log->size;
  if (active_log->type == LogEntry::Type::TX_COMMIT) {
    // Notify gct that this worker just completes a txn
    PublicCommitTS();
  } else if (active_log->type == LogEntry::Type::DATA_ENTRY || active_log->type == LogEntry::Type::BLOB_ENTRY) {
    // Public local GSN to gct for RFA operation
    PublicLocalGSN();
  }
}

void LogWorker::IterateActiveTxnEntries(const std::function<void(const LogEntry &entry)> &log_cb) {
  for (u64 cursor = TM::active_txn.StartWalCursor(); cursor != w_cursor;) {
    const auto &entry = *reinterpret_cast<LogEntry *>(&wal_buffer[cursor]);
    Ensure(entry.size > 0);
    if (FLAGS_wal_debug) { active_log->ValidateChksum(); }
    if (entry.type == LogEntry::Type::CARRIAGE_RETURN) {
      cursor = 0;
    } else {
      log_cb(entry);
      cursor += entry.size;
    }
  }
}

}  // namespace leanstore::recovery