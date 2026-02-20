#include "recovery/log_worker.h"
#include "common/exceptions.h"
#include "common/queue.h"
#include "common/rand.h"
#include "common/utils.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"
#include "transaction/transaction_manager.h"

#include "share_headers/time.h"

#include <algorithm>
#include <cstring>
#include <thread>

using leanstore::transaction::TransactionManager;

namespace leanstore::recovery {

void WorkerConsistentState::Clone(WorkerConsistentState &other) {
  while (true) {
    try {
      sync::HybridGuard guard(&other.latch, sync::GuardMode::OPTIMISTIC);
      Ensure(precommitted_tx_commit_ts <= other.precommitted_tx_commit_ts);
      last_gsn                  = other.last_gsn;
      last_wal_cursor           = other.last_wal_cursor;
      precommitted_tx_commit_ts = other.precommitted_tx_commit_ts;
      break;
    } catch (const sync::RestartException &) {}
  }
}

void WorkerConsistentState::Clone(const ToCommitState &other) {
  last_gsn                  = other.last_gsn;
  last_wal_cursor           = other.last_wal_cursor;
  precommitted_tx_commit_ts = other.precommitted_tx_commit_ts;
}

void WorkerConsistentState::Clone(const AsyncCommitState &other) {
  last_gsn                  = other.last_gsn;
  last_wal_cursor           = other.last_wal_cursor;
  precommitted_tx_commit_ts = other.precommitted_tx_commit_ts;
}

ToCommitState::ToCommitState(const WorkerConsistentState &base)
    : last_wal_cursor(base.last_wal_cursor),
      last_gsn(base.last_gsn),
      precommitted_tx_commit_ts(base.precommitted_tx_commit_ts) {}

auto ToCommitState::operator<(const ToCommitState &r) -> bool {
  return precommitted_tx_commit_ts < r.precommitted_tx_commit_ts;
}

AsyncCommitState::AsyncCommitState(const WorkerConsistentState &base, u64 lsn)
    : req_lsn(lsn),
      last_wal_cursor(base.last_wal_cursor),
      last_gsn(base.last_gsn),
      precommitted_tx_commit_ts(base.precommitted_tx_commit_ts) {}

auto AsyncCommitState::operator<(const AsyncCommitState &r) const -> bool {
  return precommitted_tx_commit_ts < r.precommitted_tx_commit_ts;
}

/**
 * @brief Reset LogWorker's internal state except the WAL Buffer
 */
LogWorker::LogWorker(std::atomic<bool> &db_is_running, LogManager *log_manager)
    : log_manager(log_manager),
      is_running(&db_is_running),
      log_buffer(FLAGS_wal_buffer_size_mb * MB, is_running),
      backend(log_manager) {}

LogWorker::~LogWorker() = default;

void LogWorker::PublicWCursor() {
  sync::HybridGuard guard(&w_state->latch, sync::GuardMode::EXCLUSIVE);
  w_state->last_wal_cursor = log_buffer.wal_cursor;
}

void LogWorker::PublicLocalGSN() {
  sync::HybridGuard guard(&w_state->latch, sync::GuardMode::EXCLUSIVE);
  w_state->last_wal_cursor = log_buffer.wal_cursor;
  w_state->last_gsn        = w_gsn_clock;
}

void LogWorker::PublicCommitTS() {
  sync::HybridGuard guard(&w_state->latch, sync::GuardMode::EXCLUSIVE);
  w_state->last_wal_cursor           = log_buffer.wal_cursor;
  w_state->last_gsn                  = w_gsn_clock;
  w_state->precommitted_tx_commit_ts = last_unharden_commit_ts;
}

auto LogWorker::GetCurrentGSN() -> timestamp_t { return w_gsn_clock; }

void LogWorker::SetCurrentGSN(timestamp_t gsn) { w_gsn_clock = gsn; }

// --------------------------------------------------------------------------------------------

/**
 * @brief This is similar to ReserveDataLog, except that it operates with a prepared buffer.
 *  Therefore, it does not waits fot free space in the local log buffer
 */
auto LogWorker::PrepareDataLogEntry(u8 *buffer, u64 payload_size, pageid_t pid) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  active_log = reinterpret_cast<LogEntry *>(buffer);
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<DataEntry *>(active_log);
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::DATA_ENTRY;
  log_entry->pid     = pid;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return *log_entry;
}

/**
 * @brief Submit the active log entry, which was a prepared log buffer somewhere in LeanStore
 */
void LogWorker::SubmitPreparedLogEntry() {
  auto log_size = active_log->size;
  assert(log_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, log_size);
  std::memcpy(log_buffer.Current(), reinterpret_cast<u8 *>(active_log), log_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  SubmitActiveLogEntry();
}

// --------------------------------------------------------------------------------------------

auto LogWorker::ReserveLogMetaEntry() -> LogMetaEntry & {
  log_buffer.EnsureEnoughSpace(this, sizeof(LogMetaEntry));
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<LogMetaEntry *>(active_log);
  log_entry->size    = sizeof(LogMetaEntry);
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return static_cast<LogMetaEntry &>(*active_log);
}

auto LogWorker::ReserveLogCommitEntry(u64 payload_size) -> TxnCommitEntry & {
  const u64 total_size = sizeof(TxnCommitEntry) + payload_size;
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<TxnCommitEntry *>(active_log);
  log_entry->type    = LogEntry::Type::TX_COMMIT;
  log_entry->size    = total_size;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  auto vector_sz     = transaction::TransactionManager::active_txn.SerializeGSNVector(log_entry->payload);
  Ensure(vector_sz == payload_size);
  return static_cast<TxnCommitEntry &>(*active_log);
}

auto LogWorker::ReserveDataLog(u64 payload_size, pageid_t pid) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<DataEntry *>(active_log);
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::DATA_ENTRY;
  log_entry->pid     = pid;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReservePageImageLog(u64 payload_size, pageid_t pid) -> PageImgEntry & {
  Ensure(payload_size <= PAGE_SIZE);
  const u64 total_size = sizeof(PageImgEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry     = reinterpret_cast<PageImgEntry *>(active_log);
  log_entry->size    = total_size;
  log_entry->type    = LogEntry::Type::PAGE_IMG;
  log_entry->pid     = pid;
  log_entry->rc.w_id = LeanStore::worker_thread_id;
  log_entry->rc.txn  = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReserveFreeExtentLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreeExtentEntry & {
  const u64 total_size = sizeof(FreeExtentEntry);
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry       = reinterpret_cast<FreeExtentEntry *>(active_log);
  log_entry->size      = total_size;
  log_entry->type      = (to_free) ? LogEntry::Type::FREE_EXTENT : LogEntry::Type::REUSE_EXTENT;
  log_entry->start_pid = start_pid;
  log_entry->lp_size   = size;
  log_entry->rc.w_id   = LeanStore::worker_thread_id;
  log_entry->rc.txn    = transaction::TransactionManager::active_txn.TxnID();
  log_entry->rc.gsn    = GetCurrentGSN();
  return *log_entry;
}

/**
 * @brief Finish the current active log entry and submit it to the log buffer
 *
 * @return true   Only for WILO variants -- if the system has just written the log buffer to the storage
 * @return false  Otherwise
 */
auto LogWorker::SubmitActiveLogEntry() -> bool {
  if (FLAGS_wal_debug) { active_log->ComputeChksum(); }

  bool just_write_log = false;
  log_buffer.wal_cursor += active_log->size;

  if (active_log->type == LogEntry::Type::TX_COMMIT) {
    last_unharden_commit_ts = TransactionManager::active_txn.commit_ts;

    /* Normal variants only published commit timestamp -> group commit execute the ack */
    /* If we can steal log, then do it. Otherwise, follow traditional WILO */
    PublicCommitTS();
    auto to_steal = ShouldStealLog();
    if (to_steal != StealDecision::NOTHING) {
      just_write_log = true;
      WorkerStealsLog(to_steal == StealDecision::TO_WRITE_LOCALLY);
    }
  } else if (active_log->type == LogEntry::Type::DATA_ENTRY) {
    // Public local GSN to gct for RFA operation
    PublicLocalGSN();
  }

  return just_write_log;
}

// --------------------------------------------------------------------------------------------
/**
 * Whether the current worker should
 * - Only write logs
 * - Try to steal logs
 * - Do not attempt to write or steal
 */
auto LogWorker::ShouldStealLog() -> StealDecision {
  auto written_cursor = log_buffer.write_cursor.load();
  auto difference     = (log_buffer.wal_cursor > written_cursor)
                          ? log_buffer.wal_cursor - written_cursor
                          : log_buffer.wal_cursor + FLAGS_wal_buffer_size_mb * MB - written_cursor;
  /* Only SSD log backend support log stealing */
  if (difference >= log_manager->worker_write_batch_size_) { return StealDecision::TO_WRITE_LOCALLY; }
  return (difference < log_manager->worker_write_batch_size_ / LogManager::stealing_border) ? StealDecision::NOTHING
                                                                                            : StealDecision::TO_STEAL;
}

void LogWorker::WorkerStealsLog(bool write_only) {
  if (write_only) {
    /* Only flush dirty logs */
    if (log_buffer.wal_cursor != log_buffer.write_cursor.load()) {
      /* Sync lifecycle - proceed as normal */
      {
        sync::HybridGuard guard(&parking_latch, sync::GuardMode::EXCLUSIVE);
        parking_lot_log_flush.emplace_back(*w_state);
      }
      log_buffer.LogFlush(this, false);
      TryPublishCommitState(LeanStore::worker_thread_id, *w_state);
    }
    return;
  }

  /* Under normal execution - try to steal from all peers within the group */
  auto start_wid  = (LeanStore::worker_thread_id / FLAGS_wal_stealing_group_size) * FLAGS_wal_stealing_group_size;
  auto group_size = std::min(FLAGS_wal_stealing_group_size, FLAGS_worker_count - start_wid);
  auto success    = std::vector<bool>(group_size, false);
  auto peer       = std::vector<WorkerConsistentState>(group_size);
  for (auto idx = 0U; idx < group_size; idx++) {
    if (start_wid + idx != LeanStore::worker_thread_id) {
      success[idx] = TryStealLogs(start_wid + idx, peer[idx]);
    } else {
      sync::HybridGuard guard(&parking_latch, sync::GuardMode::EXCLUSIVE);
      parking_lot_log_flush.emplace_back(*w_state);
    }
  }

  /* Write the whole log buffer & Public log durability state of all peers */
  log_buffer.LogFlush(this, false);
  TryPublishCommitState(LeanStore::worker_thread_id, *w_state);
  for (auto idx = 0U; idx < group_size; idx++) {
    if (success[idx]) { TryPublishCommitState(start_wid + idx, peer[idx]); }
  }
}

auto LogWorker::TryStealLogs(wid_t peer_id, WorkerConsistentState &out_state) -> bool {
  /* Retrieve peer's state */
  auto &peer_logger = log_manager->logger_[peer_id];
  auto &peer        = peer_logger.log_buffer;
  out_state.Clone(*peer_logger.w_state);
  auto last_written_cursor = peer.write_cursor.load();

  /**
   * Check if the current log buffer contains enough free space to steal logs from its peer
   *
   * Two conditions:
   * - Peer's log buffer is not empty
   * - Current log buffer contains enough space to copy all peer's buffer
   */
  bool success = false;
  if (out_state.last_wal_cursor != last_written_cursor &&
      (log_buffer.ContiguousFreeSpaceForNewEntry() >=
       LogBuffer::DirtyLogSize(out_state.last_wal_cursor, last_written_cursor) + LogBuffer::CR_ENTRY_SIZE)) {
    // state.Clone() above should already mfence() the local log buffer of the peer worker
    auto total_copy_size = 0UL;
    if (out_state.last_wal_cursor > last_written_cursor) {
      std::memcpy(log_buffer.Current(), &(peer.wal_buffer)[last_written_cursor],
                  out_state.last_wal_cursor - last_written_cursor);
      total_copy_size = out_state.last_wal_cursor - last_written_cursor;
    } else {
      auto first_part_size = FLAGS_wal_buffer_size_mb * MB - last_written_cursor;
      std::memcpy(log_buffer.Current(), &(peer.wal_buffer)[last_written_cursor], first_part_size);
      std::memcpy(log_buffer.Current() + first_part_size, peer.wal_buffer, out_state.last_wal_cursor);
      total_copy_size = first_part_size + out_state.last_wal_cursor;
    }

    // Now check if the peer worker already writes its log buffer to the storage
    {
      sync::HybridGuard guard(&(peer_logger.parking_latch), sync::GuardMode::EXCLUSIVE);
      if (peer.write_cursor.compare_exchange_strong(last_written_cursor, out_state.last_wal_cursor)) {
        peer_logger.parking_lot_log_flush.emplace_back(out_state);
        success = true;
        log_buffer.wal_cursor += total_copy_size;
        assert(log_buffer.wal_cursor < FLAGS_wal_buffer_size_mb * MB);
      }
    }
  }

  return success;
}

// --------------------------------------------------------------------------------------------

/**
 * @brief Publish the durable state of this worker to other threads
 */
void LogWorker::TryPublishCommitState(wid_t w_id, const WorkerConsistentState &w_state) {
  auto to_find = ToCommitState(w_state);
  auto &logger = log_manager->logger_[w_id];
  {
    sync::HybridGuard guard(&logger.parking_latch, sync::GuardMode::EXCLUSIVE);
    std::sort(logger.parking_lot_log_flush.begin(), logger.parking_lot_log_flush.end());
    auto ite = std::lower_bound(logger.parking_lot_log_flush.begin(), logger.parking_lot_log_flush.end(), to_find);
    Ensure(ite != logger.parking_lot_log_flush.end());
    if (ite == logger.parking_lot_log_flush.begin()) {
      log_manager->commit_state_[w_id].SyncClone(*ite);
    } else {
      *(ite - 1) = *ite;
    }
    logger.parking_lot_log_flush.erase(ite);
  }
}

// --------------------------------------------------------------------------------------------

void LogWorker::IterateActiveTxnEntries([[maybe_unused]] const std::function<void(const LogEntry &entry)> &log_cb) {
  throw leanstore::ex::TODO("IterateActiveTxnEntries is not yet implemented");
}

}  // namespace leanstore::recovery