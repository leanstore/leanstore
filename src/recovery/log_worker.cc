#include "recovery/log_worker.h"
#include "common/exceptions.h"
#include "common/queue.h"
#include "common/rand.h"
#include "common/utils.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"
#include "transaction/transaction_manager.h"

#include "share_headers/time.h"

#include <algorithm>
#include <cstring>
#include <thread>

using leanstore::transaction::CommitProtocol;
using leanstore::transaction::TransactionManager;

#define DIRTY_LOG_SIZE(wal_cursor, written_cursor)                       \
  (((wal_cursor) > (written_cursor)) ? ((wal_cursor) - (written_cursor)) \
                                     : ((wal_cursor) + (FLAGS_wal_buffer_size_mb * MB) - (written_cursor)))

namespace leanstore::recovery {

void WorkerConsistentState::Clone(const ToCommitState &other) {
  last_gsn                  = other.last_gsn;
  last_wal_cursor           = other.last_wal_cursor;
  precommitted_tx_commit_ts = other.precommitted_tx_commit_ts;
}

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

ToCommitState::ToCommitState(const WorkerConsistentState &base)
    : last_wal_cursor(base.last_wal_cursor),
      last_gsn(base.last_gsn),
      precommitted_tx_commit_ts(base.precommitted_tx_commit_ts) {}

auto ToCommitState::operator<(const ToCommitState &r) -> bool {
  return precommitted_tx_commit_ts < r.precommitted_tx_commit_ts;
}

/**
 * @brief Reset LogWorker's internal state except the WAL Buffer
 */
LogWorker::LogWorker(std::atomic<bool> &db_is_running, LogManager *log_manager)
    : log_manager(log_manager), is_running(&db_is_running), log_buffer(FLAGS_wal_buffer_size_mb * MB, is_running) {
  int ret = io_uring_queue_init(WILO_AIO_QD, &local_ring, 0);
  if (ret != 0) { throw ex::EnsureFailed("LogWorker: io_uring_queue_init error " + std::to_string(ret)); }
}

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
  auto log_entry  = reinterpret_cast<DataEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = total_size;
  log_entry->type = LogEntry::Type::DATA_ENTRY;
  log_entry->pid  = pid;
  log_entry->gsn  = GetCurrentGSN();
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
  SubmitActiveLogEntry();
}

// --------------------------------------------------------------------------------------------

auto LogWorker::ReserveLogMetaEntry() -> LogMetaEntry & {
  log_buffer.EnsureEnoughSpace(this, sizeof(LogMetaEntry));
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<LogMetaEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = sizeof(LogMetaEntry);
  return static_cast<LogMetaEntry &>(*active_log);
}

auto LogWorker::ReserveDataLog(u64 payload_size, pageid_t pid) -> DataEntry & {
  const u64 total_size = sizeof(DataEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<DataEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = total_size;
  log_entry->type = LogEntry::Type::DATA_ENTRY;
  log_entry->pid  = pid;
  log_entry->gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReservePageImageLog(u64 payload_size, pageid_t pid) -> PageImgEntry & {
  Ensure(payload_size <= PAGE_SIZE);
  const u64 total_size = sizeof(PageImgEntry) + payload_size;
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<PageImgEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = total_size;
  log_entry->type = LogEntry::Type::PAGE_IMG;
  log_entry->pid  = pid;
  log_entry->gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReserveFreePageLogEntry(bool to_free, pageid_t pid) -> FreePageEntry & {
  const u64 total_size = sizeof(FreePageEntry);
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry  = reinterpret_cast<FreePageEntry *>(active_log);
  log_entry->lsn  = w_lsn_counter++;
  log_entry->size = total_size;
  log_entry->type = (to_free) ? LogEntry::Type::FREE_PAGE : LogEntry::Type::REUSE_PAGE;
  log_entry->pid  = pid;
  log_entry->gsn  = GetCurrentGSN();
  return *log_entry;
}

auto LogWorker::ReserveFreeExtentLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreeExtentEntry & {
  const u64 total_size = sizeof(FreeExtentEntry);
  assert(total_size <= FLAGS_wal_buffer_size_mb * MB);
  log_buffer.EnsureEnoughSpace(this, total_size);
  active_log = reinterpret_cast<LogEntry *>(log_buffer.Current());
  // Cast active_log into proper type to update the log entry
  auto log_entry       = reinterpret_cast<FreeExtentEntry *>(active_log);
  log_entry->lsn       = w_lsn_counter++;
  log_entry->size      = total_size;
  log_entry->type      = (to_free) ? LogEntry::Type::FREE_EXTENT : LogEntry::Type::REUSE_EXTENT;
  log_entry->start_pid = start_pid;
  log_entry->lp_size   = size;
  log_entry->gsn       = GetCurrentGSN();
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

  /**
   * If FLAGS_wal_centralized_buffer == true, copy log records to the centralized log buffer instead
   */
  if (FLAGS_wal_centralized_buffer) {
    /* Copy log records to the centralized log buffer */
    {
      sync::HybridGuard guard(&(log_manager->buffer_latch_), sync::GuardMode::EXCLUSIVE);
      log_manager->centralized_buf_->EnsureEnoughSpace(this, active_log->size);
      std::memcpy(log_manager->centralized_buf_->Current(), log_buffer.Current(), active_log->size);
      log_manager->centralized_buf_->wal_cursor += active_log->size;
      assert(log_buffer.wal_cursor == 0);
    }

    /* Public local state */
    if (active_log->type == LogEntry::Type::TX_COMMIT) {
      last_unharden_commit_ts = TransactionManager::active_txn.commit_ts;
      PublicCommitTS();
    } else if (active_log->type == LogEntry::Type::DATA_ENTRY) {
      PublicLocalGSN();
    }
    return false;
  }

  assert(!FLAGS_wal_centralized_buffer);
  bool just_write_log = false;
  log_buffer.wal_cursor += active_log->size;

  if (active_log->type == LogEntry::Type::TX_COMMIT) {
    last_unharden_commit_ts = TransactionManager::active_txn.commit_ts;

    /* Normal variants only published commit timestamp -> group commit execute the ack */
    if ((FLAGS_txn_commit_variant != CommitProtocol::WORKERS_WRITE_LOG) &&
        (FLAGS_txn_commit_variant != CommitProtocol::WILO_STEAL)) {
      PublicCommitTS();
    } else if (FLAGS_txn_commit_variant == CommitProtocol::WORKERS_WRITE_LOG) {
      /* WILO writes log immediately */
      if (ShouldStealLog() == StealDecision::TO_WRITE_LOCALLY) {
        WorkerWritesLog(false);
        just_write_log = true;
      } else {
        PublicLocalGSN();
      }
    } else {
      /* WILO steal tries to steal logs before writing */
      assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);

      /* If we can steal log, then do it. Otherwise, follow traditional WILO */
      PublicCommitTS();
      auto to_steal = ShouldStealLog();
      if (to_steal != StealDecision::NOTHING) {
        just_write_log = true;
        WorkerStealsLog(to_steal == StealDecision::TO_STEAL, false);
      }
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
  if (FLAGS_txn_commit_variant == CommitProtocol::WORKERS_WRITE_LOG) {
    return (difference >= log_manager->worker_write_batch_size_) ? StealDecision::TO_WRITE_LOCALLY
                                                                 : StealDecision::NOTHING;
  }
  assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);
  if (difference >= log_manager->worker_write_batch_size_) { return StealDecision::TO_WRITE_LOCALLY; }
  return (difference < log_manager->worker_write_batch_size_ / (BitLength(FLAGS_wal_stealing_group_size)))
           ? StealDecision::NOTHING
           : StealDecision::TO_STEAL;
}

void LogWorker::WorkerWritesLog(bool force_commit) {
  assert(FLAGS_txn_commit_variant == CommitProtocol::WORKERS_WRITE_LOG);
  if (log_buffer.wal_cursor != log_buffer.write_cursor.load()) {
    log_buffer.PersistLog(this, (force_commit) ? FLAGS_wal_force_commit_alignment : BLK_BLOCK_SIZE);
  };
}

void LogWorker::WorkerStealsLog(bool write_only, bool force_commit) {
  assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);
  if (write_only) {
    {
      sync::HybridGuard guard(&parking_latch, sync::GuardMode::EXCLUSIVE);
      parking_lot_log_flush.emplace_back(*w_state);
    }
    if (log_buffer.wal_cursor != log_buffer.write_cursor.load()) {
      log_buffer.PersistLog(this, (force_commit) ? FLAGS_wal_force_commit_alignment : BLK_BLOCK_SIZE);
    }
    TryPublishCommitState(worker_thread_id, *w_state);
    return;
  }

  /* Under normal execution - try to steal from all peers within the group */
  auto start_wid  = (worker_thread_id / FLAGS_wal_stealing_group_size) * FLAGS_wal_stealing_group_size;
  auto group_size = std::min(FLAGS_wal_stealing_group_size, FLAGS_worker_count - start_wid);
  auto success    = std::vector<bool>(group_size, false);
  WorkerConsistentState peer[group_size];
  for (auto idx = 0U; idx < group_size; idx++) {
    if (start_wid + idx != worker_thread_id) {
      success[idx] = TryStealLogs(start_wid + idx, peer[idx]);
    } else {
      sync::HybridGuard guard(&parking_latch, sync::GuardMode::EXCLUSIVE);
      parking_lot_log_flush.emplace_back(*w_state);
    }
  }

  /* Write the whole log buffer & Public log durability state of all peers */
  log_buffer.PersistLog(this, BLK_BLOCK_SIZE, [&]() {
    /**
     * @brief With big machine of ~ 100 CPU cores, the first phase actually contributes ~ 10us per round
     * One possible solution for that is to run the first phase during log flush.
     * Doing so means we collect the consistent state a few us earlier than its actual execution,
     *  leading to potential-outdated state.
     * Although doing so is still correct, we potentially lose some txns during commit ack stage.
     * In other words, there are COMMITTED transactions that we haven't ACKed them.
     */
    if (FLAGS_txn_collect_state_during_flush) {
      auto executor_idx = worker_thread_id / FLAGS_txn_commit_group_size;
      if (log_manager->commit_latches_[executor_idx].TryLockExclusive()) {
        sync::HybridGuard guard(&(log_manager->commit_latches_[executor_idx]), sync::GuardMode::ADOPT_EXCLUSIVE);
        log_manager->gc_[executor_idx].PhaseOneWithoutThisGroup(start_wid, start_wid + group_size);
      }
    }
  });
  TryPublishCommitState(worker_thread_id, *w_state);
  for (auto idx = 0U; idx < group_size; idx++) {
    if (success[idx]) { TryPublishCommitState(start_wid + idx, peer[idx]); }
  }
}

auto LogWorker::TryStealLogs(wid_t peer_id, WorkerConsistentState &out_state) -> bool {
  assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);

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
       DIRTY_LOG_SIZE(out_state.last_wal_cursor, last_written_cursor) + LogBuffer::CR_ENTRY_SIZE)) {
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
      peer_logger.parking_lot_log_flush.emplace_back(out_state);
      if (peer.write_cursor.compare_exchange_strong(last_written_cursor, out_state.last_wal_cursor)) {
        success = true;
        log_buffer.wal_cursor += total_copy_size;
        assert(log_buffer.wal_cursor < FLAGS_wal_buffer_size_mb * MB);
      } else {
        peer_logger.parking_lot_log_flush.pop_back();
      }
    }
  }

  return success;
}

/**
 * @brief Publish the durable state of this worker to other threads - should only be used with WILO_STEAL
 *
 * This is to fix a bug of CloneIfHigherTS() design:
 * Bug scenario:
 * - When worker 1 and worker 2 both steal from worker 3, and worker 1 steals older log records
 * - If worker 2 completes its log flush, it will publish that all log records are durable,
 *  which includes both the older log records stolen by worker 1 -> *THIS IS WRONG*
 *
 * We fix this by introducing a list of on-running write ops on a certain log buffer.
 * After stealing logs successfully and before log flush,
 *  we append the expected consistent state of a worker of that log flush
 * And then, when the log flush, we check the on-running write ops on that log buffer and determine the correct state.
 * Assuming we have the following list: [state(ts=9), state(ts=12), state(ts=15)].
 * Two possible scenarios:
 * - If the flush persists the state with smallest timestamp, i.e. state(ts=9) in the example,
 *    then we know there is no non-durable older log records, and it's safe to publish the commit state
 * - On the other hand, if the flush persists either state(ts=12) or state(ts=15), i.e. state(ts=9) is not durable.
 *    In this case, we merge the to-be-durable consistent state with that of previos timestamp:
 *    + If ts=15 is flushed, we merge state_y(ts=15) & state_x(ts=12) into state_y(ts=12)
 *    + If ts=12 is flushed, we merge state_x(ts=12) & state_z(ts=9) into state_x(ts=9)
 */
void LogWorker::TryPublishCommitState(wid_t w_id, const WorkerConsistentState &w_state) {
  assert(FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL);
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

/**
 * @brief Deallocate the transaction objects locally, only used for WORKERS_WRITE_LOG
 */
void LogWorker::TryCommitRFATxns() {
  if (FLAGS_txn_commit_variant != CommitProtocol::WORKERS_WRITE_LOG || ms_queue_rfa.empty()) { return; }

  // Complete rfa txns -- it's possible that other workers may already harden current log buffer
  auto commit_rfa_start = tsctime::ReadTSC();
  auto tx_i             = 0UL;
  for (; tx_i < ms_queue_rfa.size(); tx_i++) {
    auto &txn = ms_queue_rfa[tx_i];
    if (txn.commit_ts > last_unharden_commit_ts) { break; }
    transaction::TransactionManager::DurableCommit(txn, commit_rfa_start);
  }

  if (start_profiling) { statistics::txn_processed[worker_thread_id] += tx_i; }
  ms_queue_rfa.erase(ms_queue_rfa.begin(), ms_queue_rfa.begin() + tx_i);
}

}  // namespace leanstore::recovery