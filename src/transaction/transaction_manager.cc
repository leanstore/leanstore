#include "transaction/transaction_manager.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"

#include "share_headers/time.h"

using LogManager = leanstore::recovery::LogManager;
using LogEntry   = leanstore::recovery::LogEntry;
using DataEntry  = leanstore::recovery::DataEntry;

namespace leanstore::transaction {

thread_local Transaction TransactionManager::active_txn              = Transaction();
thread_local timestamp_t TransactionManager::previous_completed_time = 0;

TransactionManager::TransactionManager(buffer::BufferManager *buffer_manager, LogManager *log_manager)
    : buffer_(buffer_manager), log_manager_(log_manager){};

auto TransactionManager::ParseIsolationLevel(const std::string &str) -> IsolationLevel {
  if (str == "ser") { return IsolationLevel::SERIALIZABLE; }
  if (str == "si") { return IsolationLevel::SNAPSHOT_ISOLATION; }
  if (str == "rc") { return IsolationLevel::READ_COMMITTED; }
  Ensure(str == "ru");
  return IsolationLevel::READ_UNCOMMITTED;
}

void TransactionManager::StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time,
                                          IsolationLevel next_tx_isolation_level, Transaction::Mode next_tx_mode,
                                          bool read_only) {
  Ensure(!active_txn.IsRunning());
  active_txn.Initialize(this, global_clock++, next_tx_type, next_tx_isolation_level, next_tx_mode, read_only);
  if (FLAGS_txn_debug) {
    active_txn.stats.start        = tsctime::ReadTSC();
    active_txn.stats.arrival_time = (next_tx_arrival_time > 0) ? next_tx_arrival_time : active_txn.stats.start;
    assert(next_tx_arrival_time <= active_txn.stats.start);
    statistics::worker_idle_ns[worker_thread_id][Rand(SAMPLING_SIZE)] =
      active_txn.stats.arrival_time - previous_completed_time;
  }
  if (!FLAGS_wal_enable) { return; }
  // Propagate WAL-related run-time context for this active transaction
  auto &logger = log_manager_->LocalLogWorker();
  if (!read_only) {
    auto &entry = logger.ReserveLogMetaEntry();
    entry.type  = LogEntry::Type::TX_START;
    logger.SubmitActiveLogEntry();
    if (FLAGS_wal_debug) { entry.ComputeChksum(); }
  }
  const auto sync_point = LogManager::global_sync_to_this_gsn.load();
  if (sync_point > logger.GetCurrentGSN()) {
    logger.SetCurrentGSN(sync_point);
    logger.PublicLocalGSN();
  }
  /**
   * If RFA is not enabled, default to serialize all transactions using GSN
   * i.e. projects all transactions onto a single time axis - the GSN,
   *  which means `needs_remote_flush = true`
   */
  if (FLAGS_wal_enable_rfa) {
    logger.rfa_gsn_flushed        = LogManager::global_min_gsn_flushed.load();
    active_txn.needs_remote_flush = false;
  } else {
    active_txn.needs_remote_flush = true;
  }
  if (next_tx_isolation_level >= IsolationLevel::READ_COMMITTED) { throw leanstore::ex::TODO("Not implemented yet"); }
}

void TransactionManager::CommitTransaction() {
  // If WAL is disabled or current transaction is read-only, commit transaction'll be a no-op
  Ensure(active_txn.state == Transaction::State::STARTED);
  // Update transactional context of current txn
  active_txn.commit_ts = global_clock++;
  if (FLAGS_txn_debug) {
    active_txn.stats.precommit = tsctime::ReadTSC();
    previous_completed_time    = active_txn.stats.precommit;
  }

  auto &logger = log_manager_->LocalLogWorker();

  // Append txn object to the pre-commit queue
  if (FLAGS_wal_enable && !active_txn.ReadOnly()) {
    // Update state of active txn
    active_txn.max_observed_gsn = logger.w_gsn_clock;
    active_txn.state            = Transaction::State::READY_TO_COMMIT;

    // Push the txn to the pre-commit queue
    QueueTransaction(active_txn);

    // Insert commit log entry to WAL
    auto &entry        = logger.ReserveLogMetaEntry();
    entry.type         = LogEntry::Type::TX_COMMIT;
    auto should_commit = logger.SubmitActiveLogEntry();
    if (FLAGS_wal_debug) { entry.ComputeChksum(); }

    // Try to trigger group commit directly within the worker
    if (FLAGS_txn_commit_variant == CommitProtocol::BASELINE_COMMIT) { log_manager_->TriggerGroupCommit(0); }
    if (FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL &&
        (should_commit || (Rand(BitLength(FLAGS_worker_count)) == 0))) {
      log_manager_->TriggerGroupCommit(worker_thread_id / FLAGS_txn_commit_group_size);
    }
  }

  // If log is disabled, update the statistics manually
  if (!FLAGS_wal_enable) {
    DurableCommit(active_txn, active_txn.stats.precommit);
    if (start_profiling) {
      if (active_txn.needs_remote_flush) {
        statistics::precommited_txn_processed[worker_thread_id] += 1;
      } else {
        statistics::precommited_rfa_txn_processed[worker_thread_id] += 1;
      }
    }
  }
}

/**
 * @brief TODO(XXX): Implement AbortTransaction -- rollback changes
 * Should also handle cases when logs are already flushed to the storage, and being overwritten in memory
 */
void TransactionManager::AbortTransaction() {
  throw leanstore::ex::TODO("Undo is not yet implemented");

  // Only support abort txn if WAL is enabled
  Ensure(FLAGS_wal_enable);
  // A transaction was initialized, and it should be running
  Ensure(active_txn.IsRunning());
  active_txn.state = Transaction::State::ABORTED;
  // If current transaction is read-only, abort transaction'll be a no-op
  if (active_txn.ReadOnly()) { return; }

  // Run-time context
  auto &logger = log_manager_->LocalLogWorker();

  // Revert back all modifications using WAL
  std::vector<const LogEntry *> entries;
  logger.IterateActiveTxnEntries([&](const LogEntry &entry) {
    if (entry.type == LogEntry::Type::DATA_ENTRY) { entries.push_back(&entry); }
  });
  std::for_each(entries.rbegin(), entries.rend(), [&](const LogEntry *entry) {
    [[maybe_unused]] const auto &data_entry = *reinterpret_cast<const DataEntry *>(entry);  // NOLINT
    throw leanstore::ex::TODO("Undo is not yet implemented");
  });
  // Insert abort log entry to WAL
  auto &entry = logger.ReserveLogMetaEntry();
  entry.type  = LogEntry::Type::TX_ABORT;
  logger.SubmitActiveLogEntry();
}

template <class T>
void TransactionManager::DurableCommit(T &txn, timestamp_t queue_phase_start) {
  txn.state = transaction::Transaction::State::COMMITTED;
  if (FLAGS_txn_debug) {
    auto commit_stats = tsctime::ReadTSC();
    if (start_profiling_latency) {
      statistics::txn_queue[worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.precommit, queue_phase_start));
      if (txn.needs_remote_flush) {
        statistics::txn_latency[worker_thread_id].emplace_back(tsctime::TscDifferenceNs(txn.stats.start, commit_stats));
      } else {
        statistics::rfa_txn_latency[worker_thread_id].emplace_back(
          tsctime::TscDifferenceNs(txn.stats.start, commit_stats));
      }
      statistics::txn_exec[worker_thread_id].push_back(tsctime::TscDifferenceNs(txn.stats.start, txn.stats.precommit));
      statistics::txn_lat_inc_wait[worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.arrival_time, commit_stats));
    }
  }
}

/**
 * @brief Generate a dummy transaction to enforce a global-order over all workers
 *  This dummy transaction doesn't generate any log entry
 */
auto TransactionManager::AddBarrierTransaction() -> timestamp_t {
  /* Retrieve latest GSN */
  auto &logger          = log_manager_->LocalLogWorker();
  const auto sync_point = LogManager::global_sync_to_this_gsn.load();
  if (sync_point > logger.GetCurrentGSN()) { logger.SetCurrentGSN(sync_point); }
  /* Append a new barrier txn */
  auto dummy = Transaction();
  dummy.Initialize(this, global_clock.load(), Transaction::Type::SYSTEM, IsolationLevel::READ_UNCOMMITTED,
                   Transaction::Mode::OLTP, false);
  dummy.commit_ts          = dummy.start_ts;
  dummy.max_observed_gsn   = sync_point;
  dummy.state              = Transaction::State::BARRIER;
  dummy.needs_remote_flush = true;  // Require global synchronization
  QueueTransaction(dummy);
  return dummy.commit_ts;
}

void TransactionManager::QueueTransaction(Transaction &txn) {
  assert(FLAGS_wal_enable);
  auto &logger = log_manager_->LocalLogWorker();

  /* Use slow concurrent queue - default for flush pipelining and baseline commit */
  if (FLAGS_txn_queue_size_mb == 0) {
    /**
     * The below will give a slight advantage of latency of centralized logging.
     * However, I (XXX) am very lazy to implement a proper centralized logging for group commit, so it's fine
     */
    if (txn.needs_remote_flush && !FLAGS_wal_centralized_buffer) {
      logger.slow_pre_queue.Push(txn);
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_txn_processed[worker_thread_id] += 1;
      }
    } else {
      logger.slow_pre_queue_rfa.Push(txn);
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_rfa_txn_processed[worker_thread_id] += 1;
      }
    }
    return;
  }

  /* Enabling lock-free queue */
  assert(FLAGS_txn_queue_size_mb > 0);
  if ((FLAGS_txn_commit_variant == CommitProtocol::WORKERS_WRITE_LOG) ||
      (FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL)) {
    if (txn.needs_remote_flush) {
      logger.precommitted_queue.Push(txn);
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_txn_processed[worker_thread_id] += 1;
      }
    } else {
      if (FLAGS_txn_commit_variant == CommitProtocol::WILO_STEAL || txn.HasBLOB()) {
        logger.precommitted_queue_rfa.Push(txn);
      } else {
        logger.ms_queue_rfa.emplace_back(txn);
      }
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_rfa_txn_processed[worker_thread_id] += 1;
      }
    }
  } else {
    if (txn.needs_remote_flush) {
      logger.precommitted_queue.Push(txn);
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_txn_processed[worker_thread_id] += 1;
      }
    } else {
      logger.precommitted_queue_rfa.Push(txn);
      if (start_profiling && txn.state != Transaction::State::BARRIER) {
        statistics::precommited_rfa_txn_processed[worker_thread_id] += 1;
      }
    }
  }
}

template void TransactionManager::DurableCommit<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &, timestamp_t);
template void TransactionManager::DurableCommit<transaction::Transaction>(transaction::Transaction &, timestamp_t);

}  // namespace leanstore::transaction