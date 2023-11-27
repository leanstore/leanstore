#include "transaction/transaction_manager.h"
#include "common/exceptions.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"

#include <chrono>

using LogManager = leanstore::recovery::LogManager;
using LogEntry   = leanstore::recovery::LogEntry;
using DataEntry  = leanstore::recovery::DataEntry;

namespace leanstore::transaction {

thread_local Transaction TransactionManager::active_txn = Transaction();

TransactionManager::TransactionManager(buffer::BufferManager *buffer_manager, LogManager *log_manager)
    : buffer_(buffer_manager), log_manager_(log_manager){};

auto TransactionManager::ParseIsolationLevel(const std::string &str) -> IsolationLevel {
  if (str == "ser") { return IsolationLevel::SERIALIZABLE; }
  if (str == "si") { return IsolationLevel::SNAPSHOT_ISOLATION; }
  if (str == "rc") { return IsolationLevel::READ_COMMITTED; }
  Ensure(str == "ru");
  return IsolationLevel::READ_UNCOMMITTED;
}

void TransactionManager::StartTransaction(Transaction::Type next_tx_type, IsolationLevel next_tx_isolation_level,
                                          Transaction::Mode next_tx_mode, bool read_only) {
  Ensure(!active_txn.IsRunning());
  active_txn.Initialize(this, global_clock++, next_tx_type, next_tx_isolation_level, next_tx_mode, read_only);
  if (FLAGS_txn_debug) {
    active_txn.stats.start            = std::chrono::high_resolution_clock::now();
    active_txn.wal_larger_than_buffer = false;
  }
  if (!FLAGS_wal_enable) { return; }
  // Propagate WAL-related run-time context for this active transaction
  auto &logger          = log_manager_->LocalLogWorker();
  active_txn.wal_start_ = logger.w_cursor;
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
   *  which means `needs_remote_flush_ = true`
   */
  if (FLAGS_wal_enable_rfa) {
    logger.rfa_gsn_flushed         = LogManager::global_min_gsn_flushed.load();
    active_txn.needs_remote_flush_ = false;
  } else {
    active_txn.needs_remote_flush_ = true;
  }
  if (next_tx_isolation_level >= IsolationLevel::READ_COMMITTED) { throw leanstore::ex::TODO("Not implemented yet"); }
}

void TransactionManager::CommitTransaction() {
  // If WAL is disabled or current transaction is read-only, commit transaction'll be a no-op
  Ensure(active_txn.state_ == Transaction::State::STARTED);
  // Update transactional context of current txn
  active_txn.commit_ts_ = global_clock++;
  if (FLAGS_txn_debug) {
    active_txn.stats.precommit        = std::chrono::high_resolution_clock::now();
    active_txn.wal_larger_than_buffer = false;
  }

  // Update the corresponding logs
  if (FLAGS_wal_enable && !active_txn.ReadOnly()) {
    auto &logger = log_manager_->LocalLogWorker();
    // Update RFA run-time env
    active_txn.max_observed_gsn_ = logger.w_gsn_clock;
    // Insert commit log entry to WAL
    auto &entry = logger.ReserveLogMetaEntry();
    entry.type  = LogEntry::Type::TX_COMMIT;
    logger.SubmitActiveLogEntry();
    // For debugging
    if (FLAGS_wal_debug) { entry.ComputeChksum(); }

    // Determine which group-commit queue for this active txn
    active_txn.state_ = Transaction::State::READY_TO_COMMIT;
    std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
    if (active_txn.needs_remote_flush_) {
      logger.precommitted_queue.push_back(active_txn);
      if (start_profiling) { statistics::precommited_txn_processed[worker_thread_id] += 1; }
    } else {
      logger.precommitted_queue_rfa.push_back(active_txn);
      if (start_profiling) { statistics::precommited_rfa_txn_processed[worker_thread_id] += 1; }
    }
  }

  // If log is disabled, update the statistics manually
  if (!FLAGS_wal_enable) {
    active_txn.state_ = Transaction::State::COMMITTED;
    statistics::txn_processed += 1;
  }
}

void TransactionManager::AbortTransaction() {
  // Only support abort txn if WAL is enabled
  Ensure(FLAGS_wal_enable);
  // A transaction was initialized, and it should be running
  Ensure(active_txn.IsRunning());
  active_txn.state_ = Transaction::State::ABORTED;
  // If current transaction is read-only, abort transaction'll be a no-op
  if (active_txn.ReadOnly()) { return; }
  if (FLAGS_txn_debug) { Ensure(!active_txn.wal_larger_than_buffer); }

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

}  // namespace leanstore::transaction