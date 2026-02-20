#include "transaction/transaction_manager.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"
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
    : buffer_(buffer_manager), log_manager_(log_manager) {};

auto TransactionManager::ParseIsolationLevel(const std::string &str) -> IsolationLevel {
  if (str == "ser") { return IsolationLevel::SERIALIZABLE; }
  // if (str == "si") { return IsolationLevel::SNAPSHOT_ISOLATION; }
  // if (str == "rc") { return IsolationLevel::READ_COMMITTED; }
  Ensure(str == "ru");
  return IsolationLevel::READ_UNCOMMITTED;
}

void TransactionManager::StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time,
                                          IsolationLevel next_tx_isolation_level, Transaction::Mode next_tx_mode) {
  Ensure(!active_txn.IsRunning());
  active_txn.Initialize(this, global_clock++, next_tx_type, next_tx_isolation_level, next_tx_mode);
  if (FLAGS_txn_debug) {
    active_txn.stats.start        = tsctime::ReadTSC();
    active_txn.stats.arrival_time = (next_tx_arrival_time > 0) ? next_tx_arrival_time : active_txn.stats.start;
    assert(next_tx_arrival_time <= active_txn.stats.start);
    statistics::worker_idle_ns[LeanStore::worker_thread_id][Rand(SAMPLING_SIZE)] =
      active_txn.stats.arrival_time - previous_completed_time;
  }
  // Propagate WAL-related run-time context for this active transaction
  auto &logger          = log_manager_->LocalLogWorker();
  const auto sync_point = LogManager::global_sync_to_this_gsn.load();
  if (sync_point > logger.GetCurrentGSN()) {
    logger.SetCurrentGSN(sync_point);
    logger.PublicLocalGSN();
  }

  logger.rfa_gsn_flushed = LogManager::global_min_gsn_flushed.load();
  if (next_tx_isolation_level > IsolationLevel::READ_UNCOMMITTED) { throw leanstore::ex::TODO("Not implemented yet"); }
}

/* At the moment, `must_not_ack` is only used for testing purpose */
void TransactionManager::CommitTransaction(bool must_not_ack) {
  auto &logger = log_manager_->LocalLogWorker();

  Ensure(active_txn.state == Transaction::State::STARTED);
  // Update transactional context of current txn
  active_txn.commit_ts = global_clock++;
  active_txn.state     = Transaction::State::READY_TO_COMMIT;
  if (FLAGS_txn_debug) {
    active_txn.stats.precommit = tsctime::ReadTSC();
    previous_completed_time    = active_txn.stats.precommit;
  }

  // Append txn object to the pre-commit queue
  if (FLAGS_wal_enable) {
    // Insert commit log entry to WAL
    active_txn.MarkAsWrite();
    auto &entry        = logger.ReserveLogCommitEntry(active_txn.SerializedVectorSize());
    entry.vector_size  = active_txn.gsn_vector.size();
    auto should_commit = logger.SubmitActiveLogEntry();

    // Push the txn to the pre-commit queue
    QueueTransaction(active_txn);

    // Try to trigger group commit directly within the worker
    if (!must_not_ack) {
      if (should_commit || (Rand(BitLength(FLAGS_worker_count + 1)) == 0)) {
        log_manager_->TriggerGroupCommit(LeanStore::worker_thread_id / FLAGS_txn_commit_group_size);
      }
    }
  }

  // If log is disabled, update the statistics manually
  if (!FLAGS_wal_enable) {
    DurableCommit(active_txn, active_txn.stats.precommit);
    if (start_profiling) { statistics::precommited_txn_processed[LeanStore::worker_thread_id] += 1; }
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
      statistics::txn_queue[LeanStore::worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.precommit, queue_phase_start));
      statistics::txn_latency[LeanStore::worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.start, commit_stats));
      statistics::txn_exec[LeanStore::worker_thread_id].push_back(
        tsctime::TscDifferenceNs(txn.stats.start, txn.stats.precommit));
      statistics::lat_inc_wait[LeanStore::worker_thread_id].emplace_back(
        tsctime::TscDifferenceNs(txn.stats.arrival_time, commit_stats));
    }
  }
}

void TransactionManager::QueueTransaction(Transaction &txn) {
  assert(FLAGS_wal_enable);
  auto &logger = log_manager_->LocalLogWorker();

  /* Enabling lock-free queue */
  logger.precommitted_queue.Push(txn);
  if (start_profiling) { statistics::precommited_txn_processed[LeanStore::worker_thread_id] += 1; }
}

template void TransactionManager::DurableCommit<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &, timestamp_t);
template void TransactionManager::DurableCommit<transaction::Transaction>(transaction::Transaction &, timestamp_t);

}  // namespace leanstore::transaction