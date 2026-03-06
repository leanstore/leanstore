#include "transaction/transaction_manager.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "recovery/log_entry.h"
#include "storage/btree/tree.h"
#include "transaction/mvcc/lock_manager.h"
#include "transaction/svcc/lock_manager.h"

#include "share_headers/time.h"

#include <chrono>
#include <thread>

using LogManager = leanstore::recovery::LogManager;
using LogEntry   = leanstore::recovery::LogEntry;
using DataEntry  = leanstore::recovery::DataEntry;

namespace leanstore::transaction {

thread_local timestamp_t TransactionManager::previous_completed_time = 0;

TransactionManager::TransactionManager(buffer::BufferManager *buffer_manager, LogManager *log_manager,
                                       std::atomic<bool> &is_running)
    : buffer_(buffer_manager), log_manager_(log_manager) {
  if (FLAGS_txn_mvcc) {
    version_manager_       = std::make_unique<mvcc::VersionManager>();
    lock_manager_          = std::make_unique<mvcc::LockManager>(version_manager_.get());
    background_version_gc_ = std::thread([&]() {
      while (is_running.load(std::memory_order_relaxed)) {
        version_manager_->Sweep();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    });
  } else {
    lock_manager_          = std::make_unique<svcc::LockManager>();
    background_version_gc_ = std::thread();
  }
};

TransactionManager::~TransactionManager() {
  if (background_version_gc_.joinable()) {
    background_version_gc_.join();  // blocks until thread finishes
  }
}

auto TransactionManager::ParseIsolationLevel(const std::string &str) -> IsolationLevel {
  if (str == "ser") { return IsolationLevel::SERIALIZABLE; }
  if (str == "si") { return IsolationLevel::SNAPSHOT_ISOLATION; }
  Ensure(str == "ru");
  return IsolationLevel::READ_UNCOMMITTED;
}

void TransactionManager::StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time,
                                          IsolationLevel next_tx_isolation_level, Transaction::Mode next_tx_mode) {
  auto &txn = Transaction::active_txn;
  Ensure(!txn.IsRunning());
  txn.Initialize(this, global_clock++, next_tx_type, next_tx_isolation_level, next_tx_mode);
  if (FLAGS_txn_debug) {
    txn.stats.start        = tsctime::ReadTSC();
    txn.stats.arrival_time = (next_tx_arrival_time > 0) ? next_tx_arrival_time : txn.stats.start;
    assert(next_tx_arrival_time <= txn.stats.start);
    statistics::worker_idle_ns[LeanStore::worker_thread_id][Rand(SAMPLING_SIZE)] =
      txn.stats.arrival_time - previous_completed_time;
  }
  // Propagate WAL-related run-time context for this active transaction
  auto &logger          = log_manager_->LocalLogWorker();
  const auto sync_point = LogManager::global_sync_to_this_gsn.load();
  if (sync_point > logger.GetCurrentGSN()) {
    logger.SetCurrentGSN(sync_point);
    logger.PublicLocalGSN();
  }

  logger.rfa_gsn_flushed = LogManager::global_min_gsn_flushed.load();
}

void TransactionManager::CommitTransaction() {
  auto &logger = log_manager_->LocalLogWorker();
  auto &txn    = Transaction::active_txn;
  Ensure(txn.state == Transaction::State::STARTED);

  // Assign timestamp and Release locks according to Concurrency Control
  txn.commit_ts = global_clock++;
  if (txn.iso_level > IsolationLevel::READ_UNCOMMITTED) {
    lock_manager_->ReleaseAllLocks(txn.start_ts, [&](const LockableTuple *tuple, timestamp_t, std::span<const u8>) {
      // This lambda -- Updating tuple's TS -- will only be triggered by MVCC impl
      if (FLAGS_txn_mvcc) {
        auto index = reinterpret_cast<storage::BTree *>(catalog[tuple->tree_id]);
        index->UpdateTimestamp({const_cast<u8 *>(tuple->key), tuple->key_len}, txn.commit_ts);
      }
    });
  }
  assert(lock_manager_->EmptyLocalSet());

  // Update transactional context of current txn
  txn.state = Transaction::State::READY_TO_COMMIT;
  if (FLAGS_txn_debug) {
    txn.stats.precommit     = tsctime::ReadTSC();
    previous_completed_time = txn.stats.precommit;
  }

  // Append txn object to the pre-commit queue
  if (FLAGS_wal_enable) {
    // Insert commit log entry to WAL
    txn.MarkAsWrite();
    auto &entry        = logger.ReserveLogCommitEntry(txn.SerializedVectorSize());
    entry.vector_size  = txn.gsn_vector.size();
    auto should_commit = logger.SubmitActiveLogEntry();

    // Push the txn to the pre-commit queue
    QueueTransaction(txn);

    // Try to trigger group commit directly within the worker
    if (should_commit || (FLAGS_wal_force_log_flush && (Rand(BitLength(FLAGS_worker_count + 1)) == 0))) {
      log_manager_->TriggerGroupCommit(LeanStore::worker_thread_id / FLAGS_txn_commit_group_size);
    }
  }

  // If log is disabled, update the statistics manually
  if (!FLAGS_wal_enable) {
    DurableCommit(txn, txn.stats.precommit);
    if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
  }

  // Advance safe commit ts in version manager
  if (FLAGS_txn_mvcc) { version_manager_->AdvanceLocalTimestamp(LeanStore::worker_thread_id, txn.commit_ts); }
}

auto TransactionManager::ValidateReadSet() -> bool {
  auto &txn = Transaction::active_txn;

  // Only validate read set if running under SERIALIZABLE level with MVCC
  if (!FLAGS_txn_mvcc || txn.iso_level < IsolationLevel::SERIALIZABLE) { return true; }
  auto mvcc_lock_manager = reinterpret_cast<mvcc::LockManager *>(lock_manager_.get());
  auto satisfy_occ       = true;
  // TODO(XXX): Implement the follow atomic-way
  // Yes: https://pages.cs.wisc.edu/~yxy/cs764-f20/slides/L24.pdf - Slide 10
  mvcc_lock_manager->ValidateReadSet([&](const LockableTuple *tuple, timestamp_t tuple_ts) {
    auto index            = reinterpret_cast<storage::BTree *>(catalog[tuple->tree_id]);
    auto current_tuple_ts = index->GetTimestamp({const_cast<u8 *>(tuple->key), tuple->key_len});
    if (current_tuple_ts != tuple_ts) { satisfy_occ = false; }
  });
  return satisfy_occ;
}

/**
 * @brief TODO(XXX): Implement AbortTransaction -- rollback changes
 * Should also handle cases when logs are already flushed to the storage, and being overwritten in memory
 * Maybe, for simplicity, we simply treat this as a no-op and do not create any CLR
 */
void TransactionManager::AbortTransaction() {
  auto &txn    = Transaction::active_txn;
  auto &logger = log_manager_->LocalLogWorker();
  Ensure(txn.IsRunning());

  // Iterate through lock_manager.write_set_ and re-install undo payload
  // If the undo payload is empty -> prev version does not exist, call tree delete
  // Otherwise, call tree update
  if (txn.iso_level > IsolationLevel::READ_UNCOMMITTED) {
    lock_manager_->ReleaseAllLocks(txn.start_ts, [&](const LockableTuple *tuple, auto undo_ts, auto payload) {
      // This lambda -- Updating tuple's TS -- will only be triggered by MVCC impl
      auto index = reinterpret_cast<storage::BTree *>(catalog[tuple->tree_id]);
      OpResult ret;
      Transaction::TUPLE_UNDO_TIMESTAMP = undo_ts;
      if (payload.empty()) {
        ret = index->Remove({const_cast<u8 *>(tuple->key), tuple->key_len});
      } else {
        ret = index->Upsert({const_cast<u8 *>(tuple->key), tuple->key_len}, payload);
      }
      assert(ret == OpResult::OK);
    });
  }
  assert(lock_manager_->EmptyLocalSet());

  // TODO(XXX): To fully support recovery, need to generate CLRs (compensation log record) here
  // before generating TX_ABORT log entry

  // Now, mark the transaction as aborted
  txn.state = Transaction::State::ABORTED;
  if (FLAGS_txn_debug) {
    txn.stats.precommit     = tsctime::ReadTSC();
    previous_completed_time = txn.stats.precommit;
  }

  // Append txn object to the pre-commit queue
  if (FLAGS_wal_enable) {
    // Insert abort log entry to WAL
    txn.MarkAsWrite();
    auto &entry       = logger.ReserveLogCommitEntry(txn.SerializedVectorSize());
    entry.vector_size = txn.gsn_vector.size();
    entry.type        = LogEntry::Type::TX_ABORT;
    logger.SubmitActiveLogEntry();

    // Push the txn to the pre-commit queue
    QueueTransaction(txn);
  }

  // if WAl is disabled, simply ack the txn
  if (!FLAGS_wal_enable) {
    if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
  }
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
  if (start_profiling) { statistics::precommited_txn[LeanStore::worker_thread_id] += 1; }
}

template void TransactionManager::DurableCommit<transaction::SerializableTransaction>(
  transaction::SerializableTransaction &, timestamp_t);
template void TransactionManager::DurableCommit<transaction::Transaction>(transaction::Transaction &, timestamp_t);

}  // namespace leanstore::transaction