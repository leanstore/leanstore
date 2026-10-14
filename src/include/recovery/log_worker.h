#pragma once

#include "common/queue.h"
#include "common/typedefs.h"
#include "recovery/log_backend.h"
#include "recovery/log_buffer.h"
#include "recovery/log_entry.h"
#include "sync/hybrid_guard.h"
#include "sync/hybrid_latch.h"
#include "transaction/transaction.h"

#include "liburing.h"

#include <atomic>
#include <deque>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

namespace leanstore::recovery {

class LogManager;

struct ToCommitState;
struct AsyncCommitState;

/* Atomic version of worker's consistent state */
struct WorkerConsistentState {
  u64 last_wal_cursor                    = 0;  // wal_cursor
  timestamp_t last_gsn                   = 0;  // w_gsn_clock
  timestamp_t precommitted_tx_commit_ts  = 0;
  timestamp_t precommitted_tx_commit_gsn = 0;
  sync::HybridLatch latch;

  void Clone(WorkerConsistentState &other);
  void Clone(const AsyncCommitState &other);
  void Clone(const ToCommitState &other);

  template <typename T>
  void SyncClone(T &other) {
    sync::HybridGuard guard(&latch, sync::GuardMode::EXCLUSIVE);
    Clone(other);
  }
};

/**
 * @brief Sometimes the worker's consistent state is already protected
 * This struct is for that purpose: un-atomic consistent state
 */
struct ToCommitState {
  u64 last_wal_cursor                    = 0;  // wal_cursor
  timestamp_t last_gsn                   = 0;  // w_gsn_clock
  timestamp_t precommitted_tx_commit_ts  = 0;
  timestamp_t precommitted_tx_commit_gsn = 0;

  auto operator<(const ToCommitState &r) -> bool;
  explicit ToCommitState(const WorkerConsistentState &base);
};

/**
 * @brief Sometimes the worker's consistent state is already protected
 * This struct is for that purpose: un-atomic consistent state
 */
struct AsyncCommitState {
  u64 req_lsn                            = 0;  // the corresponding lsn of write request to cloud log service
  u64 last_wal_cursor                    = 0;  // wal_cursor
  timestamp_t last_gsn                   = 0;  // w_gsn_clock
  timestamp_t precommitted_tx_commit_ts  = 0;
  timestamp_t precommitted_tx_commit_gsn = 0;

  AsyncCommitState() = default;
  AsyncCommitState(const WorkerConsistentState &base, u64 lsn);
  auto operator<(const AsyncCommitState &r) const -> bool;
};

/**
 * @brief VECTOR variant: a precommitted txn with dependencies which is not yet part of its worker's stable prefix
 * `prev_commit_gsn` and `prev_commit_ts` belong to the worker's txn before it, where the prefix ends while it is not
 */
struct PendingCommit {
  timestamp_t prev_commit_gsn;
  timestamp_t prev_commit_ts;
  timestamp_t commit_ts;
  std::vector<std::pair<wid_t, timestamp_t>> dependencies;
};

/**
 * @brief VECTOR variant: the stable prefix of a worker's log
 * A txn is stable if its commit is durable and so is the commit of every txn it depends on, transitively,
 *  including the earlier txns of its own worker.
 * `gsn` is the commit GSN of the last stable txn, `commit_ts` its commit timestamp.
 * Every data entry with GSN <= `gsn` belongs to a stable txn, because a txn's entries are stamped
 *  above the commit GSN of the txn before it.
 * Only txns with dependencies can end the prefix, so `pending` holds no others.
 */
struct StablePrefix {
  std::atomic<timestamp_t> gsn       = 0;
  std::atomic<timestamp_t> commit_ts = 0;
  std::deque<PendingCommit> pending;
  std::mutex pending_mutex;
};

/**
 * Abbreviations:
 *  rfa: Remote flush avoidance
 *  lsn: Log sequence number
 *  gsn: Global sequence number
 */
struct LogWorker {
  enum StealDecision : u8 { TO_STEAL, TO_WRITE_LOCALLY, NOTHING };

  // Environments
  LogManager *log_manager;
  std::atomic<bool> *is_running = nullptr;
  LogEntry *active_log          = nullptr;
  WorkerConsistentState *w_state;

  /**
   * @brief Decentralized commit state
   * There are two life cycles implemented in LeanStore:
   * - Sync lifecycle:
   *  + Support log stealing, hence multiple threads can access the `parking_lot_log_flush`
   *  + Require a latch to protect `parking_lot_log_flush`
   * - Async lifecycle:
   *  + Does not support log stealing. Theoretically, it's possible.
   *    The main reason not to support is its significant complexity, and its benefit is not worth it
   *  + Therefore, accessing `async_parking_lot` does not require latching
   */
  std::map<timestamp_t, AsyncCommitState> async_parking_lot;  // Used when FLAGS_wal_enable_async == true
  std::vector<ToCommitState> parking_lot_log_flush;           // Used when FLAGS_wal_enable_async == false
  sync::HybridLatch parking_latch;

  /* Timestamp management */
  timestamp_t w_gsn_clock              = 0;  // local copy of gsn, i.e. w_gsn_clock <= gsn.load()
  timestamp_t rfa_gsn_flushed          = 0;  // local copy of LogManager.global_min_gsn_flushed
  timestamp_t last_unharden_commit_ts  = 0;  // the commit timestamp of last harden transaction
  timestamp_t last_unharden_commit_gsn = 0;  // the commit GSN of last harden transaction

  /* Log buffer */
  LogBuffer log_buffer;
  LogBackend backend;

  /**
   * @brief Queues to store all precommitted transactions
   *
   * There are three queues:
   * - ms_queue_rfa: Mosaic-style queue where the local worker manage the commit state of transactions
   *      We only use this to manage rfa transactions, whose commit state only relies on log durability
   * - precommitted_queue: SPSC lock-free queue, sharing between 1 group commit thread and 1 worker
   * - precommitted_queue_rfa: Similar to above, but with RFA transactions.
   *      This should be used when non-RFA txns contain BLOB operations
   */
  LockFreeQueue<transaction::SerializableTransaction> precommitted_queue;
  LockFreeQueue<transaction::SerializableTransaction> precommitted_queue_rfa;

  /* Advanced by LogManager::AdvanceStablePrefix(), which consumes `stable.pending` */
  StablePrefix stable;

  LogWorker(std::atomic<bool> &db_is_running, LogManager *log_manager);
  ~LogWorker();

  // LogSequenceNumber utilities
  auto GetCurrentGSN() -> timestamp_t;
  void SetCurrentGSN(timestamp_t gsn);

  // WorkerConsistentState utilities - public local state info to the group committer
  void PublicWCursor();
  void PublicLocalGSN();
  void PublicCommitTS();

  /**
   * @brief Data log entries are usually bigger & generated more frequently than other log types.
   * In WILO_SHARE_BUFFER design, a worker may wait for its peer to write its log buffer to the storage.
   * This causes performance issue, considering that (-> means depends on the completion of next op)
   * Optimistic Latch
   *    -> Exclusive Latch
   *    -> Log generation (WAL_RECORD in tree.cc)
   *    -> EnsureEnoughSpace() -> Log write or wait for log write
   *
   * Therefore, we should provide an alternative life cycle for DataLog,
   *  which decouple Exclusive Latch and Log generation.
   */
  auto PrepareDataLogEntry(u8 *buffer, u64 payload_size, pageid_t pid) -> DataEntry &;
  void SubmitPreparedLogEntry();

  // Log entry utilities
  auto ReserveLogMetaEntry() -> LogMetaEntry &;
  auto ReserveLogCommitEntry(u64 payload_size) -> TxnCommitEntry &;
  auto ReserveDataLog(u64 payload_size, pageid_t pid) -> DataEntry &;
  auto ReserveFreeExtentLogEntry(bool to_free, pageid_t start_pid, pageid_t size) -> FreeExtentEntry &;
  auto ReservePageImageLog(u64 payload_size, pageid_t pid) -> PageImgEntry &;
  auto SubmitActiveLogEntry() -> bool;
  void AppendPendingCommit(const transaction::Transaction &txn);

  // Log buffer utilities
  auto ShouldStealLog() -> StealDecision;
  void WorkerWritesLog();
  void WorkerStealsLog(bool write_only);
  auto TryStealLogs(wid_t peer_id, WorkerConsistentState &out_state) -> bool;
  void TryPublishCommitState(wid_t w_id, const WorkerConsistentState &w_state);

  // Misc utilities
  void IterateActiveTxnEntries(const std::function<void(const LogEntry &entry)> &log_cb);
};

}  // namespace leanstore::recovery
