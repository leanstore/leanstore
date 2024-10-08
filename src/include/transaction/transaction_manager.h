#pragma once

#include "buffer/buffer_manager.h"
#include "recovery/log_manager.h"
#include "transaction/transaction.h"

#include <atomic>
#include <vector>

namespace leanstore::transaction {

class TransactionManager {
 public:
  static thread_local Transaction active_txn;
  static thread_local timestamp_t previous_completed_time;
  inline static std::atomic<timestamp_t> global_clock = 0;

  TransactionManager(buffer::BufferManager *buffer_manager, recovery::LogManager *log_manager);
  ~TransactionManager() = default;

  static auto ParseIsolationLevel(const std::string &str) -> IsolationLevel;

  void StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time = 0,
                        IsolationLevel next_tx_isolation_level = ParseIsolationLevel(FLAGS_txn_default_isolation_level),
                        Transaction::Mode next_tx_mode = Transaction::Mode::OLTP, bool read_only = false);

  void CommitTransaction();
  void AbortTransaction();
  auto AddBarrierTransaction() -> timestamp_t;

  template <class T>
  static void DurableCommit(T &txn, timestamp_t queue_phase_start);

 private:
  friend class LeanStore;
  friend class Transaction;
  void QueueTransaction(Transaction &txn);

  buffer::BufferManager *buffer_;
  recovery::LogManager *log_manager_;
};

}  // namespace leanstore::transaction