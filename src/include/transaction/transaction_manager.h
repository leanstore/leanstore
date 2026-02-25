#pragma once

#include "buffer/buffer_manager.h"
#include "leanstore/kv_interface.h"
#include "recovery/log_manager.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/mvcc/version_manager.h"
#include "transaction/transaction.h"

#include <atomic>
#include <thread>
#include <vector>

namespace leanstore::transaction {

class TransactionManager {
 public:
  static thread_local Transaction active_txn;
  static thread_local timestamp_t previous_completed_time;
  inline static std::atomic<timestamp_t> global_clock = 1;  // Valid timestamp always >= 1

  TransactionManager(buffer::BufferManager *buffer_manager, recovery::LogManager *log_manager,
                     std::atomic<bool> &is_running);
  ~TransactionManager();

  static auto ParseIsolationLevel(const std::string &str) -> IsolationLevel;

  void StartTransaction(Transaction::Type next_tx_type, timestamp_t next_tx_arrival_time = 0,
                        IsolationLevel next_tx_isolation_level = ParseIsolationLevel(FLAGS_txn_default_isolation_level),
                        Transaction::Mode next_tx_mode         = Transaction::Mode::OLTP);
  void CommitTransaction(const InternalCatalog &catalog);
  auto ValidateReadSet(const InternalCatalog &catalog) -> bool;
  void AbortTransaction();

  template <class T>
  static void DurableCommit(T &txn, timestamp_t queue_phase_start);

 private:
  friend class LeanStore;
  friend class Transaction;
  void QueueTransaction(Transaction &txn);

  buffer::BufferManager *buffer_;
  recovery::LogManager *log_manager_;
  std::unique_ptr<ILockManager> lock_manager_;
  std::unique_ptr<mvcc::VersionManager> version_manager_ = nullptr;
  std::thread background_version_gc_;
};

}  // namespace leanstore::transaction