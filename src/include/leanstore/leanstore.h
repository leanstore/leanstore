#pragma once

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/rand.h"
#include "common/utils.h"
#include "common/worker_pool.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/schema.h"
#include "leanstore/statistics.h"
#include "recovery/group_commit.h"
#include "recovery/log_manager.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "transaction/transaction_manager.h"

#include <random>
#include <string>
#include <thread>
#include <typeindex>
#include <typeinfo>

namespace leanstore {

using Transaction = transaction::Transaction;
using BlobState   = storage::blob::BlobState;

class LeanStore {
 public:
  // Database is running (Must be on TOP)
  std::atomic<bool> is_running = true;

  // Buffer Manager, Log Manager, Transaction Manager, & Group-Commit executor
  std::unique_ptr<buffer::BufferManager> buffer_pool;
  std::unique_ptr<recovery::LogManager> log_manager;
  std::unique_ptr<transaction::TransactionManager> transaction_manager;
  std::unique_ptr<storage::blob::BlobManager> blob_manager;

  // Worker pool (excluding Group commit thread & Profiling thread)
  WorkerPool worker_pool;
  std::thread stat_collector;
  std::thread group_committer;

  // Misc
  ZipfGenerator gen;
  std::unordered_map<std::type_index, std::unique_ptr<KVInterface>> indexes;  // Stupid Catalog

  LeanStore();
  ~LeanStore();
  void Shutdown();
  void CheckDuringIdle();

  // Catalog operations
  void RegisterTable(const std::type_index &relation);
  auto RetrieveIndex(const std::type_index &relation) -> KVInterface *;

  // Convenient txn helpers
  void StartTransaction(timestamp_t txn_arrival_time = 0, bool read_only = false,
                        Transaction::Mode tx_mode             = Transaction::Mode::OLTP,
                        const std::string &tx_isolation_level = FLAGS_txn_default_isolation_level);
  void CommitTransaction();
  void AbortTransaction();

  // Utilities for benchmarking
  auto AllocatedSize() -> float;
  auto DBSize() -> float;
  auto WALSize() -> float;
  void DropCache();

  // Background thread management
  void StartProfilingThread();

  // Blob utilities
  auto CreateNewBlob(std::span<const u8> blob_payload, BlobState *prev_blob, bool likely_grow) -> std::span<const u8>;
  void LoadBlob(const BlobState *blob_t, const storage::blob::BlobCallbackFunc &read_cb, bool partial_load = true);
  void RemoveBlob(BlobState *blob_t);

  // Comparison utilities
  auto RetrieveComparisonFunc(ComparisonOperator cmp_op) -> ComparisonLambda;
};

}  // namespace leanstore