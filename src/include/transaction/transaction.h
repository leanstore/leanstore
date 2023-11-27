#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "storage/extent/large_page.h"

#include "gtest/gtest_prod.h"

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <span>

namespace leanstore::recovery {
struct LogWorker;
class GroupCommitExecutor;
}  // namespace leanstore::recovery

namespace leanstore::transaction {

class TransactionManager;

enum class IsolationLevel : u8 {
  READ_UNCOMMITTED   = 0,
  READ_COMMITTED     = 1,
  SNAPSHOT_ISOLATION = 2,
  SERIALIZABLE       = 3,
};

class Transaction {
 public:
  enum class Type { USER, SYSTEM };
  enum class Mode { OLTP, OLAP };
  enum class State { IDLE, STARTED, READY_TO_COMMIT, COMMITTED, ABORTED };

  // debug properties
  struct {
    std::chrono::high_resolution_clock::time_point start, precommit, commit;
  } stats;

  bool wal_larger_than_buffer = false;

#ifdef ENABLE_TESTING
  void ResetState() { state_ = State::IDLE; }
#endif
  void Initialize(TransactionManager *manager, timestamp_t start_ts, Type txn_type, IsolationLevel level, Mode txn_mode,
                  bool read_only);

  // Txn context - public interfaces
  auto ReadOnly() -> bool;
  auto StartTS() -> timestamp_t;
  auto CommitTS() -> timestamp_t;
  auto StartWalCursor() -> u64;
  auto IsRunning() -> bool;
  void MarkAsWrite();
  auto LogWorker() -> recovery::LogWorker &;

  // Blob/Extent utility
  void LogBlob(std::span<const u8> payload);
  auto ToFlushedLargePages() -> storage::LargePageList &;
  auto ToEvictedExtents() -> storage::LargePageList &;
  auto ToFreeExtents() -> storage::TierList &;

  // PageGSN utilities
  template <class PageClass>
  void DetectGSNDependency(PageClass *page, pageid_t pid);
  template <class PageClass>
  void AdvancePageGSN(PageClass *page, pageid_t pid);

 private:
  friend class TransactionManager;
  friend struct recovery::LogWorker;
  friend class recovery::GroupCommitExecutor;
  FRIEND_TEST(TestTransaction, TransactionLifeTime);
  FRIEND_TEST(TestTransaction, AllocateBlobWithLogging);

  // Transaction config
  TransactionManager *manager_;
  bool is_read_only_;
  Type type_;
  Mode mode_;
  IsolationLevel iso_level_;

  // Transaction run-time
  State state_{State::IDLE};
  timestamp_t start_ts_;
  timestamp_t commit_ts_;

  // Logging context
  u64 wal_start_;             // The cursor value of LogWorker when this txn started
  logid_t max_observed_gsn_;  // The smallest upper-limit of gsn at which all logs of all workers have been flushed
  bool needs_remote_flush_;   // whether active_txn can avoid remote flush before commit

  // Required async-write these extents
  storage::LargePageList to_write_pages_;
  storage::LargePageList to_evict_extents_;
  storage::TierList to_free_extents_;
};

}  // namespace leanstore::transaction