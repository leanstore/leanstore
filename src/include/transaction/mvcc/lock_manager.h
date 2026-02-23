#pragma once

#include "common/typedefs.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include "tbb/concurrent_hash_map.h"

#include <unordered_set>

namespace leanstore::transaction::mvcc {

class LockManager : public ILockManager {
 public:
  using LocalWriteSet   = std::unordered_set<const LockableTuple *, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
  using InternalHashMap = tbb::concurrent_hash_map<LockableTuple *, bool, LockableTuple::HashTBB>;

  // Commit APIs
  void ReleaseAllLocks(timestamp_t txn_ts, const std::function<void(const LockableTuple *)> &iterate_fn) override;

  // Lock APIs
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryLock(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryUpgradeLock(timestamp_t txn_ts, const LockableTuple *) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *) override;

 private:
  auto GetOrInsert(const LockableTuple *key, InternalHashMap::accessor &out_acc) -> bool;

  // Thread-local write sets, i.e., exclusive locks that current txn is holding
  static thread_local LocalWriteSet write_set_;

  // Internal lock table mapping keys to WaitDieLocks
  InternalHashMap internal_;
};

}  // namespace leanstore::transaction::mvcc
