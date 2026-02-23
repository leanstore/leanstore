#pragma once

#include "common/typedefs.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"
#include "transaction/svcc/wait_die_lock.h"

#include "tbb/concurrent_hash_map.h"

#include <unordered_map>

namespace leanstore::transaction::svcc {

class LockManager : public ILockManager {
 public:
  using LocalReadWriteSet =
    std::unordered_map<const LockableTuple *, LockType, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
  using InternalHashMap = tbb::concurrent_hash_map<LockableTuple *, WaitDieLock, LockableTuple::HashTBB>;
  LockManager()         = default;
  ~LockManager()        = default;

  void ReleaseAllLocks(timestamp_t txn_ts, const std::function<void(const LockableTuple *)> &iterate_fn) override;

  // Lock APIs
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryLock(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryUpgradeLock(timestamp_t txn_ts, const LockableTuple *) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *) override;

 private:
  void GetOrInsert(const LockableTuple *key, InternalHashMap::accessor &out_acc);

  // Thread-local set of currently held locks (read/write)
  static thread_local LocalReadWriteSet rws_;

  // Internal lock table mapping keys to WaitDieLocks
  InternalHashMap internal_;
};

}  // namespace leanstore::transaction::svcc
