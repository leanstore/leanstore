#pragma once

#include "common/typedefs.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"

#include "tbb/concurrent_hash_map.h"

#include <unordered_map>

namespace leanstore::transaction::mvcc {

class LockManager : public ILockManager {
 public:
  void ReleaseAllLocks() override;

  // Lock APIs
  bool TryLockShared(u64 txn_ts, const LockableTuple *) override;
  bool TryLock(u64 txn_ts, const LockableTuple *) override;
  bool TryUpgradeLock(u64 txn_ts, const LockableTuple *) override;
  void Unlock(u64 txn_ts, const LockableTuple *) override;
  void UnlockShared(u64 txn_ts, const LockableTuple *) override;

 private:
};

}  // namespace leanstore::transaction::mvcc
