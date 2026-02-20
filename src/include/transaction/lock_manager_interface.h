#pragma once

#include "common/typedefs.h"
#include "transaction/lockable_tuple.h"

#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <unordered_map>

namespace leanstore::transaction {

enum class LockType { SHARED, EXCLUSIVE };

// Abstract lock manager interface
class ILockManager {
 public:
  virtual ~ILockManager() = default;

  virtual void ReleaseAllLocks() = 0;

  /**
   * Try to acquire a shared (read) lock for a transaction
   * @param txn_ts Transaction timestamp (for MVCC/SVCC)
   * @param key Key to lock
   * @return true if the lock can be acquired immediately, false if blocked
   */
  virtual bool TryLockShared(u64 txn_ts, const LockableTuple *) = 0;

  /**
   * Try to acquire an exclusive (write) lock for a transaction
   * @param txn_ts Transaction timestamp
   * @param key Key to lock
   * @return true if the lock can be acquired immediately, false if blocked
   */
  virtual bool TryLock(u64 txn_ts, const LockableTuple *) = 0;

  /**
   * Try to upgrade a held shared lock to an exclusive lock
   * @param txn_ts Transaction timestamp
   * @param key Key to upgrade lock
   * @return true if the upgrade can be done immediately, false if blocked
   */
  virtual bool TryUpgradeLock(u64 txn_ts, const LockableTuple *) = 0;

  /**
   * Release a previously acquired exclusive lock
   * @param txn_ts Transaction timestamp
   * @param key Key to unlock
   */
  virtual void Unlock(u64 txn_ts, const LockableTuple *) = 0;

  /**
   * Release a previously acquired shared lock
   * @param txn_ts Transaction timestamp
   * @param key Key to unlock
   */
  virtual void UnlockShared(u64 txn_ts, const LockableTuple *) = 0;
};

}  // namespace leanstore::transaction
