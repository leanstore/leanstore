#pragma once

#include "common/typedefs.h"
#include "transaction/lockable_tuple.h"

#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <stdexcept>
#include <unordered_map>

namespace leanstore::transaction {

using WriteSetCallback = std::function<void(const LockableTuple *, timestamp_t, std::span<const u8>)>;

// Abstract lock manager interface
class ILockManager {
 public:
  virtual ~ILockManager() = default;

  virtual bool EmptyLocalSet()                                                           = 0;
  virtual void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) = 0;

  /**
   * Try to acquire a shared (read) lock for a transaction
   * @param txn_ts Transaction timestamp (for MVCC/SVCC)
   * @param key Key to lock
   * @return true if the lock can be acquired immediately, false if blocked
   */
  virtual bool TryLockShared(timestamp_t txn_ts, const LockableTuple *) = 0;

  /**
   * Try to acquire an exclusive (write) lock for a transaction
   * @param txn_ts Transaction start timestamp
   * @param undo_ts The tuple's timestamp
   * @param undo_payload The prev payload, used for undo
   * @param key Key to lock
   * @return true if the lock can be acquired immediately, false if blocked
   */
  virtual bool TryLock(timestamp_t txn_ts, timestamp_t undo_ts, std::span<u8> undo_payload, const LockableTuple *) = 0;

  /**
   * Release a previously acquired exclusive lock
   * @param txn_ts Transaction timestamp
   * @param key Key to unlock
   */
  virtual void Unlock(timestamp_t txn_ts, const LockableTuple *) = 0;

  /**
   * Release a previously acquired shared lock
   * @param txn_ts Transaction timestamp
   * @param key Key to unlock
   */
  virtual void UnlockShared(timestamp_t txn_ts, const LockableTuple *) = 0;
};

}  // namespace leanstore::transaction
