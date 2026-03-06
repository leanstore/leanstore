#pragma once

#include "common/typedefs.h"
#include "sync/epoch_handler.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"
#include "transaction/svcc/wait_die_lock.h"

#include "tbb/concurrent_hash_map.h"

#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace leanstore::transaction::svcc {

/**
 * @brief This lock manager prototype not yet reclaim memory stored by cold tuple and its associated wait-die lock
 * TODO(XXX): Implement a memory reclaimation for it
 */
class LockManager : public ILockManager {
 public:
  using LocalReadSet = std::unordered_set<const LockableTuple *, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
  using LocalWriteSet =
    std::unordered_map<const LockableTuple *, std::vector<u8>, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
  using InternalHashMap = tbb::concurrent_hash_map<LockableTuple *, WaitDieLock *, LockableTuple::HashTBB>;

  LockManager()  = default;
  ~LockManager() = default;

  bool EmptyLocalSet();
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;

  // Lock APIs
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryLock(timestamp_t txn_ts, timestamp_t undo_ts, std::span<u8> undo_payload, const LockableTuple *) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *) override;

 private:
  auto GetOrInsert(const LockableTuple *key) -> std::pair<LockableTuple *, WaitDieLock *>;

  // Thread-local set of currently held locks (read/write)
  static thread_local LocalReadSet read_set_;
  static thread_local LocalWriteSet write_set_;

  // Internal lock table mapping keys to WaitDieLocks
  InternalHashMap internal_;
};

}  // namespace leanstore::transaction::svcc
