#pragma once

#include "common/typedefs.h"
#include "transaction/lock_manager_interface.h"
#include "transaction/lockable_tuple.h"
#include "transaction/mvcc/version_manager.h"

#include "tbb/concurrent_hash_map.h"

#include <unordered_map>

namespace leanstore::transaction::mvcc {

class LockManager : public ILockManager {
 public:
  using LocalWriteSet =
    std::unordered_map<const LockableTuple *, TupleVersion *, LockableTuple::HashPtr, LockableTuple::EqualPtr>;
  using LocalReadSet =
    std::unordered_map<const LockableTuple *, timestamp_t, LockableTuple::HashPtr, LockableTuple::EqualPtr>;

  // Misc helpers
  LockManager(VersionManager *ver_);
  void SetTupleTimestamp(const LockableTuple *, timestamp_t tuple_ts, bool require_serializable);
  auto GetTupleTimestamp(const LockableTuple *) -> timestamp_t;

  // Commit APIs
  bool EmptyLocalSet();
  void ValidateReadSet(const std::function<void(const LockableTuple *, timestamp_t)> &validate_fn);
  void ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) override;

  // Lock APIs
  static auto OwnTuple(const LockableTuple *) -> bool;
  bool TryLockShared(timestamp_t txn_ts, const LockableTuple *) override;
  bool TryLock(timestamp_t txn_ts, timestamp_t undo_ts, std::span<u8> undo_payload, const LockableTuple *) override;
  void Unlock(timestamp_t txn_ts, const LockableTuple *) override;
  void UnlockShared(timestamp_t txn_ts, const LockableTuple *) override;

 private:
  // Thread-local read sets, for validating reads at commit time
  static thread_local LocalReadSet read_set_;

  // Thread-local write sets, i.e., exclusive locks that current txn is holding
  static thread_local LocalWriteSet write_set_;

  // Internal lock table mapping keys to WaitDieLocks
  VersionManager *version_manager_;
};

}  // namespace leanstore::transaction::mvcc
