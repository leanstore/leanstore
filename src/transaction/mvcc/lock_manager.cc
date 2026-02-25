#include "transaction/mvcc/lock_manager.h"
#include "common/exceptions.h"

namespace leanstore::transaction::mvcc {

thread_local LockManager::LocalReadSet LockManager::read_set_;
thread_local LockManager::LocalWriteSet LockManager::write_set_;

void LockManager::SetTupleTimestamp(const LockableTuple *key, timestamp_t tuple_ts) {
  auto it = read_set_.find(key);
  Ensure(it != read_set_.end() && ((it->second == INVALID_TS) || (it->second == tuple_ts)));
  read_set_[key] = tuple_ts;
}

void LockManager::ReleaseAllLocks([[maybe_unused]] timestamp_t txn_ts,
                                  const std::function<void(const LockableTuple *)> &update_tuple_ts_fn) {
  std::erase_if(LockManager::write_set_, [&](const auto &tuple) {
    update_tuple_ts_fn(tuple);
    // Lookup the WaitDieLock in the internal map
    if (!internal_.erase(const_cast<LockableTuple *>(tuple))) {
      throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
    }
    return true;  // remove everything
  });
}

void LockManager::ValidateReadSet(const std::function<void(const LockableTuple *, timestamp_t)> &validate_fn) {
  std::erase_if(LockManager::read_set_, [&](const auto &tuple) {
    Ensure(tuple.second != INVALID_TS);
    validate_fn(tuple.first, tuple.second);
    return true;  // remove everything
  });
}

// For MVCC:
// - we never acquire shared lock on a tuple.
// - After calling this fn, we always call SetTupleTimestamp()
bool LockManager::TryLockShared([[maybe_unused]] timestamp_t txn_ts, const LockableTuple *key) {
  // The correct timestamp will be updated later using SetTupleTimestamp()
  if (!read_set_.contains(key)) { read_set_.insert({key, INVALID_TS}); }
  return true;
}

bool LockManager::TryLock([[maybe_unused]] timestamp_t txn_ts, timestamp_t latest_tuple_ts, const LockableTuple *key) {
  assert(txn_ts >= latest_tuple_ts);
  // If already hold an exclusive lock on this tuple, return true immediately
  if (write_set_.contains(key)) { return true; }

  InternalHashMap::accessor acc;
  // Lookup or insert into the internal map to acquire X-lock
  auto granted = GetOrInsert(key, acc);
  if (granted) {
    // If we already read this key, need to double check if we are reading the latest value
    auto it = read_set_.find(key);
    if (it != read_set_.end()) {
      if (it->second != latest_tuple_ts) {
        // We read previous version, hence release the current lock and abort txn
        read_set_.erase(it);
        internal_.erase(const_cast<LockableTuple *>(key));
        return false;
      }
      read_set_.erase(it);  // This txn reads the latest version, move this tuple to write set
    }
    // Acquire successfully
    write_set_.emplace(key);
  }
  return granted;
}

void LockManager::Unlock([[maybe_unused]] timestamp_t txn_ts, const LockableTuple *key) {
  // Check if we currently hold the lock
  auto it = write_set_.find(key);
  if (it == write_set_.end()) { throw std::runtime_error("Unlock called without holding an exclusive lock"); }

  // Lookup the WaitDieLock in the internal map
  auto success = internal_.erase(const_cast<LockableTuple *>(key));
  if (!success) { throw std::runtime_error("Unlock: Lock object missing in internal map"); }

  // Release the exclusive lock & Remove from thread-local map
  write_set_.erase(it);
}

void LockManager::UnlockShared([[maybe_unused]] timestamp_t txn_ts, [[maybe_unused]] const LockableTuple *key) {
  (void)0;  // no-op
}

auto LockManager::GetOrInsert(const LockableTuple *key, InternalHashMap::accessor &out_acc) -> bool {
  auto new_key = LockableTuple::Constructor(*key);  // allocate new key
  auto success = internal_.insert(out_acc, new_key);
  if (!success) { LockableTuple::Release(new_key); }
  return success;
}

}  // namespace leanstore::transaction::mvcc