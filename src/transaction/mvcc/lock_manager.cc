#include "transaction/mvcc/lock_manager.h"

namespace leanstore::transaction::mvcc {

thread_local LockManager::LocalWriteSet LockManager::write_set_;

void LockManager::ReleaseAllLocks([[maybe_unused]] timestamp_t txn_ts,
                                  const std::function<void(const LockableTuple *)> &iterate_fn) {
  std::erase_if(LockManager::write_set_, [&](const auto &tuple) {
    iterate_fn(tuple);
    // Lookup the WaitDieLock in the internal map
    if (!internal_.erase(const_cast<LockableTuple *>(tuple))) {
      throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
    }
    return true;  // remove everything
  });
}

// For MVCC, we never acquire shared lock on a tuple
bool LockManager::TryLockShared([[maybe_unused]] timestamp_t txn_ts, [[maybe_unused]] const LockableTuple *key) {
  return true;
}

bool LockManager::TryLock([[maybe_unused]] timestamp_t txn_ts, const LockableTuple *key) {
  // If already hold an exclusive lock on this tuple, return true immediately
  if (write_set_.contains(key)) { return true; }
  // Lookup or insert into the internal map to acquire X-lock
  InternalHashMap::accessor acc;
  auto granted = GetOrInsert(key, acc);
  if (granted) { write_set_.emplace(key); }
  return granted;
}

bool LockManager::TryUpgradeLock(timestamp_t txn_ts, const LockableTuple *key) { return TryLock(txn_ts, key); }

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