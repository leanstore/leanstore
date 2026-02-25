#include "transaction/svcc/lock_manager.h"

namespace leanstore::transaction::svcc {

thread_local LockManager::LocalReadWriteSet LockManager::rws_;

void LockManager::ReleaseAllLocks(timestamp_t txn_ts,
                                  [[maybe_unused]] const std::function<void(const LockableTuple *)> &iterate_fn) {
  std::erase_if(LockManager::rws_, [&](const auto &kv) {
    const auto &[tuple, lock_type] = kv;

    // Lookup the WaitDieLock in the internal map
    InternalHashMap::accessor acc;
    if (!internal_.find(acc, const_cast<LockableTuple *>(tuple))) {
      throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
    }

    // Release the exclusive lock
    if (lock_type == LockType::SHARED) {
      acc->second.UnlockShared(txn_ts);
    } else {
      assert(lock_type == LockType::EXCLUSIVE);
      acc->second.Unlock(txn_ts);
    }

    return true;  // remove everything
  });
}

bool LockManager::TryLockShared(u64 txn_ts, const LockableTuple *key) {
  // Already hold an lock on the tuple, return
  if (LockManager::rws_.contains(key)) { return true; }
  // Haven't locked the tuple before, insert a new WaitDieLock in the internal map
  InternalHashMap::accessor acc;
  GetOrInsert(key, acc);
  // trying to lock it in shared mode
  bool granted = acc->second.TryLockShared(txn_ts);
  if (granted) { rws_.emplace(key, LockType::SHARED); }
  return granted;
}

bool LockManager::TryLock(u64 txn_ts, [[maybe_unused]] timestamp_t tuple_ts, const LockableTuple *key) {
  InternalHashMap::accessor acc;

  // If already hold a lock on this tuple
  auto it = rws_.find(key);
  if (it != rws_.end()) {
    // If we already hold the lock, make sure it is exclusive
    if (it->second == LockType::EXCLUSIVE) { return true; }

    // Otherwise, try upgrade the lock
    assert(it->second == LockType::SHARED);
    if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
      throw std::runtime_error("TryLock: This lock must be already held in SHARED mode");
    }

    // Try to upgrade using WaitDieLock
    bool upgraded = acc->second.TryLockUpgrade(txn_ts);
    if (upgraded) { it->second = LockType::EXCLUSIVE; }
    return upgraded;
  }

  // Otherwise, insert WaitDieLock to the internal map
  GetOrInsert(key, acc);
  // Try to acquire exclusive lock
  bool granted = acc->second.TryLock(txn_ts);
  // If granted, track it in thread-local map
  if (granted) { rws_.emplace(key, LockType::EXCLUSIVE); }
  return granted;
}

void LockManager::Unlock(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold the lock
  auto it = rws_.find(key);
  if (it == rws_.end() || it->second != LockType::EXCLUSIVE) {
    throw std::runtime_error("Unlock called without holding an exclusive lock");
  }

  // Lookup the WaitDieLock in the internal map
  InternalHashMap::accessor acc;
  if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
    throw std::runtime_error("Unlock: Lock object missing in internal map");
  }

  // Release the exclusive lock & Remove from thread-local map
  acc->second.Unlock(txn_ts);
  rws_.erase(it);
}

void LockManager::UnlockShared(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold a shared lock
  auto it = rws_.find(key);
  if (it == rws_.end() || it->second != LockType::SHARED) {
    throw std::runtime_error("UnlockShared called without holding a shared lock");
  }

  // Lookup the WaitDieLock in the internal map
  InternalHashMap::accessor acc;
  if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
    throw std::runtime_error("UnlockShared: Lock object missing in internal map");
  }

  // Release the shared lock & Remove from thread-local map
  acc->second.UnlockShared(txn_ts);
  rws_.erase(it);
}

void LockManager::GetOrInsert(const LockableTuple *key, InternalHashMap::accessor &out_acc) {
  auto new_key = LockableTuple::Constructor(*key);  // allocate new key
  auto success = internal_.insert(out_acc, new_key);
  if (!success) { LockableTuple::Release(new_key); }
}

}  // namespace leanstore::transaction::svcc