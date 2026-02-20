#include "transaction/svcc/lock_manager.h"

namespace leanstore::transaction::svcc {

thread_local std::unordered_map<const LockableTuple *, LockType, LockableTuple::HashPtr, LockableTuple::EqualPtr>
  LockManager::rws_;

void LockManager::ReleaseAllLocks() {}

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

bool LockManager::TryLock(u64 txn_ts, const LockableTuple *key) {
  // If already hold an exclusive lock on this tuple, return true immediately
  auto it = rws_.find(key);
  if (it != rws_.end()) {
    // If we already hold the lock, make sure it is exclusive
    if (it->second == LockType::EXCLUSIVE) { return true; }

    // Otherwise, try upgrade the lock
    return TryUpgradeLock(txn_ts, key);
  }
  // Lookup or insert WaitDieLock in the internal map
  InternalHashMap::accessor acc;
  GetOrInsert(key, acc);
  // Try to acquire exclusive lock
  bool granted = acc->second.TryLock(txn_ts);
  // If granted, track it in thread-local map
  if (granted) { rws_.emplace(key, LockType::EXCLUSIVE); }
  return granted;
}

bool LockManager::TryUpgradeLock(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold a shared lock
  auto it = rws_.find(key);
  // If we already hold the lock, make sure it is exclusive
  if (it != rws_.end() && it->second == LockType::EXCLUSIVE) { return true; }
  // Cannot upgrade if we don't hold a shared lock
  if (it == rws_.end() || it->second != LockType::SHARED) { return false; }

  // Lookup the WaitDieLock in the internal map
  InternalHashMap::accessor acc;
  if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
    throw std::runtime_error("This lock must be already held in SHARED mode");
  }

  // Try to upgrade using WaitDieLock
  bool upgraded = acc->second.TryLockUpgrade(txn_ts);
  if (upgraded) { it->second = LockType::EXCLUSIVE; }
  return upgraded;
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
    throw std::runtime_error("Lock object missing in internal map");
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
    throw std::runtime_error("Lock object missing in internal map");
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