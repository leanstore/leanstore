#include "transaction/svcc/lock_manager.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

namespace leanstore::transaction::svcc {

thread_local LockManager::LocalReadWriteSet LockManager::rws_;

void LockManager::ReleaseAllLocks(
  timestamp_t txn_ts, [[maybe_unused]] const std::function<void(const LockableTuple *)> &update_tuple_ts_fn) {
  std::erase_if(LockManager::rws_, [&](const auto &kv) {
    const auto &[tuple, lock_type] = kv;

    // Lookup the WaitDieLock in the internal map
    WaitDieLock *lock;
    {
      InternalHashMap::const_accessor acc;
      if (!internal_.find(acc, const_cast<LockableTuple *>(tuple))) {
        throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
      }
      lock = acc->second;
    }

    // Release the lock
    if (lock_type == LockType::SHARED) {
      lock->UnlockShared(txn_ts);
    } else {
      assert(lock_type == LockType::EXCLUSIVE);
      lock->Unlock(txn_ts);
    }
    return true;
  });
}

bool LockManager::TryLockShared(u64 txn_ts, const LockableTuple *key) {
  // Already hold an lock on the tuple, return
  if (LockManager::rws_.contains(key)) { return true; }
  // Haven't locked the tuple before, insert a new WaitDieLock in the internal map
  auto lock = GetOrInsert(key);
  // trying to lock it in shared mode
  bool granted = lock->TryLockShared(txn_ts);
  if (granted) { rws_.emplace(key, LockType::SHARED); }
  return granted;
}

bool LockManager::TryLock(u64 txn_ts, [[maybe_unused]] timestamp_t tuple_ts, const LockableTuple *key) {
  WaitDieLock *lock = nullptr;

  // If already hold a lock on this tuple
  auto it = rws_.find(key);
  if (it != rws_.end()) {
    // If we already hold the lock, make sure it is exclusive
    if (it->second == LockType::EXCLUSIVE) { return true; }
    assert(it->second == LockType::SHARED);

    // Otherwise, try upgrade the lock
    {
      InternalHashMap::const_accessor acc;
      if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
        throw std::runtime_error("TryLock: This lock must be already held in SHARED mode");
      }
      lock = acc->second;
    }

    // Try to upgrade using WaitDieLock
    bool upgraded = lock->TryLockUpgrade(txn_ts);
    if (upgraded) { it->second = LockType::EXCLUSIVE; }
    return upgraded;
  }

  // Otherwise, insert WaitDieLock to the internal map
  lock = GetOrInsert(key);
  // Try to acquire exclusive lock
  bool granted = lock->TryLock(txn_ts);
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
  WaitDieLock *lock;
  {
    InternalHashMap::const_accessor acc;
    if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
      throw std::runtime_error("Unlock: Lock object missing in internal map");
    }
    lock = acc->second;
  }

  // Release the exclusive lock & Remove from thread-local map
  lock->Unlock(txn_ts);
  rws_.erase(it);
}

void LockManager::UnlockShared(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold a shared lock
  auto it = rws_.find(key);
  if (it == rws_.end() || it->second != LockType::SHARED) {
    throw std::runtime_error("UnlockShared called without holding a shared lock");
  }

  // Lookup the WaitDieLock in the internal map
  WaitDieLock *lock;
  {
    InternalHashMap::const_accessor acc;
    if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
      throw std::runtime_error("UnlockShared: Lock object missing in internal map");
    }
    lock = acc->second;
  }

  // Release the shared lock & Remove from thread-local map
  lock->UnlockShared(txn_ts);
  rws_.erase(it);
}

auto LockManager::GetOrInsert(const LockableTuple *key) -> WaitDieLock * {
  InternalHashMap::accessor acc;
  auto new_key = LockableTuple::Constructor(*key);  // allocate new key on the heap as tbb::hash will use the ptr as key
  auto success = internal_.insert(acc, new_key);
  if (success) {
    acc->second = new WaitDieLock();
  } else {
    LockableTuple::Release(new_key);
  }
  return acc->second;
}

}  // namespace leanstore::transaction::svcc