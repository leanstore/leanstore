#include "transaction/svcc/lock_manager.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

namespace leanstore::transaction::svcc {

thread_local LockManager::LocalReadSet LockManager::read_set_;
thread_local LockManager::LocalWriteSet LockManager::write_set_;

void LockManager::ReleaseAllLocks(timestamp_t txn_ts, const WriteSetCallback &write_set_cb) {
  WaitDieLock *lock;

  std::erase_if(LockManager::read_set_, [&](const auto &tuple) {
    // Lookup the WaitDieLock in the internal map
    {
      InternalHashMap::const_accessor acc;
      if (!internal_.find(acc, const_cast<LockableTuple *>(tuple))) {
        throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
      }
      lock = acc->second;
    }

    // Release the lock
    lock->UnlockShared(txn_ts);
    return true;
  });

  std::erase_if(LockManager::write_set_, [&](const auto &kv) {
    const auto &[tuple, payload] = kv;

    // Lookup the WaitDieLock in the internal map
    {
      InternalHashMap::const_accessor acc;
      if (!internal_.find(acc, const_cast<LockableTuple *>(tuple))) {
        throw std::runtime_error("ReleaseAllLocks: Lock object missing in internal map");
      }
      lock = acc->second;
    }

    // Release the lock. SVCC doesn't need tuple timestamp for both commit/abort
    write_set_cb(tuple, INVALID_TS, std::span(payload));
    lock->Unlock(txn_ts);
    return true;
  });
}

bool LockManager::TryLockShared(u64 txn_ts, const LockableTuple *key) {
  // Already hold an lock on the tuple, return
  if (LockManager::read_set_.contains(key) || LockManager::write_set_.contains(key)) { return true; }
  // Haven't locked the tuple before, insert a new WaitDieLock in the internal map
  auto lock = GetOrInsert(key);
  // trying to lock it in shared mode
  bool granted = lock->TryLockShared(txn_ts);
  if (granted) { read_set_.emplace(key); }
  return granted;
}

bool LockManager::TryLock(u64 txn_ts, std::span<u8> undo_payload, const LockableTuple *key) {
  WaitDieLock *lock = nullptr;

  // If we already hold exclusive lock, return
  if (write_set_.contains(key)) { return true; }

  // If already hold a shared lock on this tuple, upgrade
  auto it = read_set_.find(key);
  if (it != read_set_.end()) {
    {
      InternalHashMap::const_accessor acc;
      if (!internal_.find(acc, const_cast<LockableTuple *>(key))) {
        throw std::runtime_error("TryLock: This lock must be already held in SHARED mode");
      }
      lock = acc->second;
    }

    // Try to upgrade using WaitDieLock
    bool upgraded = lock->TryLockUpgrade(txn_ts);
    if (upgraded) {
      read_set_.erase(it);
      write_set_.emplace(key, std::vector<u8>(undo_payload.begin(), undo_payload.end()));
    }
    return upgraded;
  }

  // Otherwise, insert WaitDieLock to the internal map
  lock = GetOrInsert(key);
  // Try to acquire exclusive lock
  bool granted = lock->TryLock(txn_ts);
  // If granted, track it in thread-local map
  if (granted) { write_set_.emplace(key, std::vector<u8>(undo_payload.begin(), undo_payload.end())); }
  return granted;
}

void LockManager::Unlock(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold the lock
  auto it = write_set_.find(key);
  if (it == write_set_.end()) { throw std::runtime_error("Unlock called without holding an exclusive lock"); }

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
  write_set_.erase(it);
}

void LockManager::UnlockShared(u64 txn_ts, const LockableTuple *key) {
  // Check if we currently hold a shared lock
  auto it = read_set_.find(key);
  if (it == read_set_.end()) { throw std::runtime_error("UnlockShared called without holding a shared lock"); }

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
  read_set_.erase(it);
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