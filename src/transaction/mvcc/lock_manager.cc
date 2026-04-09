#include "transaction/mvcc/lock_manager.h"
#include "common/exceptions.h"

namespace leanstore::transaction::mvcc {

thread_local LockManager::LocalReadSet LockManager::read_set_;
thread_local LockManager::LocalWriteSet LockManager::write_set_;

LockManager::LockManager(VersionManager *ver_) : version_manager_(ver_) {}

bool LockManager::EmptyLocalSet() { return read_set_.empty() && write_set_.empty(); }

/* IMPORTANT: `tuple_ts <= transaction's start ts` must always hold; Otherwise, conflict with serializable */
void LockManager::SetTupleTimestamp(const LockableTuple *key, timestamp_t tuple_ts, bool require_serializable) {
  auto it = read_set_.find(key);
  Ensure(it != read_set_.end());
  if (it->second == INVALID_TS) {
    it->second = tuple_ts;
  } else if (require_serializable && (it->second != tuple_ts)) {
    throw ex::AbortTransaction();
  }
}

auto LockManager::GetTupleTimestamp(const LockableTuple *key) -> timestamp_t {
  auto it = read_set_.find(key);
  Ensure(it != read_set_.end());
  return it->second;
}

void LockManager::ReleaseAllLocks([[maybe_unused]] timestamp_t txn_ts, const WriteSetCallback &write_set_cb) {
  std::erase_if(LockManager::write_set_, [&](const auto &datum) {
    auto &[tuple, version] = datum;
    write_set_cb(tuple, version->ts, {version->payload, version->size});
    // Do not remove the version from the version chain for correctness & simplicity
    // The GC will be responsible for cleaning the version chains later
    // Also, it's fine to have version->ts == INVALID_TS here; it means the key does not exist
    return true;  // remove everything
  });
  // remove everything from read_set
  LockManager::read_set_.clear();
}

void LockManager::ValidateReadSet(const std::function<void(const LockableTuple *, timestamp_t)> &validate_fn) {
  std::erase_if(LockManager::read_set_, [&](const auto &tuple) {
    validate_fn(tuple.first, tuple.second);
    return true;  // remove everything
  });
}

auto LockManager::OwnTuple(const LockableTuple *key) -> bool { return write_set_.contains(key); }

// For MVCC:
// - we never acquire shared lock on a tuple.
// - After calling this fn, we always call SetTupleTimestamp()
bool LockManager::TryLockShared([[maybe_unused]] timestamp_t txn_ts, const LockableTuple *key) {
  // The correct timestamp will be updated later using SetTupleTimestamp()
  // It's fine for the below to fail
  auto new_key      = LockableTuple::Constructor(*key);
  auto [_, success] = read_set_.insert({new_key, INVALID_TS});
  if (!success) { LockableTuple::Release(new_key); }
  // Always success for MVCC
  return true;
}

bool LockManager::TryLock([[maybe_unused]] timestamp_t txn_ts, timestamp_t undo_ts, std::span<u8> undo_payload,
                          const LockableTuple *key) {
  // If already hold an exclusive lock on this tuple, return true immediately
  if (write_set_.contains(key)) { return true; }
  // Lookup or insert into the internal map to acquire X-lock
  if (undo_ts == INVALID_TS) { return false; }
  // Only append undo version upon acquiring successfully
  auto [real_key, version] = version_manager_->AppendVersion(undo_ts, key, undo_payload);
  // If we already read this key, need to double check if we are reading the latest value
  auto it = read_set_.find(real_key);
  if (it != read_set_.end()) {
    assert(it->second <= txn_ts);
    read_set_.erase(it);  // This txn reads the latest version, move this tuple to write set
  }
  // Acquire successfully
  write_set_.emplace(real_key, version);
  return true;
}

void LockManager::Unlock([[maybe_unused]] timestamp_t txn_ts, const LockableTuple *key) {
  // Check if we currently hold the lock
  auto it = write_set_.find(key);
  if (it == write_set_.end()) {
    throw std::runtime_error("Unlock called without holding an exclusive lock");
    // Release the exclusive lock & Remove from thread-local map
    write_set_.erase(it);
  }
}

void LockManager::UnlockShared([[maybe_unused]] timestamp_t txn_ts, [[maybe_unused]] const LockableTuple *key) {
  (void)0;  // no-op
}

}  // namespace leanstore::transaction::mvcc