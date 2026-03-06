#pragma once

#include "common/atomic_array.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "transaction/lockable_tuple.h"
#include "transaction/mvcc/version_chain.h"

#include "tbb/concurrent_hash_map.h"

#include <atomic>
#include <unordered_set>

namespace leanstore::transaction::mvcc {

/**
 * @brief In-memory Hyper-style version chain manager for MVCC.
 *
 * Manages version chain of tuples in memory, providing a lock-free, high-performance mechanism to
 * store and access historical versions.
 * Historical versions are duplicated from the log to enable faster and simpler access,
 * compared to reading log records directly, which can be particularly complicated for large write transactions.
 *
 * Key responsibilities:
 *  - Maintaining per-tuple version chains in `VersionHashMap`.
 *  - Providing transactional reads via `ReadValidVersion()`, invoking a
 *    callback on the most recent version visible to a given timestamp.
 *  - Appending new versions using `AppendVersion()`.
 *  - Reclaiming memory by cleaning up old versions in `Sweep()`.
 *  - Tracking per-thread local timestamps in `local_timestamp_`.
 *
 * TODO(XXX): Implement a memory reclaimation for the VersionHashMap.
 */
class VersionManager {
 public:
  using VersionHashMap = tbb::concurrent_hash_map<LockableTuple *, VersionChain *, LockableTuple::HashTBB>;

  VersionManager();
  ~VersionManager() = default;

  auto ReadValidVersion(timestamp_t ts, const LockableTuple *key, const AccessPayloadFunc &read_cb,
                        timestamp_t &out_tuple_ts) -> bool;
  void AppendVersion(timestamp_t ts, const LockableTuple *key, const std::span<u8> &payload);
  void AdvanceLocalTimestamp(wid_t w_id, timestamp_t ts);
  void Sweep();

 private:
  auto GetOrInsert(const LockableTuple *key) -> std::pair<LockableTuple *, VersionChain *>;

  VersionHashMap version_;
  AtomicArray<timestamp_t> local_timestamp_;
};

}  // namespace leanstore::transaction::mvcc
