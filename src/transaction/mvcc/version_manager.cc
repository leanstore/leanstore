#include "transaction/mvcc/version_manager.h"
#include "leanstore/config.h"

namespace leanstore::transaction::mvcc {

VersionManager::VersionManager() : local_timestamp_(FLAGS_worker_count) {}

void VersionManager::ReadValidVersion(timestamp_t ts, const LockableTuple *key, const AccessPayloadFunc &read_cb) {
  std::span<const u8> payload;
  {
    VersionHashMap::accessor acc;
    if (!version_.find(acc, const_cast<LockableTuple *>(key))) {
      throw std::runtime_error("ReadValidVersion: Lock object missing in internal map");
    }
    payload = acc->second.FindCorrectVersion(ts);
  }
  read_cb(payload);
}

void VersionManager::AppendVersion(timestamp_t ts, const LockableTuple *key, const std::span<u8> &payload) {
  VersionHashMap::accessor acc;
  if (!version_.find(acc, const_cast<LockableTuple *>(key))) {
    throw std::runtime_error("AppendVersion: Lock object missing in internal map");
  }
  acc->second.Append(ts, payload);
}

void VersionManager::AdvanceLocalTimestamp(wid_t w_id, timestamp_t ts) {
  assert(ts >= local_timestamp_[w_id]);
  local_timestamp_[w_id] = ts;
}

void VersionManager::Sweep() {
  auto min_ts = local_timestamp_.Min();
  for (auto it = version_.begin(); it != version_.end(); ++it) {
    VersionHashMap::accessor acc;
    if (version_.find(acc, it->first)) { acc->second.Sweep(min_ts); }
  }
}

}  // namespace leanstore::transaction::mvcc