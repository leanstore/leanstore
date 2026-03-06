#include "transaction/mvcc/version_manager.h"
#include "leanstore/config.h"

#include "fmt/format.h"

namespace leanstore::transaction::mvcc {

VersionManager::VersionManager() : local_timestamp_(FLAGS_worker_count) {}

auto VersionManager::ReadValidVersion(timestamp_t ts, const LockableTuple *key, const AccessPayloadFunc &read_cb,
                                      timestamp_t &out_tuple_ts) -> bool {
  VersionChain *chain;
  {
    VersionHashMap::accessor acc;
    if (!version_.find(acc, const_cast<LockableTuple *>(key))) {
      throw std::runtime_error("ReadValidVersion: Lock object missing in internal map");
    }
    chain = acc->second;
  }
  auto payload = chain->FindCorrectVersion(ts, out_tuple_ts);
  if (payload.empty()) { return false; }
  read_cb(payload);
  return true;
}

void VersionManager::AppendVersion(timestamp_t ts, const LockableTuple *key, const std::span<u8> &payload) {
  auto [_, version_chain] = GetOrInsert(key);
  version_chain->Append(ts, payload);
}

void VersionManager::AdvanceLocalTimestamp(wid_t w_id, timestamp_t ts) {
  assert(ts >= local_timestamp_[w_id]);
  local_timestamp_[w_id] = ts;
}

void VersionManager::Sweep() {
  auto min_ts = local_timestamp_.Min();
  for (auto it = version_.begin(); it != version_.end(); ++it) {
    VersionChain *chain = nullptr;
    {
      VersionHashMap::accessor acc;
      if (version_.find(acc, it->first)) { chain = acc->second; }
    }
    chain->Sweep(min_ts);
  }
}

auto VersionManager::GetOrInsert(const LockableTuple *key) -> std::pair<LockableTuple *, VersionChain *> {
  VersionHashMap::accessor acc;
  auto new_key = LockableTuple::Constructor(*key);  // allocate new key on the heap as tbb::hash will use the ptr as key
  fmt::println("VersionManager -- Insert to internal: {}", fmt::ptr(new_key));
  auto success = version_.insert(acc, new_key);
  if (success) {
    acc->second = new VersionChain();
  } else {
    LockableTuple::Release(new_key);
  }
  return std::make_pair(acc->first, acc->second);
}

}  // namespace leanstore::transaction::mvcc