#include "transaction/mvcc/version_chain.h"

#include "fmt/format.h"

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>

namespace leanstore::transaction::mvcc {

// ---------------------------------------------------------------------------
// TupleVersion
// ---------------------------------------------------------------------------

// Private constructor — only called from Create() via placement new.
TupleVersion::TupleVersion(TupleVersion *prev, timestamp_t ts, const u8 *data, u64 sz) : prev(prev), ts(ts), size(sz) {
  if (sz > 0) { memcpy(payload, data, sz); }
}

// static
TupleVersion *TupleVersion::Create(TupleVersion *prev, timestamp_t ts, const u8 *data, u64 sz) {
  // Allocate one contiguous block: struct fields + sz inline payload bytes.
  void *mem = ::operator new(sizeof(TupleVersion) + sz);
  return new (mem) TupleVersion(prev, ts, data, sz);
}

// static
void TupleVersion::Destroy(TupleVersion *v) {
  if (v == nullptr) { return; }
  fmt::println("Destroy {}", fmt::ptr(v));
  v->~TupleVersion();    // run destructor (releases atomics, etc.)
  ::operator delete(v);  // free the raw block allocated in Create()
}

// ---------------------------------------------------------------------------
// VersionChain
// ---------------------------------------------------------------------------

VersionChain::VersionChain()
    : sentinel_(TupleVersion::Create(nullptr, 0, nullptr, 0)), tail_(const_cast<TupleVersion *>(sentinel_)) {}

VersionChain::~VersionChain() { Sweep(std::numeric_limits<timestamp_t>::max()); }

auto VersionChain::Append(timestamp_t ts, const std::span<u8> &payload) -> TupleVersion * {
  auto old_tail = tail_.load();
  assert(ts >= old_tail->ts);
  auto node = TupleVersion::Create(old_tail, ts, payload.data(), payload.size());
  tail_     = node;
  return node;
}

auto VersionChain::FindCorrectVersion(timestamp_t read_ts, timestamp_t &out_tuple_ts) -> std::span<const u8> {
  TupleVersion *it;
  for (it = tail_.load(); it != sentinel_; it = it->prev.load(std::memory_order_relaxed)) {
    if (it != sentinel_ && it->ts <= read_ts) { break; }
  }

  out_tuple_ts = it->ts;
  return {it->payload, it->size};
}

void VersionChain::Sweep(timestamp_t sweep_ts) {
  auto curr = tail_.load();
  for (; (curr != sentinel_) && (curr->ts >= sweep_ts); curr = curr->prev.load(std::memory_order_relaxed)) {}
  if (curr == sentinel_) { return; }  // Always maintain at least one tail node at the end
  fmt::println("Sweeping");
  for (auto it = curr->prev.load(std::memory_order_relaxed); it != sentinel_;) {
    auto tmp = it->prev.load(std::memory_order_relaxed);
    TupleVersion::Destroy(it);
    it = tmp;
  }
  curr->prev = const_cast<TupleVersion *>(sentinel_);
}

}  // namespace leanstore::transaction::mvcc
