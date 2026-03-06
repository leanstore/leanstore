#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <cstdint>
#include <span>

namespace leanstore::transaction::mvcc {

struct TupleVersion {
  std::atomic<TupleVersion *> prev;
  const timestamp_t ts;
  const u64 size;
  u8 payload[];  // Flexible array member — payload bytes live inline after the struct
                 // If size == 0, this version is a sentinel.

  // Factory: allocates sizeof(TupleVersion) + sz bytes in one shot.
  // Must be freed with TupleVersion::Destroy(), never with plain delete.
  static TupleVersion *Create(TupleVersion *prev, timestamp_t ts, const u8 *data, u64 sz);
  static void Destroy(TupleVersion *v);

  // non-copyable / non-movable
  TupleVersion(const TupleVersion &)            = delete;
  TupleVersion &operator=(const TupleVersion &) = delete;
  TupleVersion(TupleVersion &&)                 = delete;
  TupleVersion &operator=(TupleVersion &&)      = delete;

 private:
  // Private: callers must go through Create().
  TupleVersion(TupleVersion *prev, timestamp_t ts, const u8 *data, u64 sz);
  ~TupleVersion() = default;
};

class VersionChain {
 public:
  VersionChain();
  ~VersionChain();

  // Append new version to the version chain, requires the caller to already hold an X-lock.
  void Append(timestamp_t ts, const std::span<u8> &payload);

  // Iterate from tail backward to find first node with ts <= read_ts.
  auto FindCorrectVersion(timestamp_t read_ts, timestamp_t &out_tuple_ts) -> std::span<const u8>;

  // Sweep nodes with ts < sweep_ts
  void Sweep(timestamp_t sweep_ts);

 private:
  const TupleVersion *sentinel_;      // sentinel_ is always the oldest node of the chain
  std::atomic<TupleVersion *> tail_;  // N2O (new-to-old), i.e., timestamp in desc ordering, version chain
};

}  // namespace leanstore::transaction::mvcc
