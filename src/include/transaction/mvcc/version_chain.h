#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>

namespace leanstore::transaction::mvcc {

struct TupleVersion {
  std::atomic<TupleVersion *> next;
  TupleVersion *prev;  // prev is safe because Push() is exclusive
  const timestamp_t ts;
  const u64 size;
  const u8 *payload;

  /**
   * @brief Construct a new Version in the tuple's version chain
   *
   * If size == 0, this version was deleted.
   * Otherwise, full content of prev version
   */
  TupleVersion(TupleVersion *prev, timestamp_t e, const u8 *data, u64 sz) : next(nullptr), prev(prev), ts(e), size(sz) {
    if (sz > 0) {
      u8 *tmp = new u8[sz];   // allocate mutable memory
      memcpy(tmp, data, sz);  // copy data
      payload = tmp;          // assign to const pointer
    } else {
      payload = nullptr;
    }
  }

  ~TupleVersion() {
    if (size > 0) { delete[] payload; }
  }

  // non-copyable / non-movable to avoid double-free
  TupleVersion(const TupleVersion &)            = delete;
  TupleVersion &operator=(const TupleVersion &) = delete;
  TupleVersion(TupleVersion &&)                 = delete;
  TupleVersion &operator=(TupleVersion &&)      = delete;
};

class VersionChain {
 public:
  VersionChain() : sentinel_(new TupleVersion(nullptr, 0, nullptr, 0)), head_(sentinel_), tail_(sentinel_) {}

  ~VersionChain() {
    TupleVersion *it;
    for (it = head_.load(); it;) {
      auto tmp = it->next.load();
      delete it;
      it = tmp;
    }
  }

  // Append new version to the version chain, requires the caller to already hold an X-lock.
  void Append(timestamp_t ts, const std::span<u8> &payload) {
    auto old_tail = tail_.load();
    assert(ts >= old_tail->ts);
    auto node = new TupleVersion(old_tail, ts, payload.data(), payload.size());

    old_tail->next.store(node);
    tail_ = node;
  }

  // Iterate from tail backward to find first node with epoch >= current_epoch
  auto FindCorrectVersion(timestamp_t ts, timestamp_t &out_tuple_ts) -> std::span<const u8> {
    TupleVersion *it;
    for (it = tail_.load(); it;) {
      auto tmp = it->prev;
      if (tmp->ts >= ts) { it = tmp; }
    }

    out_tuple_ts = it->ts;
    return {it->payload, it->size};
  }

  // Sweep nodes with epoch < sweep_ts
  void Sweep(timestamp_t sweep_ts) {
    TupleVersion *it;
    for (it = sentinel_->next.load(); it && (it->ts < sweep_ts);) {
      auto tmp        = it->next.load();
      sentinel_->next = tmp;
      it              = tmp;
    }
  }

 private:
  TupleVersion *sentinel_;  // sentinel_ is always the 1st node of the chain, i.e., head_ == sentinel_ always hold true
  std::atomic<TupleVersion *> head_;
  std::atomic<TupleVersion *> tail_;
};

}  // namespace leanstore::transaction::mvcc
