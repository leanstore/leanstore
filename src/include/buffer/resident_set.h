#pragma once

#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/page_state.h"

#include <atomic>
#include <functional>
#include <shared_mutex>

namespace leanstore::buffer {

class ResidentPageSet {
 public:
  static constexpr u64 EMPTY     = ~0ULL;
  static constexpr u64 TOMBSTONE = (~0ULL) - 1;

  explicit ResidentPageSet(u64 max_count);
  ~ResidentPageSet();

  auto Capacity() -> u64;
  auto Contain(pageid_t page_id) -> bool;
  void Insert(pageid_t page_id);
  auto Remove(pageid_t page_id) -> bool;
  void IterateClockBatch(u64 batch, const std::function<void(pageid_t)> &evict_fn);

 private:
  class Entry {
   public:
    std::atomic<pageid_t> pid;
  };

  const u64 capacity_;
  const u64 count_;
  const u64 mask_;
  std::atomic<u64> clock_pos_;
  Entry *slots_;
};

}  // namespace leanstore::buffer