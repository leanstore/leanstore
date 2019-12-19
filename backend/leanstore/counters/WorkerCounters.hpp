#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// Well, we might need atomics here...
struct WorkerCounters {
  u64 id; // TODO;
  u64 hot_hit_counter = 0; // TODO: give it a try ?
  u64 cold_hit_counter = 0;
  u64 read_operations_counter = 0;
  u64 allocate_operations_counter = 0;
  u64 tx = 0;
  // -------------------------------------------------------------------------------------
  std::unordered_map<u64, u64> dt_misses_counter;
  // -------------------------------------------------------------------------------------
  static tbb::enumerable_thread_specific<WorkerCounters> worker_counters;
  static tbb::enumerable_thread_specific<WorkerCounters>::reference myCounters() { return worker_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------