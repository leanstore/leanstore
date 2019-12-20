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
  u64 t_id = 9999; // TODO;
  u64 hot_hit_counter = 0; // TODO: give it a try ?
  u64 cold_hit_counter = 0;
  u64 read_operations_counter = 0;
  u64 allocate_operations_counter = 0;
  u64 restarts_counter = 0;
  u64 tx = 0;
  // -------------------------------------------------------------------------------------
  u64 dt_misses_counter[20] = {0};
  u64 dt_restarts_modify[20] = {0};
  u64 dt_restarts_read[20] = {0};
  // -------------------------------------------------------------------------------------
  WorkerCounters() { t_id = workers_counter++; }
  // -------------------------------------------------------------------------------------
  static atomic<u64> workers_counter;
  static tbb::enumerable_thread_specific<WorkerCounters> worker_counters;
  static tbb::enumerable_thread_specific<WorkerCounters>::reference myCounters() { return worker_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
