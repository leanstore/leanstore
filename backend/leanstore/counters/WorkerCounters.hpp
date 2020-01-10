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
  atomic<u64> t_id = 9999; // TODO;
  atomic<u64> hot_hit_counter = 0; // TODO: give it a try ?
  atomic<u64> cold_hit_counter = 0;
  atomic<u64> read_operations_counter = 0;
  atomic<u64> allocate_operations_counter = 0;
  atomic<u64> restarts_counter = 0;
  atomic<u64> tx = 0;
  // -------------------------------------------------------------------------------------
  atomic<u64> dt_misses_counter[20] = {0};
  atomic<u64> dt_restarts_update_same_size[20] = {0}; // without structural change
  atomic<u64> dt_restarts_structural_change[20] = {0}; // includes insert, remove, update with different size
  atomic<u64> dt_restarts_read[20] = {0};
  atomic<u64> dt_researchy_0[20] = {0}; // temporary counter used to track some value for an idea in my mind
  atomic<u64> dt_researchy_1[20] = {0}; // temporary counter used to track some value for an idea in my mind
  atomic<u64> dt_researchy_2[20] = {0}; // temporary counter used to track some value for an idea in my mind
  // -------------------------------------------------------------------------------------
  WorkerCounters() { t_id = workers_counter++; }
  // -------------------------------------------------------------------------------------
  static atomic<u64> workers_counter;
  static tbb::enumerable_thread_specific<WorkerCounters> worker_counters;
  static tbb::enumerable_thread_specific<WorkerCounters>::reference myCounters() { return worker_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
