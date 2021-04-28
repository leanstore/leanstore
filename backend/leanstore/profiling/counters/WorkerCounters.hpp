#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>

#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct WorkerCounters {
   static constexpr u64 max_researchy_counter = 10;
   static constexpr u64 max_dt_id = 20;
   // -------------------------------------------------------------------------------------
   atomic<u64> t_id = 9999;                // used by tpcc
   atomic<u64> variable_for_workload = 0;  // Used by tpcc
                                           // -------------------------------------------------------------------------------------
   atomic<u64> worker_id = -1;
   // -------------------------------------------------------------------------------------
   atomic<u64> hot_hit_counter = 0;  // TODO: give it a try ?
   atomic<u64> cold_hit_counter = 0;
   atomic<u64> read_operations_counter = 0;
   atomic<u64> allocate_operations_counter = 0;
   atomic<u64> restarts_counter = 0;
   atomic<u64> tx = 0;
   atomic<u64> tx_abort = 0;
   atomic<u64> tmp = 0;
   // -------------------------------------------------------------------------------------
   // Space and contention management
   atomic<u64> contention_split_succ_counter[max_dt_id] = {0};
   atomic<u64> contention_split_fail_counter[max_dt_id] = {0};
   atomic<u64> dt_merge_succ[max_dt_id] = {0};
   atomic<u64> dt_merge_parent_succ[max_dt_id] = {0};
   atomic<u64> dt_merge_fail[max_dt_id] = {0};
   atomic<u64> dt_merge_parent_fail[max_dt_id] = {0};
   atomic<u64> xmerge_partial_counter[max_dt_id] = {0};
   atomic<u64> xmerge_full_counter[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   atomic<u64> dt_misses_counter[max_dt_id] = {0};
   atomic<u64> dt_restarts_update_same_size[max_dt_id] = {0};   // without structural change
   atomic<u64> dt_restarts_structural_change[max_dt_id] = {0};  // includes insert, remove, update with different size
   atomic<u64> dt_restarts_read[max_dt_id] = {0};
   atomic<u64> dt_researchy[max_dt_id][max_researchy_counter] = {};  // temporary counter used to track some value for an idea in my mind
   // -------------------------------------------------------------------------------------
   atomic<u64> dt_skipped_leaf[max_dt_id] = {0};
   atomic<u64> dt_empty_leaf[max_dt_id] = {0};
   atomic<u64> dt_goto_page[max_dt_id] = {0};
   atomic<u64> dt_next_tuple[max_dt_id] = {0};
   atomic<u64> dt_prev_tuple[max_dt_id] = {0};
   atomic<u64> dt_inner_page[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   // Concurrency control counters
   atomic<u64> cc_snapshot_restart = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_read_versions_visited[max_dt_id] = {0};
   atomic<u64> cc_read_versions_visited_not_found[max_dt_id] = {0};
   atomic<u64> cc_read_chains_not_found[max_dt_id] = {0};
   atomic<u64> cc_read_chains[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_update_versions_visited[max_dt_id] = {0};
   atomic<u64> cc_update_versions_removed[max_dt_id] = {0};
   atomic<u64> cc_update_versions_kept[max_dt_id] = {0};
   atomic<u64> cc_update_versions_kept_max[max_dt_id] = {0};
   atomic<u64> cc_update_versions_skipped[max_dt_id] = {0};
   atomic<u64> cc_update_versions_recycled[max_dt_id] = {0};
   atomic<u64> cc_update_versions_created[max_dt_id] = {0};
   atomic<u64> cc_update_chains[max_dt_id] = {0};
   atomic<u64> cc_update_chains_hwm[max_dt_id] = {0};
   atomic<u64> cc_update_chains_pgc[max_dt_id] = {0};
   atomic<u64> cc_update_chains_pgc_skipped[max_dt_id] = {0};
   atomic<u64> cc_update_chains_pgc_workers_visited[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_todo_chains[max_dt_id] = {0};
   atomic<u64> cc_todo_remove[max_dt_id] = {0};
   atomic<u64> cc_todo_updates[max_dt_id] = {0};
   atomic<u64> cc_todo_updates_versions_removed[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_rtodo_opt_staged[max_dt_id] = {0};
   atomic<u64> cc_rtodo_opt_considered[max_dt_id] = {0};
   atomic<u64> cc_rtodo_opt_executed[max_dt_id] = {0};
   atomic<u64> cc_rtodo_shrt_executed[max_dt_id] = {0};
   atomic<u64> cc_rtodo_lng_executed[max_dt_id] = {0};
   atomic<u64> cc_rtodo_to_lng[max_dt_id] = {0};
   // -------------------------------------------------------------------------------------
   constexpr static u64 VW_MAX_STEPS = 10;
   atomic<u64> vw_version_step[max_dt_id][VW_MAX_STEPS] = {{0}};
   // -------------------------------------------------------------------------------------
   // WAL
   atomic<u64> wal_read_bytes = 0;
   atomic<u64> wal_buffer_hit = 0;
   atomic<u64> wal_buffer_miss = 0;
   // -------------------------------------------------------------------------------------
   WorkerCounters() { t_id = workers_counter++; }
   // -------------------------------------------------------------------------------------
   static atomic<u64> workers_counter;
   static tbb::enumerable_thread_specific<WorkerCounters> worker_counters;
   static tbb::enumerable_thread_specific<WorkerCounters>::reference myCounters() { return worker_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
