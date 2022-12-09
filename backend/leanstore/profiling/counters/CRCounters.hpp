#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>

// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct CRCounters {
   atomic<s64> worker_id = -1;
   atomic<u64> written_log_bytes = 0;
   atomic<u64> wal_reserve_blocked = 0;
   atomic<u64> wal_reserve_immediate = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> gct_total_ms = 0;
   atomic<u64> gct_phase_1_ms = 0;
   atomic<u64> gct_phase_2_ms = 0;
   atomic<u64> gct_write_ms = 0;
   atomic<u64> gct_write_bytes = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> gct_rounds = 0;
   atomic<u64> gct_committed_tx = 0;
   atomic<u64> rfa_committed_tx = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> cc_prepare_igc = 0;
   atomic<u64> cc_cross_workers_visibility_check = 0;
   atomic<u64> cc_versions_space_removed = {0};
   atomic<u64> cc_snapshot_restart = 0;
   // -------------------------------------------------------------------------------------
   // Time
   atomic<u64> cc_ms_snapshotting = 0; // Everything related to commit log
   atomic<u64> cc_ms_gc = 0;
   atomic<u64> cc_ms_gc_graveyard = 0;
   atomic<u64> cc_ms_gc_history_tree = 0;
   atomic<u64> cc_ms_gc_cm = 0;
   atomic<u64> cc_ms_committing = 0;
   atomic<u64> cc_ms_history_tree_insert = 0;
   atomic<u64> cc_ms_history_tree_retrieve = 0;
   atomic<u64> cc_ms_refresh_global_state = 0;

   atomic<u64> cc_ms_oltp_tx = 0;
   atomic<u64> cc_ms_olap_tx = 0;
   atomic<u64> cc_ms_fat_tuple = 0;
   atomic<u64> cc_ms_fat_tuple_conversion = 0;

   atomic<u64> cc_ms_start_tx = 0;
   atomic<u64> cc_ms_commit_tx = 0;
   atomic<u64> cc_ms_abort_tx = 0;
   // -------------------------------------------------------------------------------------
   // Latency
   static constexpr u64 latency_tx_capacity = 1024;  // ATTENTION: buffer overflow if more than max_dt_id in system are registered
   atomic<u64> cc_ms_precommit_latency[latency_tx_capacity] = {0};
   atomic<u64> cc_ms_commit_latency[latency_tx_capacity] = {0};
   atomic<u64> cc_flushes_counter[latency_tx_capacity] = {0};
   atomic<u64> cc_latency_cursor = {0};
   atomic<u64> cc_rfa_ms_precommit_latency[latency_tx_capacity] = {0};
   atomic<u64> cc_rfa_ms_commit_latency[latency_tx_capacity] = {0};
   atomic<u64> cc_rfa_latency_cursor = {0};
   // -------------------------------------------------------------------------------------
   CRCounters() {}
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<CRCounters> cr_counters;
   static tbb::enumerable_thread_specific<CRCounters>::reference myCounters() { return cr_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
