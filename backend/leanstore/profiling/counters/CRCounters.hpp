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
   atomic<u64> cc_versions_space_removed = 0;
   atomic<u64> cc_versions_space_inserted = 0;
   atomic<u64> cc_versions_space_inserted_opt = 0;
   // -------------------------------------------------------------------------------------
   CRCounters() {}
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<CRCounters> cr_counters;
   static tbb::enumerable_thread_specific<CRCounters>::reference myCounters() { return cr_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
