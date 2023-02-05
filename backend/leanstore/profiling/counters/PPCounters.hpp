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
struct PPCounters {
   // ATTENTION: These counters should be only used by page provider threads or slow path worker code
   atomic<s64> phase_1_ms = 0, phase_2_ms = 0, phase_3_ms = 0, poll_ms = 0;
   // Phase 1 detailed
   atomic<u64> find_parent_ms = 0, iterate_children_ms = 0;
   // Phase 3 detailed
   atomic<u64> async_wb_ms = 0, submit_ms = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> phase_1_counter = 0, phase_2_counter = 0, phase_3_counter = 0;
   atomic<u64> phase_2_added = 0;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   atomic<u64> evicted_pages = 0, pp_thread_rounds = 0;
   atomic<u64> failed_bf_pick_attempts  = 0;
   atomic<u64> failed_bf_pick_attempts_cause_dbg  = 0;
   atomic<u64> submitted = 0;
   atomic<u64> submit_cnt = 0;
   atomic<u64> pp_qlen = 0;
   atomic<u64> pp_pages_iterate = 0;
   atomic<u64> pp_qlen_cnt = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> touched_bfs_counter = 0;
   atomic<u64> flushed_pages_counter = 0;
   atomic<u64> unswizzled_pages_counter = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> inner_evicted = 0;
   atomic<u64> leaf_evicted = 0;
   // -------------------------------------------------------------------------------------
   atomic<u64> outstandinig_50p = 0;
   atomic<u64> outstandinig_99p9 = 0;
   atomic<u64> outstandinig_read = 0;
   atomic<u64> outstandinig_write = 0;
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<PPCounters> pp_counters;
   static tbb::enumerable_thread_specific<PPCounters>::reference myCounters() { return pp_counters.local(); }
};
}  // namespace leanstore
