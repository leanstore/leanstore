#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <tbb/enumerable_thread_specific.h>

#include "PerfEvent.hpp"
#include "leanstore/utils/Hist.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
struct SSDCounters {
   static constexpr u64 max_ssds = 20;
   atomic<u64> t_id = 9999;                // used by tpcc
   // -------------------------------------------------------------------------------------
   // Space and contention management
   atomic<u64> pushed[max_ssds] = {0};
   atomic<u64> polled[max_ssds] = {0};
   atomic<s64> outstandingx_max[max_ssds] = {0};
   atomic<s64> outstandingx_min[max_ssds] = {0};
   atomic<u64> read_latncy50p[max_ssds] = {0};
   atomic<u64> read_latncy99p9[max_ssds] = {0};
   atomic<u64> read_latncy_max[max_ssds] = {0};
   atomic<u64> write_latncy50p[max_ssds] = {0};
   atomic<u64> write_latncy99p9[max_ssds] = {0};
   atomic<u64> writes[max_ssds] = {0};
   atomic<u64> reads[max_ssds] = {0};
   // -------------------------------------------------------------------------------------
   SSDCounters() { }
   // -------------------------------------------------------------------------------------
   static tbb::enumerable_thread_specific<SSDCounters> ssd_counters;
   static tbb::enumerable_thread_specific<SSDCounters>::reference myCounters() { return ssd_counters.local(); }
};
}  // namespace leanstore
// -------------------------------------------------------------------------------------
