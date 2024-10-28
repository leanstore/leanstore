#include "ResultsTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum_reset;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
std::string ResultsTable::getName()
{
   return "results";
}
// -------------------------------------------------------------------------------------
void ResultsTable::open()
{
   columns.emplace("total_newPages", [&](Column& col) {col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::new_pages_counter); });
   columns.emplace("total_misses", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::missed_hit_counter); });
   columns.emplace("total_hits", [&](Column& col) {
      col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::hot_hit_counter); });
   columns.emplace("total_jumps", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::jumps); });
   columns.emplace("total_evictions", [&](Column& col) { col << sum_reset(PPCounters::pp_counters, &PPCounters::total_evictions); });
   columns.emplace("total_touches", [&](Column& col) { col << sum_reset(PPCounters::pp_counters, &PPCounters::total_touches); });
   columns.emplace("total_writes", [&](Column& col) { col << sum_reset(PPCounters::pp_counters, &PPCounters::total_writes); });
   columns.emplace("total_transactions", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::tx_counter); });
   columns.emplace("total_time", [&](Column& col) { col << total_seconds; });
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
void ResultsTable::next()
{
   clear();
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
