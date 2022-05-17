#include "ResultsTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
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
   columns.emplace("total_newPages", [&](Column& col) {
      col << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::new_pages_counter);
   });
   columns.emplace("total_misses", [&](Column& col) { col << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::missed_hit_counter); });
   columns.emplace("total_hits", [&](Column& col) { col << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::hot_hit_counter) +
                                                               utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cold_hit_counter); });
   columns.emplace("total_jumps", [&](Column& col) { col << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::jumps); });
   // -------------------------------------------------------------------------------------
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
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
