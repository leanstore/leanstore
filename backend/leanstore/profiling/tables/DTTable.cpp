#include "DTTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
DTTable::DTTable(BufferManager& bm) : bm(bm) {}
// -------------------------------------------------------------------------------------
std::string DTTable::getName()
{
   return "dt";
}
// -------------------------------------------------------------------------------------
void DTTable::open()
{
   columns.emplace("key", [&](Column& col) { col << dt_id; });
   columns.emplace("dt_name", [&](Column& col) { col << dt_name; });
   columns.emplace("dt_misses_counter", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_misses_counter, dt_id); });
   columns.emplace("dt_restarts_update_same_size",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_update_same_size, dt_id); });
   columns.emplace("dt_restarts_structural_change",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_structural_change, dt_id); });
   columns.emplace("dt_restarts_read", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_read, dt_id); });
   columns.emplace("contention_split_succ_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_succ_counter, dt_id); });
   columns.emplace("contention_split_fail_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_fail_counter, dt_id); });
   columns.emplace("cm_merge_succ_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_merge_succ_counter, dt_id); });
   columns.emplace("cm_merge_fail_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_merge_fail_counter, dt_id); });
   columns.emplace("xmerge_partial_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_partial_counter, dt_id); });
   columns.emplace("xmerge_full_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_full_counter, dt_id); });
   for (u64 i = 1; i < WorkerCounters::VW_MAX_STEPS; i++) {
      columns.emplace("vw_version_step_" + std::to_string(i),
                      [&, i](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::vw_version_step, dt_id, i); });
   }
}
// -------------------------------------------------------------------------------------
void DTTable::next()
{
   clear();
   for (const auto& dt : bm.getDTRegistry().dt_instances_ht) {
      dt_id = dt.first;
      dt_name = std::get<2>(dt.second);
      for (auto& c : columns) {
         c.second.generator(c.second);
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
