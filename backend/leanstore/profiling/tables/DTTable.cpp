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
   // -------------------------------------------------------------------------------------
   columns.emplace("dt_empty_leaf", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_empty_leaf, dt_id); });
   columns.emplace("dt_skipped_leaf", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_skipped_leaf, dt_id); });
   columns.emplace("dt_goto_page_exec", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_goto_page_exec, dt_id); });
   columns.emplace("dt_goto_page_shared",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_goto_page_shared, dt_id); });
   columns.emplace("dt_next_tuple", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_next_tuple, dt_id); });
   columns.emplace("dt_prev_tuple", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_prev_tuple, dt_id); });
   columns.emplace("dt_inner_page", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_inner_page, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("contention_split_succ_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_succ_counter, dt_id); });
   columns.emplace("contention_split_fail_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::contention_split_fail_counter, dt_id); });
   columns.emplace("dt_merge_succ", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_succ, dt_id); });
   columns.emplace("dt_merge_fail", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_fail, dt_id); });
   columns.emplace("dt_merge_parent_succ",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_succ, dt_id); });
   columns.emplace("dt_merge_parent_fail",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_fail, dt_id); });
   columns.emplace("xmerge_partial_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_partial_counter, dt_id); });
   columns.emplace("xmerge_full_counter",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::xmerge_full_counter, dt_id); });
   for (u64 i = 1; i < WorkerCounters::VW_MAX_STEPS; i++) {
      columns.emplace("vw_version_step_" + std::to_string(i),
                      [&, i](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::vw_version_step, dt_id, i); });
   }
   // -------------------------------------------------------------------------------------

   columns.emplace("cc_read_versions_visited",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_read_versions_visited, dt_id); });
   columns.emplace("cc_read_versions_visited_not_found",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_read_versions_visited_not_found, dt_id); });
   columns.emplace("cc_read_chains", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_read_chains, dt_id); });
   columns.emplace("cc_read_chains_not_found",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_read_chains_not_found, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_update_versions_visited",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_visited, dt_id); });
   columns.emplace("cc_update_versions_removed",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_removed, dt_id); });
   columns.emplace("cc_update_versions_skipped",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_skipped, dt_id); });
   columns.emplace("cc_update_versions_kept",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_kept, dt_id); });
   columns.emplace("cc_update_versions_kept_max", [&](Column& col) {
      col << utils::threadlocal::max(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_kept_max, dt_id);
   });
   columns.emplace("cc_update_versions_recycled",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_recycled, dt_id); });
   columns.emplace("cc_update_versions_created",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_created, dt_id); });
   columns.emplace("cc_update_chains", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains, dt_id); });
   columns.emplace("cc_update_chains_hwm",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_hwm, dt_id); });
   columns.emplace("cc_update_chains_pgc",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc, dt_id); });
   columns.emplace("cc_update_chains_pgc_skipped",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_skipped, dt_id); });
   columns.emplace("cc_update_chains_pgc_workers_visited",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_workers_visited, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_todo_removed", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_removed, dt_id); });
   columns.emplace("cc_todo_moved_gy", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_moved_gy, dt_id); });
   columns.emplace("cc_todo_oltp_executed", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_oltp_executed, dt_id); });
   columns.emplace("cc_todo_olap_executed", [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_olap_executed, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_fat_tuple_convert",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_fat_tuple_convert, dt_id); });
   columns.emplace("cc_fat_tuple_decompose",
                   [&](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::cc_fat_tuple_decompose, dt_id); });
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
