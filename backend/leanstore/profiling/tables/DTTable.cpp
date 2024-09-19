#include "DTTable.hpp"

#include "leanstore/Config.hpp"
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
   columns.emplace("dt_page_reads", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_page_reads, dt_id); });
   columns.emplace("dt_page_writes", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_page_writes, dt_id); });
   columns.emplace("dt_restarts_update_same_size",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_update_same_size, dt_id); });
   columns.emplace("dt_restarts_structural_change",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_structural_change, dt_id); });
   columns.emplace("dt_restarts_read", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_read, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("dt_empty_leaf", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_empty_leaf, dt_id); });
   columns.emplace("dt_goto_page_exec", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_goto_page_exec, dt_id); });
   columns.emplace("dt_goto_page_shared",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_goto_page_shared, dt_id); });
   columns.emplace("dt_next_tuple", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_next_tuple, dt_id); });
   columns.emplace("dt_next_tuple_opt", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_next_tuple_opt, dt_id); });
   columns.emplace("dt_prev_tuple", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_prev_tuple, dt_id); });
   columns.emplace("dt_prev_tuple_opt", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_prev_tuple_opt, dt_id); });
   columns.emplace("dt_inner_page", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_inner_page, dt_id); });
   columns.emplace("dt_scan_asc", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_scan_asc, dt_id); });
   columns.emplace("dt_scan_desc", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_scan_desc, dt_id); });
   columns.emplace("dt_scan_callback", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_scan_callback, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("dt_append", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_append, dt_id); });
   columns.emplace("dt_append_opt", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_append_opt, dt_id); });
   columns.emplace("dt_range_removed", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_range_removed, dt_id); });
   // -------------------------------------------------------------------------------------
   for (u64 r_i = 0; r_i < WorkerCounters::max_researchy_counter; r_i++) {
      columns.emplace("dt_researchy_" + std::to_string(r_i), [&, r_i](Column& col) {
         col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_researchy, dt_id, r_i);
      });
   }
   // -------------------------------------------------------------------------------------
   columns.emplace("contention_split_succ_counter",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::contention_split_succ_counter, dt_id); });
   columns.emplace("contention_split_fail_counter",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::contention_split_fail_counter, dt_id); });
   columns.emplace("dt_split", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_split, dt_id); });
   columns.emplace("dt_merge_succ", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_succ, dt_id); });
   columns.emplace("dt_merge_fail", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_fail, dt_id); });
   columns.emplace("dt_merge_parent_succ",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_succ, dt_id); });
   columns.emplace("dt_merge_parent_fail",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_merge_parent_fail, dt_id); });
   columns.emplace("xmerge_partial_counter",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::xmerge_partial_counter, dt_id); });
   columns.emplace("xmerge_full_counter",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::xmerge_full_counter, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("dt_find_parent", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_find_parent, dt_id); });
   columns.emplace("dt_find_parent_root",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_find_parent_root, dt_id); });
   columns.emplace("dt_find_parent_fast",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_find_parent_fast, dt_id); });
   columns.emplace("dt_find_parent_slow",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::dt_find_parent_slow, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_read_versions_visited",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_read_versions_visited, dt_id); });
   columns.emplace("cc_read_versions_visited_not_found",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_read_versions_visited_not_found, dt_id); });
   columns.emplace("cc_read_chains", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_read_chains, dt_id); });
   columns.emplace("cc_read_chains_not_found",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_read_chains_not_found, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_update_versions_visited",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_visited, dt_id); });
   columns.emplace("cc_update_versions_removed",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_removed, dt_id); });
   columns.emplace("cc_update_versions_skipped",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_skipped, dt_id); });
   columns.emplace("cc_update_versions_kept",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_kept, dt_id); });
   columns.emplace("cc_update_versions_kept_max", [&](Column& col) {
      col << utils::threadlocal::max_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_kept_max, dt_id);
   });
   columns.emplace("cc_update_versions_recycled",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_recycled, dt_id); });
   columns.emplace("cc_update_versions_created",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_versions_created, dt_id); });
   columns.emplace("cc_update_chains", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains, dt_id); });
   columns.emplace("cc_update_chains_hwm",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_hwm, dt_id); });
   columns.emplace("cc_update_chains_pgc",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc, dt_id); });
   columns.emplace("cc_update_chains_pgc_skipped",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_skipped, dt_id); });
   columns.emplace("cc_update_chains_pgc_workers_visited",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_workers_visited, dt_id); });
   columns.emplace("cc_update_chains_pgc_heavy_removed",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_heavy_removed, dt_id); });
   columns.emplace("cc_update_chains_pgc_heavy",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_heavy, dt_id); });
   columns.emplace("cc_update_chains_pgc_light_removed",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_light_removed, dt_id); });
   columns.emplace("cc_update_chains_pgc_light",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_update_chains_pgc_light, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_todo_removed", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_removed, dt_id); });
   columns.emplace("cc_todo_moved_gy", [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_moved_gy, dt_id); });
   columns.emplace("cc_todo_oltp_executed",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_oltp_executed, dt_id); });
   columns.emplace("cc_todo_olap_executed",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_todo_olap_executed, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_fat_tuple_triggered",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_fat_tuple_triggered, dt_id); });
   columns.emplace("cc_fat_tuple_convert",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_fat_tuple_convert, dt_id); });
   columns.emplace("cc_fat_tuple_decompose",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_fat_tuple_decompose, dt_id); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_versions_space_inserted",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_versions_space_inserted, dt_id); });
   columns.emplace("cc_versions_space_inserted_opt",
                   [&](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cc_versions_space_inserted_opt, dt_id); });
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
