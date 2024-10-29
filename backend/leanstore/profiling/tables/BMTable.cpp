#include "BMTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum_reset;
using leanstore::utils::threadlocal::sum_reset_add_to;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
BMTable::BMTable(BufferManager& bm) : ProfilingTable(), bm(bm) {}
// -------------------------------------------------------------------------------------
std::string BMTable::getName()
{
   return "bm";
}
// -------------------------------------------------------------------------------------
void BMTable::open()
{
   columns.emplace("key", [](Column& col) { col << 0; });
   columns.emplace("space_usage_gib", [&](Column& col) {
      const double gib = bm.consumedPages() * 1.0 * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0;
      col << gib;
   });
   columns.emplace("space_usage_kib", [&](Column& col) {
      const double kib = bm.consumedPages() * 1.0 * PAGE_SIZE / 1024.0;
      col << kib;
   });
   columns.emplace("consumed_pages", [&](Column& col) { col << bm.consumedPages(); });
   columns.emplace("p1_pct", [&](Column& col) { col << (local_phase_1_ms * 100.0 / total); });
   columns.emplace("p2_pct", [&](Column& col) { col << (local_phase_2_ms * 100.0 / total); });
   columns.emplace("p3_pct", [&](Column& col) { col << (local_phase_3_ms * 100.0 / total); });
   columns.emplace("poll_pct", [&](Column& col) { col << ((local_poll_ms * 100.0 / total)); });
   columns.emplace("find_parent_pct", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
   columns.emplace("iterate_children_pct",
                   [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
   columns.emplace("t_t", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::threshold_tests)); });
   columns.emplace("e_t", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::eviction_tests)); });

   columns.emplace("free_pct", [&](Column& col) { col << (local_total_free * 100.0 / bm.getPoolSize()); });
   columns.emplace("evicted_pages", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::evicted_pages)); });
   columns.emplace("rounds", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::pp_thread_rounds)); });
   columns.emplace("touches", [&](Column& col) { col << (sum_reset_add_to(PPCounters::pp_counters, &PPCounters::touched_bfs_counter, &PPCounters::total_touches)); });
   columns.emplace("submit_ms", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::submit_ms) * 100.0 / total); });
   columns.emplace("async_mb_ws", [&](Column& col) { col << (sum_reset(PPCounters::pp_counters, &PPCounters::async_wb_ms)); });
   columns.emplace("w_mib", [&](Column& col) {
      col << (sum_reset(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("allocate_ops", [&](Column& col) { col << (sum_reset(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter)); });
   columns.emplace("r_mib", [&](Column& col) {
      col << (sum_reset(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
}
// -------------------------------------------------------------------------------------
void BMTable::next()
{
   clear();
   local_phase_1_ms = sum_reset(PPCounters::pp_counters, &PPCounters::threshold_tests_ms);
   local_phase_2_ms = sum_reset(PPCounters::pp_counters, &PPCounters::eviction_ms);
   local_phase_3_ms = sum_reset(PPCounters::pp_counters, &PPCounters::phase_3_ms);
   local_poll_ms = sum_reset(PPCounters::pp_counters, &PPCounters::poll_ms);
   // -------------------------------------------------------------------------------------
   local_total_free = 0;
   for (u64 p_i = 0; p_i < bm.partitions_count; p_i++) {
      local_total_free += bm.getPartition(p_i).dram_free_list.counter.load();
   }
   total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
