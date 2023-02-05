#include "BMTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include <iomanip>
#include <string>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
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
   columns.emplace("consumed_pages", [&](Column& col) { col << bm.consumedPages(); });
   columns.emplace("p1_ms", [&](Column& col) { col << (local_phase_1_ms ); });
   columns.emplace("p2_ms", [&](Column& col) { col << (local_phase_2_ms ); });
   columns.emplace("p3_ms", [&](Column& col) { col << (local_phase_3_ms ); });
   columns.emplace("p1_pct", [&](Column& col) { col << (local_phase_1_ms * 100.0 / total); });
   columns.emplace("p2_pct", [&](Column& col) { col << (local_phase_2_ms * 100.0 / total); });
   columns.emplace("p3_pct", [&](Column& col) { col << (local_phase_3_ms * 100.0 / total); });
   columns.emplace("poll_pct", [&](Column& col) { col << ((local_poll_ms * 100.0 / total)); });
   columns.emplace("find_parent_pct", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
   columns.emplace("iterate_children_pct",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
   columns.emplace("pc1", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_1_counter)); });
   columns.emplace("pc2", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_2_counter)); });
   columns.emplace("pc2_added", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_2_added)); });
   columns.emplace("pc3", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::phase_3_counter)); });
   columns.emplace("a_free", [&](Column& col) { col << local_total_free; });
   columns.emplace("a_free_pct", [&](Column& col) { col << (local_total_free * 100.0 / bm.getPoolSize()); });
   columns.emplace("a_cool", [&](Column& col) { col << (local_total_cool); });
   columns.emplace("a_cool_pct", [&](Column& col) { col << (local_total_cool * 100.0 / bm.getPoolSize()); });
   columns.emplace("a_cool_pct_should", [&](Column& col) {
      col << (std::max<s64>(0, ((FLAGS_cool_pct * 1.0 * bm.getPoolSize() / 100.0) - local_total_free) * 100.0 / bm.getPoolSize()));
   });
   columns.emplace("evicted_mib",
                   [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::evicted_pages) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0); });
   columns.emplace("a_pp_avg_sub", [&](Column& col) { col << (local_pp_submit_cnt == 0 ? "-" : std::to_string(1.0 * local_pp_submitted / local_pp_submit_cnt)); });
   columns.emplace("a_pp_subs", [&](Column& col) { col << local_pp_submitted; });
   columns.emplace("a_pp_sub_cnt", [&](Column& col) { col << local_pp_submit_cnt; });
   columns.emplace("a_pp_failed_picks", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::failed_bf_pick_attempts)); });
   columns.emplace("a_pp_failed_picks_cause_dbg", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::failed_bf_pick_attempts_cause_dbg)); });
   columns.emplace("a_pp_qlen_avg", [&](Column& col) { col << 1.0 * (sum(PPCounters::pp_counters, &PPCounters::pp_qlen)) / local_pp_qlen_cnt ; });
   columns.emplace("a_pp_pages_iterate_avg", [&](Column& col) { col << 1.0 * (sum(PPCounters::pp_counters, &PPCounters::pp_pages_iterate)) / local_pp_qlen_cnt ; });
   columns.emplace("a_pp_qlen_cnt", [&](Column& col) { col << local_pp_qlen_cnt ; });
   columns.emplace("rounds", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds)); });
   columns.emplace("touches", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::touched_bfs_counter)); });
   columns.emplace("unswizzled", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::unswizzled_pages_counter)); });
   columns.emplace("submit_ms", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::submit_ms) * 100.0 / total); });
   columns.emplace("async_mb_ws", [&](Column& col) { col << (sum(PPCounters::pp_counters, &PPCounters::async_wb_ms)); });
   columns.emplace("w_mib", [&](Column& col) {
      col << (sum(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("allocate_ops", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter)); });
   columns.emplace("inner_p", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter)); });
   columns.emplace("a_free_list_pop_failed", [&](Column& col) { col << (sum(WorkerCounters::worker_counters, &WorkerCounters::free_list_pop_failed)); });
   columns.emplace("r_mib", [&](Column& col) {
      col << (sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("io_outstanding_50p", [&](Column& col) { col << (leanstore::utils::threadlocal::thr_aggr_max(PPCounters::pp_counters, &PPCounters::outstandinig_50p)); });
   columns.emplace("io_outstanding_99p9", [&](Column& col) { col << (leanstore::utils::threadlocal::thr_aggr_max(PPCounters::pp_counters, &PPCounters::outstandinig_99p9)); });
   columns.emplace("io_outstanding_read", [&](Column& col) { col << (leanstore::utils::threadlocal::thr_aggr_max(PPCounters::pp_counters, &PPCounters::outstandinig_read)); });
   columns.emplace("io_outstanding_write", [&](Column& col) { col << (leanstore::utils::threadlocal::thr_aggr_max(PPCounters::pp_counters, &PPCounters::outstandinig_write)); });
   // -------------------------------------------------------------------------------------
   columns.emplace("timestamp", [](Column& col) {
      using std::chrono::system_clock;
      auto currentTime = std::chrono::system_clock::now();
      char buffer[80];
      auto transformed = currentTime.time_since_epoch().count() / 1000000;
      int millis = transformed % 1000;
      std::time_t tt = system_clock::to_time_t( currentTime );
      auto timeinfo = localtime(&tt);
      strftime(buffer, 80, "%F %H:%M:%S", timeinfo);
      char buffer2[85];
      snprintf(buffer2, 85,"%s:%i", buffer, (int)millis);
      col << std::string(buffer2);;
   });
}
// -------------------------------------------------------------------------------------
void BMTable::next()
{
   clear();
   local_phase_1_ms = sum(PPCounters::pp_counters, &PPCounters::phase_1_ms);
   local_phase_2_ms = sum(PPCounters::pp_counters, &PPCounters::phase_2_ms);
   local_phase_3_ms = sum(PPCounters::pp_counters, &PPCounters::phase_3_ms);
   local_poll_ms = sum(PPCounters::pp_counters, &PPCounters::poll_ms);
   // -------------------------------------------------------------------------------------
   local_total_free = 0;
   local_total_cool = 0;
   for (u64 p_i = 0; p_i < bm.cooling_partitions_count; p_i++) {
      local_total_free += bm.cooling_partitions[p_i].dram_free_list.counter.load();
      local_total_cool += bm.cooling_partitions[p_i].cooling_bfs_counter.load();
   }
   local_pp_submit_cnt = sum(PPCounters::pp_counters, &PPCounters::submit_cnt);
   local_pp_submitted = sum(PPCounters::pp_counters, &PPCounters::submitted);
   local_pp_qlen_cnt = sum(PPCounters::pp_counters, &PPCounters::pp_qlen_cnt);

   total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
