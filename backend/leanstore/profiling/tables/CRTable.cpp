#include "CRTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
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
std::string CRTable::getName()
{
   return "cr";
}
// -------------------------------------------------------------------------------------
void CRTable::open()
{
   columns.emplace("key", [&](Column& out) { out << 0; });
   columns.emplace("wal_reserve_blocked", [&](Column& col) { col << (sum(CRCounters::cr_counters, &CRCounters::wal_reserve_blocked)); });
   columns.emplace("wal_reserve_immediate", [&](Column& col) { col << (sum(CRCounters::cr_counters, &CRCounters::wal_reserve_immediate)); });
   columns.emplace("gct_phase_1_pct", [&](Column& col) { col << 100.0 * p1 / total; });
   columns.emplace("gct_phase_2_pct", [&](Column& col) { col << 100.0 * p2 / total; });
   columns.emplace("gct_write_pct", [&](Column& col) { col << 100.0 * write / total; });
   columns.emplace("gct_committed_tx", [&](Column& col) { col << sum(CRCounters::cr_counters, &CRCounters::gct_committed_tx); });
   columns.emplace("gct_rounds", [&](Column& col) { col << sum(CRCounters::cr_counters, &CRCounters::gct_rounds); });
   columns.emplace("tx", [&](Column& col) { col << local_tx; });
   columns.emplace("tx_abort", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::tx_abort); });
   // -------------------------------------------------------------------------------------
   columns.emplace("tx_latency_us", [&](Column& col) {
     col << (local_tx > 0 ? sum(WorkerCounters::worker_counters, &WorkerCounters::total_tx_time) / local_tx : 0);
   });
   columns.emplace("tx_latency_us_95p", [&](Column& col) {
     col << local_tx_lat95p_us;
   });
   columns.emplace("tx_latency_us_99p", [&](Column& col) {
     col << local_tx_lat99p_us;
   });
   columns.emplace("tx_latency_us_99p9", [&](Column& col) {
     col << local_tx_lat99p9_us;
   });
   // -------------------------------------------------------------------------------------
   columns.emplace("wal_read_gib", [&](Column& col) {
      col << (sum(WorkerCounters::worker_counters, &WorkerCounters::wal_read_bytes) * 1.0) / 1024.0 / 1024.0 / 1024.0;
   });
   columns.emplace("wal_write_gib",
                   [&](Column& col) { col << (sum(CRCounters::cr_counters, &CRCounters::gct_write_bytes) * 1.0) / 1024.0 / 1024.0 / 1024.0; });
   columns.emplace("wal_miss_pct", [&](Column& col) { col << wal_miss_pct; });
   columns.emplace("wal_hit_pct", [&](Column& col) { col << wal_hit_pct; });
   columns.emplace("wal_miss", [&](Column& col) { col << wal_miss; });
   columns.emplace("wal_hit", [&](Column& col) { col << wal_hits; });
   columns.emplace("wal_total", [&](Column& col) { col << wal_total; });
}
// -------------------------------------------------------------------------------------
void CRTable::next()
{
   wal_hits = sum(WorkerCounters::worker_counters, &WorkerCounters::wal_buffer_hit);
   wal_miss = sum(WorkerCounters::worker_counters, &WorkerCounters::wal_buffer_miss);
   wal_total = wal_hits + wal_miss;
   wal_hit_pct = wal_hits * 1.0 / wal_total;
   wal_miss_pct = wal_miss * 1.0 / wal_total;
   // -------------------------------------------------------------------------------------
   p1 = sum(CRCounters::cr_counters, &CRCounters::gct_phase_1_ms);
   p2 = sum(CRCounters::cr_counters, &CRCounters::gct_phase_2_ms);
   write = sum(CRCounters::cr_counters, &CRCounters::gct_write_ms);
   total = p1 + p2 + write;

   local_tx = sum(WorkerCounters::worker_counters, &WorkerCounters::tx);
   u64 lat95p = 0;
   u64 lat99p = 0;
   u64 lat99p9 = 0;
   int counters = 0;
   for (typename decltype(WorkerCounters::worker_counters)::iterator i = WorkerCounters::worker_counters.begin(); i != WorkerCounters::worker_counters.end(); ++i) {
      lat95p = std::max(lat95p, i->tx_latency_hist.getPercentile(95));
      lat99p = std::max(lat99p, i->tx_latency_hist.getPercentile(99));
      lat99p9 = std::max(lat99p9, i->tx_latency_hist.getPercentile(99.9));
      i->tx_latency_hist.resetData();
      counters++;
   }
   local_tx_lat95p_us = lat95p;// / counters;
   local_tx_lat99p_us = lat99p;// / counters;
   local_tx_lat99p9_us = lat99p9;// / counters;

   clear();
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
