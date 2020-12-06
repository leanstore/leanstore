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
   columns.emplace("tx", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::tx); });
   columns.emplace("tx_abort", [](Column& col) { col << sum(WorkerCounters::worker_counters, &WorkerCounters::tx_abort); });
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
   clear();
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
