#include "CRTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
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
std::string CRTable::getName()
{
   return "cr";
}
// -------------------------------------------------------------------------------------
void CRTable::open()
{
   columns.emplace("key", [&](Column& out) { out << 0; });
   columns.emplace("wal_reserve_blocked", [&](Column& col) { col << (sum_reset(CRCounters::cr_counters, &CRCounters::wal_reserve_blocked)); });
   columns.emplace("wal_reserve_immediate", [&](Column& col) { col << (sum_reset(CRCounters::cr_counters, &CRCounters::wal_reserve_immediate)); });
   columns.emplace("gct_phase_1_pct", [&](Column& col) { col << 100.0 * p1 / total; });
   columns.emplace("gct_phase_2_pct", [&](Column& col) { col << 100.0 * p2 / total; });
   columns.emplace("gct_write_pct", [&](Column& col) { col << 100.0 * write / total; });
   columns.emplace("gct_committed_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::gct_committed_tx); });
   columns.emplace("gct_rounds", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::gct_rounds); });
   columns.emplace("tx", [](Column& col) { col << sum_reset_add_to(WorkerCounters::worker_counters, &WorkerCounters::tx, &WorkerCounters::tx_counter); });
   columns.emplace("tx_abort", [](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::tx_abort); });
   columns.emplace("olap_tx", [](Column& col) { col << sum_reset_add_to(WorkerCounters::worker_counters, &WorkerCounters::olap_tx, &WorkerCounters::tx_counter); });
   columns.emplace("olap_scanned_tuples", [](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::olap_scanned_tuples); });
   columns.emplace("olap_tx_abort", [](Column& col) { col << sum_reset(WorkerCounters::worker_counters, &WorkerCounters::olap_tx_abort); });
   columns.emplace("rfa_committed_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::rfa_committed_tx); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_snapshot_restart", [](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_snapshot_restart); });
   // -------------------------------------------------------------------------------------
   columns.emplace("wal_read_gib", [&](Column& col) {
      col << (sum_reset(WorkerCounters::worker_counters, &WorkerCounters::wal_read_bytes) * 1.0) / 1024.0 / 1024.0 / 1024.0;
   });
   columns.emplace("gct_write_gib", [&](Column& col) {
      col << (sum_reset(CRCounters::cr_counters, &CRCounters::gct_write_bytes) * 1.0) / 1024.0 / 1024.0 / 1024.0; });
   columns.emplace("wal_write_gib", [&](Column& col) {
      col << (sum_reset(WorkerCounters::worker_counters, &WorkerCounters::wal_write_bytes) * 1.0) / 1024.0 / 1024.0 / 1024.0; });
   columns.emplace("wal_miss_pct", [&](Column& col) { col << wal_miss_pct; });
   columns.emplace("wal_hit_pct", [&](Column& col) { col << wal_hit_pct; });
   columns.emplace("wal_miss", [&](Column& col) { col << wal_miss; });
   columns.emplace("wal_hit", [&](Column& col) { col << wal_hits; });
   columns.emplace("wal_total", [&](Column& col) { col << wal_total; });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_prepare_igc", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_prepare_igc); });
   columns.emplace("cc_cross_workers_visibility_check",
                   [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_cross_workers_visibility_check); });
   columns.emplace("cc_versions_space_removed", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_versions_space_removed); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_ms_oltp_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_oltp_tx); });
   columns.emplace("cc_ms_olap_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_olap_tx); });
   columns.emplace("cc_ms_gc", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_gc); });
   columns.emplace("cc_ms_gc_cm", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_gc_cm); });
   columns.emplace("cc_ms_gc_graveyard", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_gc_graveyard); });
   columns.emplace("cc_ms_gc_history_tree", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_gc_history_tree); });
   columns.emplace("cc_ms_fat_tuple", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_fat_tuple); });
   columns.emplace("cc_ms_fat_tuple_conversion", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_fat_tuple_conversion); });
   columns.emplace("cc_ms_snapshotting", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_snapshotting); });
   columns.emplace("cc_ms_committing", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_committing); });
   columns.emplace("cc_ms_history_tree_insert", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_history_tree_insert); });
   columns.emplace("cc_ms_history_tree_retrieve", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_history_tree_retrieve); });
   columns.emplace("cc_ms_refresh_global_state", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_refresh_global_state); });
   // -------------------------------------------------------------------------------------
   columns.emplace("cc_ms_start_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_start_tx); });
   columns.emplace("cc_ms_commit_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_commit_tx); });
   columns.emplace("cc_ms_abort_tx", [&](Column& col) { col << sum_reset(CRCounters::cr_counters, &CRCounters::cc_ms_abort_tx); });
}
// -------------------------------------------------------------------------------------
void CRTable::next()
{
   wal_hits = sum_reset(WorkerCounters::worker_counters, &WorkerCounters::wal_buffer_hit);
   wal_miss = sum_reset(WorkerCounters::worker_counters, &WorkerCounters::wal_buffer_miss);
   wal_total = wal_hits + wal_miss;
   wal_hit_pct = wal_hits * 1.0 / wal_total;
   wal_miss_pct = wal_miss * 1.0 / wal_total;
   // -------------------------------------------------------------------------------------
   p1 = sum_reset(CRCounters::cr_counters, &CRCounters::gct_phase_1_ms);
   p2 = sum_reset(CRCounters::cr_counters, &CRCounters::gct_phase_2_ms);
   write = sum_reset(CRCounters::cr_counters, &CRCounters::gct_write_ms);
   total = p1 + p2 + write;
   clear();
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
