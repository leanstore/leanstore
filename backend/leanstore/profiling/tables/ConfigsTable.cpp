#include "ConfigsTable.hpp"

#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
std::string ConfigsTable::getName()
{
   return "configs";
}
// -------------------------------------------------------------------------------------
void ConfigsTable::add(string name, string value)
{
   columns.emplace(name, [&, value](Column& col) { col << value; });
}
// -------------------------------------------------------------------------------------
void ConfigsTable::open()
{
   columns.emplace("c_tag", [&](Column& col) { col << FLAGS_tag; });
   columns.emplace("c_worker_threads", [&](Column& col) { col << FLAGS_worker_threads; });
   columns.emplace("c_pin_threads", [&](Column& col) { col << FLAGS_pin_threads; });
   columns.emplace("c_smt", [&](Column& col) { col << FLAGS_smt; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_free_pct", [&](Column& col) { col << FLAGS_free_pct; });
   columns.emplace("c_pp_threads", [&](Column& col) { col << FLAGS_pp_threads; });
   columns.emplace("c_partition_bits", [&](Column& col) { col << FLAGS_partition_bits; });
   columns.emplace("c_dram_gib", [&](Column& col) { col << FLAGS_dram_gib; });
   columns.emplace("c_ssd_gib", [&](Column& col) { col << FLAGS_ssd_gib; });
   columns.emplace("c_target_gib", [&](Column& col) { col << FLAGS_target_gib; });
   columns.emplace("c_run_for_seconds", [&](Column& col) { col << FLAGS_run_for_seconds; });
   columns.emplace("c_bulk_insert", [&](Column& col) { col << FLAGS_bulk_insert; });
   columns.emplace("c_nc_reallocation", [&](Column& col) { col << FLAGS_nc_reallocation; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_contention_split", [&](Column& col) { col << FLAGS_contention_split; });
   columns.emplace("c_cm_update_on", [&](Column& col) { col << FLAGS_cm_update_on; });
   columns.emplace("c_cm_period", [&](Column& col) { col << FLAGS_cm_period; });
   columns.emplace("c_cm_slowpath_threshold", [&](Column& col) { col << FLAGS_cm_slowpath_threshold; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_xmerge_k", [&](Column& col) { col << FLAGS_xmerge_k; });
   columns.emplace("c_xmerge", [&](Column& col) { col << FLAGS_xmerge; });
   columns.emplace("c_xmerge_target_pct", [&](Column& col) { col << FLAGS_xmerge_target_pct; });
   columns.emplace("c_btree_prefix_compression", [&](Column& col) { col << FLAGS_btree_prefix_compression; });
   columns.emplace("c_btree_heads", [&](Column& col) { col << FLAGS_btree_heads; });
   columns.emplace("c_btree_hints", [&](Column& col) { col << FLAGS_btree_hints; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_zipf_factor", [&](Column& col) { col << FLAGS_zipf_factor; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_wal", [&](Column& col) { col << FLAGS_wal; });
   columns.emplace("c_wal_rfa", [&](Column& col) { col << FLAGS_wal_rfa; });
   columns.emplace("c_wal_tuple_rfa", [&](Column& col) { col << FLAGS_wal_tuple_rfa; });
   columns.emplace("c_wal_io_hack", [&](Column& col) { col << FLAGS_wal_pwrite; });
   columns.emplace("c_wal_fsync", [&](Column& col) { col << FLAGS_wal_fsync; });
   columns.emplace("c_wal_variant", [&](Column& col) { col << FLAGS_wal_variant; });
   columns.emplace("c_wal_log_writers", [&](Column& col) { col << FLAGS_wal_log_writers; });
   columns.emplace("c_todo", [&](Column& col) { col << FLAGS_todo; });
   columns.emplace("c_mv", [&](Column& col) { col << FLAGS_mv; });
   columns.emplace("c_vi", [&](Column& col) { col << FLAGS_vi; });
   columns.emplace("c_vi_fat_tuple", [&](Column& col) { col << FLAGS_vi_fat_tuple; });
   columns.emplace("c_vi_fat_tuple_alternative", [&](Column& col) { col << FLAGS_vi_fat_tuple_alternative; });
   columns.emplace("c_pgc", [&](Column& col) { col << FLAGS_pgc; });
   columns.emplace("c_isolation_level", [&](Column& col) { col << FLAGS_isolation_level; });
   columns.emplace("c_si_commit_protocol", [&](Column& col) { col << FLAGS_si_commit_protocol; });
   columns.emplace("c_olap_mode", [&](Column& col) { col << FLAGS_olap_mode; });
   columns.emplace("c_graveyard", [&](Column& col) { col << FLAGS_graveyard; });
   columns.emplace("c_history_tree_inserts", [&](Column& col) { col << FLAGS_history_tree_inserts; });
   // -------------------------------------------------------------------------------------
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
// -------------------------------------------------------------------------------------
u64 ConfigsTable::hash()
{
   std::stringstream config_concatenation;
   for (const auto& c : columns) {
      config_concatenation << c.second.values[0];
   }
   return std::hash<std::string>{}(config_concatenation.str());
}
// -------------------------------------------------------------------------------------
void ConfigsTable::next()
{
   // one time is enough
   return;
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
