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
   columns.emplace("c_cool_pct", [&](Column& col) { col << FLAGS_cool_pct; });
   columns.emplace("c_pp_threads", [&](Column& col) { col << FLAGS_pp_threads; });
   columns.emplace("c_partition_bits", [&](Column& col) { col << FLAGS_partition_bits; });
   columns.emplace("c_dram_gib", [&](Column& col) { col << FLAGS_dram_gib; });
   columns.emplace("c_ssd_gib", [&](Column& col) { col << FLAGS_ssd_gib; });
   columns.emplace("c_target_gib", [&](Column& col) { col << FLAGS_target_gib; });
   columns.emplace("c_run_for_seconds", [&](Column& col) { col << FLAGS_run_for_seconds; });
   columns.emplace("c_bulk_insert", [&](Column& col) { col << FLAGS_bulk_insert; });
   columns.emplace("c_backoff_strategy", [&](Column& col) { col << FLAGS_backoff_strategy; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_contention_split", [&](Column& col) { col << FLAGS_contention_split; });
   columns.emplace("c_cm_update_on", [&](Column& col) { col << FLAGS_cm_update_on; });
   columns.emplace("c_cm_period", [&](Column& col) { col << FLAGS_cm_period; });
   columns.emplace("c_cm_slowpath_threshold", [&](Column& col) { col << FLAGS_cm_slowpath_threshold; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_xmerge_k", [&](Column& col) { col << FLAGS_xmerge_k; });
   columns.emplace("c_xmerge", [&](Column& col) { col << FLAGS_xmerge; });
   columns.emplace("c_xmerge_target_pct", [&](Column& col) { col << FLAGS_xmerge_target_pct; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_zipf_factor", [&](Column& col) { col << FLAGS_zipf_factor; });
   columns.emplace("c_backoff", [&](Column& col) { col << FLAGS_backoff; });
   // -------------------------------------------------------------------------------------
   columns.emplace("c_wal", [&](Column& col) { col << FLAGS_wal; });
   columns.emplace("c_si", [&](Column& col) { col << FLAGS_si; });
   columns.emplace("c_vw", [&](Column& col) { col << FLAGS_vw; });
   columns.emplace("c_vw_todo", [&](Column& col) { col << FLAGS_vw_todo; });
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
