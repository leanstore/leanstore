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
void ConfigsTable::open()
{
  columns.emplace_back("c_tag", [&](ColumnValues& values) { values.push_back(FLAGS_tag); });
  columns.emplace_back("c_worker_threads", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_worker_threads)); });
  columns.emplace_back("c_pin_threads", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_pin_threads)); });
  columns.emplace_back("c_smt", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_smt)); });
  // -------------------------------------------------------------------------------------
  columns.emplace_back("c_free_pct", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_free_pct)); });
  columns.emplace_back("c_cool_pct", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_cool_pct)); });
  columns.emplace_back("c_pp_threads", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_pp_threads)); });
  columns.emplace_back("c_partition_bits", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_partition_bits)); });
  columns.emplace_back("c_dram_gib", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_dram_gib)); });
  columns.emplace_back("c_ssd_gib", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_ssd_gib)); });
  columns.emplace_back("c_target_gib", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_target_gib)); });
  columns.emplace_back("c_run_for_seconds", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_run_for_seconds)); });
  columns.emplace_back("c_bulk_insert", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_bulk_insert)); });
  columns.emplace_back("c_backoff_strategy", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_backoff_strategy)); });
  // -------------------------------------------------------------------------------------
  columns.emplace_back("c_contention_split", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_contention_split)); });
  columns.emplace_back("c_cm_update_on", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_cm_update_on)); });
  columns.emplace_back("c_cm_period", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_cm_period)); });
  columns.emplace_back("c_cm_slowpath_threshold", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_cm_slowpath_threshold)); });
  // -------------------------------------------------------------------------------------
  columns.emplace_back("c_xmerge_k", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_xmerge_k)); });
  columns.emplace_back("c_xmerge", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_xmerge)); });
  columns.emplace_back("c_xmerge_target_pct", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_xmerge_target_pct)); });
  // -------------------------------------------------------------------------------------
  columns.emplace_back("c_zipf_factor", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_zipf_factor)); });
  columns.emplace_back("c_backoff", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_backoff)); });
  // -------------------------------------------------------------------------------------
  columns.emplace_back("c_wal", [&](ColumnValues& values) { values.push_back(to_string(FLAGS_wal)); });
  // -------------------------------------------------------------------------------------
  for (auto& c : columns) {
    c.generator(c.values);
  }
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
