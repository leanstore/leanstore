#include "LeanStore.hpp"

#include "leanstore/counters/PPCounters.hpp"
#include "leanstore/counters/ThreadCounters.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <sstream>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
  // Set the default logger to file logger
  BMC::global_bf = &buffer_manager;
  buffer_manager.registerDatastructureType(99, btree::vs::BTree::getMeta());
}
// -------------------------------------------------------------------------------------
void LeanStore::startDebuggingThread()
{
  std::thread debugging_thread([&]() { debuggingThread(); });
  bg_threads_counter++;
  debugging_thread.detach();
}
// -------------------------------------------------------------------------------------
btree::vs::BTree& LeanStore::registerVSBTree(string name)
{
  assert(vs_btrees.find(name) == vs_btrees.end());
  auto& btree = vs_btrees[name];
  DTID dtid = buffer_manager.registerDatastructureInstance(99, reinterpret_cast<void*>(&btree), name);
  btree.init(dtid);
  return btree;
}
// -------------------------------------------------------------------------------------
btree::vs::BTree& LeanStore::retrieveVSBTree(string name)
{
  return vs_btrees[name];
}
using leanstore::utils::threadlocal::sum;
// -------------------------------------------------------------------------------------
void LeanStore::registerConfigEntry(string name, statCallback b)
{
  config_entries.emplace_back(std::move(name), b);
}
// -------------------------------------------------------------------------------------
u64 LeanStore::getConfigHash()
{
  return config_hash;
}
// -------------------------------------------------------------------------------------
void LeanStore::debuggingThread()
{
  pthread_setname_np(pthread_self(), "debugging_thread");
  // -------------------------------------------------------------------------------------
  std::ofstream stats_csv, dts_csv, threads_csv;
  std::ofstream::openmode open_flags;
  if (FLAGS_csv_truncate) {
    open_flags = ios::trunc;
  } else {
    open_flags = ios::app;
  }
  auto csv_open = [&](std::ofstream& csv, std::string name) {
    csv.open(name, open_flags);
    csv.seekp(0, ios::end);
    csv << std::setprecision(2) << std::fixed;
  };
  csv_open(stats_csv, FLAGS_csv_path + "_stats.csv");
  csv_open(dts_csv, FLAGS_csv_path + "_dts.csv");
  csv_open(threads_csv, FLAGS_csv_path + "_threads.csv");
  // -------------------------------------------------------------------------------------
  s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0, local_poll_ms = 0, total;
  u64 local_tx, local_total_free, local_total_cool;
  // -------------------------------------------------------------------------------------
  stat_entries.emplace_back("space_usage_gib", [&](ostream& out) {
    const double gib = buffer_manager.consumedPages() * 1.0 * PAGE_SIZE / 1024.0 / 1024.0 / 1024.0;
    out << gib;
  });
  stat_entries.emplace_back("consumed_pages", [&](ostream& out) { out << buffer_manager.consumedPages(); });
  stat_entries.emplace_back("p1_pct", [&](ostream& out) { out << (local_phase_1_ms * 100.0 / total); });
  stat_entries.emplace_back("p2_pct", [&](ostream& out) { out << (local_phase_2_ms * 100.0 / total); });
  stat_entries.emplace_back("p3_pct", [&](ostream& out) { out << (local_phase_3_ms * 100.0 / total); });
  stat_entries.emplace_back("poll_pct", [&](ostream& out) { out << (local_poll_ms * 100.0 / total); });
  stat_entries.emplace_back("find_parent_pct",
                            [&](ostream& out) { out << (sum(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
  stat_entries.emplace_back("iterate_children_pct",
                            [&](ostream& out) { out << (sum(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
  stat_entries.emplace_back("pc1", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_1_counter); });
  stat_entries.emplace_back("pc2", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_2_counter); });
  stat_entries.emplace_back("pc3", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_3_counter); });
  stat_entries.emplace_back("free_pct", [&](ostream& out) { out << (local_total_free * 100.0 / buffer_manager.dram_pool_size); });
  stat_entries.emplace_back("cool_pct", [&](ostream& out) { out << (local_total_cool * 100.0 / buffer_manager.dram_pool_size); });
  stat_entries.emplace_back("cool_pct_should", [&](ostream& out) {
    out << std::max<s64>(0,
                         ((FLAGS_cool_pct * 1.0 * buffer_manager.dram_pool_size / 100.0) - local_total_free) * 100.0 / buffer_manager.dram_pool_size);
  });
  stat_entries.emplace_back("evicted_mib", [&](ostream& out) {
    out << (sum(PPCounters::pp_counters, &PPCounters::evicted_pages) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
  });
  stat_entries.emplace_back("rounds", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds); });
  stat_entries.emplace_back("touches", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::touched_bfs_counter); });
  stat_entries.emplace_back("unswizzled", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::unswizzled_pages_counter); });
  stat_entries.emplace_back("submit_ms", [&](ostream& out) { out << (sum(PPCounters::pp_counters, &PPCounters::submit_ms) * 100.0 / total); });
  stat_entries.emplace_back("async_mb_ws", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::async_wb_ms); });
  stat_entries.emplace_back("w_mib", [&](ostream& out) {
    out << sum(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;
  });
  // -------------------------------------------------------------------------------------
  stat_entries.emplace_back("allocate_ops",
                            [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter); });
  stat_entries.emplace_back("r_mib", [&](ostream& out) {
    out << sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;
  });
  stat_entries.emplace_back("tx", [&](ostream& out) { out << local_tx; });
  // -------------------------------------------------------------------------------------
  // Constants for identifying the run [c for constants]
  config_entries.emplace_back("c_tag", [&](ostream& out) { out << FLAGS_tag; });
  config_entries.emplace_back("c_mutex", [&](ostream& out) { out << FLAGS_mutex; });
  config_entries.emplace_back("c_worker_threads", [&](ostream& out) { out << FLAGS_worker_threads; });
  config_entries.emplace_back("c_pin_threads", [&](ostream& out) { out << FLAGS_pin_threads; });
  config_entries.emplace_back("c_smt_interleaved", [&](ostream& out) { out << FLAGS_smt_interleaved; });
  // -------------------------------------------------------------------------------------
  config_entries.emplace_back("c_free_pct", [&](ostream& out) { out << FLAGS_free_pct; });
  config_entries.emplace_back("c_cool_pct", [&](ostream& out) { out << FLAGS_cool_pct; });
  config_entries.emplace_back("c_pp_threads", [&](ostream& out) { out << FLAGS_pp_threads; });
  config_entries.emplace_back("c_partition_bits", [&](ostream& out) { out << FLAGS_partition_bits; });
  config_entries.emplace_back("c_dram_gib", [&](ostream& out) { out << FLAGS_dram_gib; });
  config_entries.emplace_back("c_target_gib", [&](ostream& out) { out << FLAGS_target_gib; });
  config_entries.emplace_back("c_run_for_seconds", [&](ostream& out) { out << FLAGS_run_for_seconds; });
  config_entries.emplace_back("c_fs", [&](ostream& out) { out << FLAGS_fs; });
  config_entries.emplace_back("c_bstar", [&](ostream& out) { out << FLAGS_bstar; });
  config_entries.emplace_back("c_bulk_insert", [&](ostream& out) { out << FLAGS_bulk_insert; });
  config_entries.emplace_back("c_backoff_strategy", [&](ostream& out) { out << FLAGS_backoff_strategy; });
  // -------------------------------------------------------------------------------------
  config_entries.emplace_back("c_cm_split", [&](ostream& out) { out << FLAGS_cm_split; });
  config_entries.emplace_back("c_cm_update_on", [&](ostream& out) { out << FLAGS_cm_update_on; });
  config_entries.emplace_back("c_cm_period", [&](ostream& out) { out << FLAGS_cm_period; });
  config_entries.emplace_back("c_cm_slowpath_threshold", [&](ostream& out) { out << FLAGS_cm_slowpath_threshold; });
  // -------------------------------------------------------------------------------------
  config_entries.emplace_back("c_su_kwaymerge", [&](ostream& out) { out << FLAGS_su_kwaymerge; });
  config_entries.emplace_back("c_su_merge", [&](ostream& out) { out << FLAGS_su_merge; });
  config_entries.emplace_back("c_su_target_pct", [&](ostream& out) { out << FLAGS_su_target_pct; });
  // -------------------------------------------------------------------------------------
  config_entries.emplace_back("c_zipf_factor", [&](ostream& out) { out << FLAGS_zipf_factor; });
  config_entries.emplace_back("c_backoff", [&](ostream& out) { out << FLAGS_backoff; });
  // -------------------------------------------------------------------------------------
  std::stringstream config_concatenation;
  for (const auto& stat : config_entries) {
    stat.callback(config_concatenation);
  }
  config_hash = std::hash<std::string>{}(config_concatenation.str());
  config_entries.emplace_back("c_hash", [&](ostream& out) { out << config_hash; });
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  string dt_name;
  u64 dt_id;
  dt_entries.emplace_back("c_hash", [&](ostream& out) { out << config_hash; });
  dt_entries.emplace_back("dt_id", [&](ostream& out) { out << dt_id; });
  dt_entries.emplace_back("dt_name", [&](ostream& out) { out << dt_name; });
  dt_entries.emplace_back("dt_misses_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_misses_counter, dt_id); });
  dt_entries.emplace_back("dt_restarts_update_same_size",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_update_same_size, dt_id); });
  dt_entries.emplace_back("dt_restarts_structural_change",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_structural_change, dt_id); });
  dt_entries.emplace_back("dt_restarts_read",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_read, dt_id); });
  dt_entries.emplace_back("cm_split_succ_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_split_succ_counter, dt_id); });
  dt_entries.emplace_back("cm_split_fail_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_split_fail_counter, dt_id); });
  dt_entries.emplace_back("cm_merge_succ_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_merge_succ_counter, dt_id); });
  dt_entries.emplace_back("cm_merge_fail_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::cm_merge_fail_counter, dt_id); });
  dt_entries.emplace_back("su_merge_partial_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::su_merge_partial_counter, dt_id); });
  dt_entries.emplace_back("su_merge_full_counter",
                          [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::su_merge_full_counter, dt_id); });
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  // Print header
  if (stats_csv.tellp() == 0) {
    stats_csv << "t";
    for (const auto& stat : config_entries) {
      stats_csv << "," << stat.name;
    }
    for (const auto& stat : stat_entries) {
      stats_csv << "," << stat.name;
    }
    stats_csv << endl;
    // -------------------------------------------------------------------------------------
    threads_csv << "t,name,c_hash";
    {
      PerfEvent e;
      e.printCSVHeaders(threads_csv);
    }
    threads_csv << endl;
    // -------------------------------------------------------------------------------------
    dts_csv << "t";
    for (const auto& stat : dt_entries) {
      dts_csv << "," << stat.name;
    }
    for (u64 r_i = 0; r_i < WorkerCounters::max_researchy_counter; r_i++) {
      dts_csv << ","
              << "dt_researchy_" << std::to_string(r_i);
    }
    dts_csv << endl;
  }
  // -------------------------------------------------------------------------------------
  u64 time = 0;
  // -------------------------------------------------------------------------------------
  while (FLAGS_print_debug && bg_threads_keep_running) {
    // -------------------------------------------------------------------------------------
    local_phase_1_ms = sum(PPCounters::pp_counters, &PPCounters::phase_1_ms);
    local_phase_2_ms = sum(PPCounters::pp_counters, &PPCounters::phase_2_ms);
    local_phase_3_ms = sum(PPCounters::pp_counters, &PPCounters::phase_3_ms);
    local_poll_ms = sum(PPCounters::pp_counters, &PPCounters::poll_ms);
    // -------------------------------------------------------------------------------------
    total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
    // -------------------------------------------------------------------------------------
    local_tx = sum(WorkerCounters::worker_counters, &WorkerCounters::tx);
    global_stats.accumulated_tx_counter += local_tx;
    local_total_free = 0;
    local_total_cool = 0;
    for (u64 p_i = 0; p_i < buffer_manager.partitions_count; p_i++) {
      local_total_free += buffer_manager.partitions[p_i].dram_free_list.counter.load();
      local_total_cool += buffer_manager.partitions[p_i].cooling_bfs_counter.load();
    }
    // -------------------------------------------------------------------------------------
    stats_csv << time;
    for (const auto& entry : config_entries) {
      stats_csv << ",";
      entry.callback(stats_csv);
    }
    for (const auto& entry : stat_entries) {
      stats_csv << ",";
      entry.callback(stats_csv);
    }
    stats_csv << endl;
    // -------------------------------------------------------------------------------------
    {
      std::unique_lock guard(ThreadCounters::mutex);
      for (auto& counter : ThreadCounters::thread_counters) {
        threads_csv << time << "," << counter.second.name << "," << config_hash;
        counter.second.e->stopCounters();
        counter.second.e->printCSVData(threads_csv, 1);
        counter.second.e->startCounters();
        threads_csv << endl;
      }
    }
    // -------------------------------------------------------------------------------------
    for (const auto& dt : buffer_manager.dt_registry.dt_instances_ht) {
      dt_id = dt.first;
      dt_name = std::get<2>(dt.second);
      // -------------------------------------------------------------------------------------
      dts_csv << time;
      for (const auto& entry : dt_entries) {
        dts_csv << ",";
        entry.callback(dts_csv);
      }
      for (u64 r_i = 0; r_i < WorkerCounters::max_researchy_counter; r_i++) {
        dts_csv << "," << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_researchy, dt_id, r_i);
      }
      // -------------------------------------------------------------------------------------
      dts_csv << endl;
    }
    // -------------------------------------------------------------------------------------
    if (FLAGS_print_tx_console) {
      cout << time << "," << local_tx << endl;
    }
    // -------------------------------------------------------------------------------------
    sleep(FLAGS_print_debug_interval_s);
    time += FLAGS_print_debug_interval_s;
  }
  stats_csv.close();
  bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
LeanStore::GlobalStats LeanStore::getGlobalStats()
{
  return global_stats;
}
// -------------------------------------------------------------------------------------
void LeanStore::persist()
{
  buffer_manager.persist();
  std::vector<string> btree_names(fs_btrees.size());
  std::vector<u8> btree_objects(btree_size * fs_btrees.size());
  u64 b_i = 0;
  for (const auto& btree : fs_btrees) {
    btree_names.push_back(btree.first);
    std::memcpy(btree_objects.data() + (btree_size * b_i), btree.second.get(), btree_size);
    b_i++;
  }
  utils::writeBinary("leanstore_btree_names", btree_names);
  utils::writeBinary("leanstore_btree_objects", btree_objects);
}
// -------------------------------------------------------------------------------------
void LeanStore::restore()
{
  buffer_manager.restore();
  utils::FVector<std::string_view> btree_names("leanstore_btree_names");
  utils::FVector<u8> btree_objects("leanstore_btree_objects");
  for (u64 b_i = 0; b_i < btree_names.size(); b_i++) {
    auto iter = fs_btrees.emplace(btree_names[b_i], std::make_unique<u8[]>(btree_size));
    std::memcpy(iter.first->second.get(), btree_objects.data + (btree_size * b_i), btree_size);
  }
}
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
  bg_threads_keep_running = false;
  while (bg_threads_counter) {
    MYPAUSE();
  }
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
// -------------------------------------------------------------------------------------
