#include "LeanStore.hpp"

#include "leanstore/counters/PPCounters.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_bool(log_stdout, false, "");
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
  // Set the default logger to file logger
  BMC::global_bf = &buffer_manager;
  buffer_manager.registerDatastructureType(99, btree::vs::BTree::getMeta());
  // -------------------------------------------------------------------------------------
  if (FLAGS_file_suffix == "") {
    file_suffix = to_string(chrono::high_resolution_clock::now().time_since_epoch().count());
  } else {
    file_suffix = FLAGS_file_suffix;
  }
  e = make_unique<PerfEvent>();
  std::thread debugging_thread([&]() { debuggingThread(); });
  bg_threads_counter++;
  debugging_thread.detach();
  // -------------------------------------------------------------------------------------
  e = make_unique<PerfEvent>();
  e->startCounters();
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
void LeanStore::debuggingThread()
{
  pthread_setname_np(pthread_self(), "debugging_thread");
  // -------------------------------------------------------------------------------------
  auto file_name = [&](const string prefix) { return prefix + "_" + file_suffix + ".csv"; };
  // -------------------------------------------------------------------------------------
  std::ofstream pp_csv;
  string pp_csv_file_path;
  pp_csv.open(file_name("pp"), ios::out | ios::trunc);
  pp_csv << std::setprecision(2) << std::fixed;
  // -------------------------------------------------------------------------------------
  std::ofstream dt_csv;
  string workers_csv_file_path;
  dt_csv.open(file_name("dt"), ios::out | ios::trunc);
  dt_csv << std::setprecision(2) << std::fixed;
  // -------------------------------------------------------------------------------------
  using statCallback = std::function<void(ostream&)>;
  struct StatEntry {
    string name;
    statCallback callback;
    StatEntry(string&& n, statCallback b) : name(std::move(n)), callback(b) {}
  };
  // -------------------------------------------------------------------------------------
  vector<StatEntry> stats;
  s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0, local_poll_ms = 0, total;
  u64 local_tx, local_total_free, local_total_cool;
  // -------------------------------------------------------------------------------------
  stats.emplace_back("p1_pct", [&](ostream& out) { out << (local_phase_1_ms * 100.0 / total); });
  stats.emplace_back("p2_pct", [&](ostream& out) { out << (local_phase_2_ms * 100.0 / total); });
  stats.emplace_back("p3_pct", [&](ostream& out) { out << (local_phase_3_ms * 100.0 / total); });
  stats.emplace_back("poll_pct", [&](ostream& out) { out << (local_poll_ms * 100.0 / total); });
  stats.emplace_back("find_parent_pct", [&](ostream& out) { out << (sum(PPCounters::pp_counters, &PPCounters::find_parent_ms) * 100.0 / total); });
  stats.emplace_back("iterate_children_pct",
                     [&](ostream& out) { out << (sum(PPCounters::pp_counters, &PPCounters::iterate_children_ms) * 100.0 / total); });
  stats.emplace_back("pc1", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_1_counter); });
  stats.emplace_back("pc2", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_2_counter); });
  stats.emplace_back("pc3", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::phase_3_counter); });
  stats.emplace_back("free_pct", [&](ostream& out) { out << (local_total_free * 100.0 / buffer_manager.dram_pool_size); });
  stats.emplace_back("cool_pct", [&](ostream& out) { out << (local_total_cool * 100.0 / buffer_manager.dram_pool_size); });
  stats.emplace_back("evicted_mib", [&](ostream& out) {
    out << (sum(PPCounters::pp_counters, &PPCounters::evicted_pages) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0);
  });
  stats.emplace_back("rounds", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::pp_thread_rounds); });
  stats.emplace_back("unswizzled", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::unswizzled_pages_counter); });
  stats.emplace_back("cpus", [&](ostream& out) { out << e->getCPUs(); });
  stats.emplace_back("submit_ms", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::submit_ms); });
  stats.emplace_back("async_mb_ws", [&](ostream& out) { out << sum(PPCounters::pp_counters, &PPCounters::async_wb_ms); });
  stats.emplace_back("w_mib", [&](ostream& out) {
    out << sum(PPCounters::pp_counters, &PPCounters::flushed_pages_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;
  });
  // -------------------------------------------------------------------------------------
  stats.emplace_back("allocate_ops",
                     [&](ostream& out) { out << sum(WorkerCounters::worker_counters, &WorkerCounters::allocate_operations_counter); });
  stats.emplace_back("r_mib", [&](ostream& out) {
    out << sum(WorkerCounters::worker_counters, &WorkerCounters::read_operations_counter) * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0;
  });
  stats.emplace_back("tx", [&](ostream& out) { out << local_tx; });
  // -------------------------------------------------------------------------------------
  // Print header
  pp_csv << "t";
  for (const auto& stat : stats) {
    pp_csv << "," << stat.name;
  }
  e->printCSVHeaders(pp_csv);
  pp_csv << endl;
  // -------------------------------------------------------------------------------------
  dt_csv << "t,id,name,miss,restarts_updates,restarts_structural,restarts_read, researchy" << endl;
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------
  u64 time = 0;
  // -------------------------------------------------------------------------------------
  while (FLAGS_print_debug && bg_threads_keep_running) {
    e->stopCounters();
    // -------------------------------------------------------------------------------------
    local_phase_1_ms = sum(PPCounters::pp_counters, &PPCounters::phase_1_ms);
    local_phase_2_ms = sum(PPCounters::pp_counters, &PPCounters::phase_2_ms);
    local_phase_3_ms = sum(PPCounters::pp_counters, &PPCounters::phase_3_ms);
    local_poll_ms = sum(PPCounters::pp_counters, &PPCounters::poll_ms);
    // -------------------------------------------------------------------------------------
    total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
    // -------------------------------------------------------------------------------------
    local_tx = sum(WorkerCounters::worker_counters, &WorkerCounters::tx);
    local_total_free = 0;
    local_total_cool = 0;
    for (u64 p_i = 0; p_i < buffer_manager.partitions_count; p_i++) {
      local_total_free += buffer_manager.partitions[p_i].dram_free_list.counter.load();
      local_total_cool += buffer_manager.partitions[p_i].cooling_bfs_counter.load();
    }
    e->stopCounters();
    // -------------------------------------------------------------------------------------
    pp_csv << time;
    for (const auto& stat : stats) {
      pp_csv << ",";
      stat.callback(pp_csv);
    }
    e->printCSVData(pp_csv, local_tx);
    pp_csv << endl;
    // -------------------------------------------------------------------------------------
    // -------------------------------------------------------------------------------------
    for (const auto& dt : buffer_manager.dt_registry.dt_instances_ht) {
      const u64 dt_id = dt.first;
      const string& dt_name = std::get<2>(dt.second);
      dt_csv << time << "," << dt_id << "," << dt_name << "," << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_misses_counter, dt_id)
             << "," << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_update_same_size, dt_id) << ","
             << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_structural_change, dt_id) << ","
             << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_restarts_read, dt_id) << ","
             << sum(WorkerCounters::worker_counters, &WorkerCounters::dt_researchy, dt_id) << endl;
      // -------------------------------------------------------------------------------------
      WorkerCounters::myCounters().dt_misses_counter[dt_id] = WorkerCounters::myCounters().dt_restarts_update_same_size[dt_id] =
          WorkerCounters::myCounters().dt_restarts_read[dt_id] = WorkerCounters::myCounters().dt_restarts_structural_change[dt_id] =
              WorkerCounters::myCounters().dt_researchy[dt_id] = 0;
    }
    // -------------------------------------------------------------------------------------
    e->startCounters();
    // -------------------------------------------------------------------------------------
    sleep(FLAGS_print_debug_interval_s);
    time += FLAGS_print_debug_interval_s;
  }
  pp_csv.close();
  bg_threads_counter--;
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
    _mm_pause();
  }
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
// -------------------------------------------------------------------------------------
