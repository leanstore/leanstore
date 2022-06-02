#include "LeanStore.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

#include <locale>
#include <sstream>
// -------------------------------------------------------------------------------------
using namespace tabulate;
using leanstore::utils::threadlocal::sum_reset;
namespace rs = rapidjson;
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   LeanStore::addStringFlag("SSD_PATH", &FLAGS_ssd_path);
   if (FLAGS_recover_file != "./leanstore.json") {
      FLAGS_recover = true;
   }
   if (FLAGS_persist_file != "./leanstore.json") {
      FLAGS_persist = true;
   }
   if (FLAGS_recover) {
      deserializeFlags();
   }
   // -------------------------------------------------------------------------------------
   // Check if configurations make sense
   ensure(!FLAGS_vw || FLAGS_wal);
   // -------------------------------------------------------------------------------------
   // Set the default logger to file logger
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT | O_CREAT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   if (ssd_fd == -1) {
      perror("posix error");
      SetupFailed("Could not open the file or the SSD block device");
   }
   if (FLAGS_falloc > 0) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (u8*)aligned_alloc(512, gib_size);
      for (u64 i = 0; i < FLAGS_falloc; i++) {
         const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
         posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   // -------------------------------------------------------------------------------------
   buffer_manager = make_unique<storage::BufferManager>(ssd_fd);
   BMC::global_bf = buffer_manager.get();
   // -------------------------------------------------------------------------------------
   DTRegistry::global_dt_registry.registerDatastructureType(0, storage::btree::BTreeLL::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(1, storage::btree::BTreeVW::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(2, storage::btree::BTreeVI::getMeta());
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      deserializeState();
   }
   // -------------------------------------------------------------------------------------
   u64 end_of_block_device;
   if (FLAGS_wal_offset_gib == 0) {
      ioctl(ssd_fd, BLKGETSIZE64, &end_of_block_device);
   } else {
      end_of_block_device = FLAGS_wal_offset_gib * 1024 * 1024 * 1024;
   }
   cr_manager = make_unique<cr::CRManager>(ssd_fd, end_of_block_device);
   cr::CRManager::global = cr_manager.get();
}
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
      MYPAUSE();
   }
   if (FLAGS_persist) {
      serializeState();
      buffer_manager->writeAllBufferFrames();
   }
}
// -------------------------------------------------------------------------------------
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([&]() { doProfiling(); });
   bg_threads_counter++;
   profiling_thread.detach();
   printStats(true);
}
void LeanStore::doProfiling()
{
   // Needed Datastructures
   profiling::BMTable bm_table(*buffer_manager.get());
   profiling::DTTable dt_table(*buffer_manager.get());
   profiling::CPUTable cpu_table;
   profiling::CRTable cr_table;
   profiling::ResultsTable results_table;
   vector<profiling::ProfilingTable*> timedTables = {&bm_table, &dt_table, &cpu_table, &cr_table};
   vector<profiling::ProfilingTable*> untimedTables = {&results_table, &configs_table};
   // -------------------------------------------------------------------------------------
   vector<ofstream> timedCsvs, untimedCsvs;
   ofstream config_csv;
   for (u64 t_i = 0; t_i < timedTables.size(); t_i++) {
      timedCsvs.emplace_back();
      prepareCSV(timedTables[t_i], timedCsvs.back());
   }
   for (u64 t_i = 0; t_i < untimedTables.size(); t_i++) {
      untimedCsvs.emplace_back();
      prepareCSV(untimedTables[t_i], untimedCsvs.back(), false);
   }
   // -------------------------------------------------------------------------------------
   config_hash = configs_table.hash();
   // -------------------------------------------------------------------------------------
   // Timd tables: every second
   u64 seconds = 0;
   while (bg_threads_keep_running) {
      for (u64 t_i = 0; t_i < timedTables.size(); t_i++) {
         printTable(timedTables[t_i], timedCsvs[t_i], seconds);
         // TODO: Websocket, CLI
      }
      // -------------------------------------------------------------------------------------
      const u64 tx = stoi(cr_table.get("0", "tx"));
      // Global Stats
      global_stats.accumulated_tx_counter += tx;
      // -------------------------------------------------------------------------------------
      // Console
      // -------------------------------------------------------------------------------------
      print_tx_console(bm_table, cpu_table, cr_table, seconds, tx);
      seconds ++;
   }
   printStats(false);
   if(seconds > 0){
      results_table.total_seconds =seconds -1;
   }
   for (u64 t_i = 0; t_i < untimedTables.size(); t_i++) {
      printTable(untimedTables[t_i], untimedCsvs[t_i], 0, false);
   }
   bg_threads_counter--;
}
void LeanStore::prepareCSV(profiling::ProfilingTable* table, ofstream& csv, bool print_seconds) const
{
   table->open();
   // -------------------------------------------------------------------------------------
   csv.open(FLAGS_csv_path + "_" + table->getName() + ".csv", FLAGS_csv_truncate ? ios::trunc : ios::app);
   csv.seekp(0, ios::end);
   csv << setprecision(2) << fixed;
   if (csv.tellp() == 0) {
      if(print_seconds){
         csv << "t,";
      }
      csv << "c_hash";
      for (auto& c : table->getColumns()) {
         csv << "," << c.first;
      }
      csv << endl;
   }
}
void LeanStore::print_tx_console(profiling::BMTable& bm_table,
                                profiling::CPUTable& cpu_table,
                                profiling::CRTable& cr_table,
                                u64 seconds,
                                const u64 tx) const
{
   if (FLAGS_print_tx_console) {
      const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
      const double cycles_per_tx = cpu_table.workers_agg_events["cycle"] / tx;
      const double l1_per_tx = cpu_table.workers_agg_events["L1-miss"] / tx;
      const double lc_per_tx = cpu_table.workers_agg_events["LLC-miss"] / tx;
      Table table;
      table.add_row({"t", "TX P", "TX A", "TX C", "W MiB", "R MiB", "Instrs/TX", "Cycles/TX", "CPUs", "L1/TX", "LLC", "WAL T", "WAL R G",
                     "WAL W G", "GCT Rounds", "FreeListLength", "Part0", "Part1", "last_min"});
      u64 free = buffer_manager->free_list.counter, part0 = buffer_manager->getPartition(0).partition_size, part1 = buffer_manager->getPartition(1).partition_size;
      table.add_row({to_string(seconds), cr_table.get("0", "tx"), cr_table.get("0", "tx_abort"), cr_table.get("0", "gct_committed_tx"),
                     bm_table.get("0", "w_mib"), bm_table.get("0", "r_mib"), to_string(instr_per_tx), to_string(cycles_per_tx),
                     to_string(cpu_table.workers_agg_events["CPU"]), to_string(l1_per_tx), to_string(lc_per_tx),
                     cr_table.get("0", "wal_total"), cr_table.get("0", "wal_read_gib"), cr_table.get("0", "wal_write_gib"),
                     cr_table.get("0", "gct_rounds"), to_string(free), to_string(part0), to_string(part1), to_string(buffer_manager->last_min)});
      // -------------------------------------------------------------------------------------
      table.format().width(10);
      table.column(0).format().width(5);
      table.column(1).format().width(10);
      // -------------------------------------------------------------------------------------
      auto print_table = [](Table& table, function<bool(u64)> predicate) {
         stringstream ss;
         table.print(ss);
         string str = ss.str();
         u64 line_n = 0;
         for (u64 i = 0; i < str.size(); i++) {
            if (str[i] == '\n') {
               line_n++;
            }
            if (predicate(line_n)) {
               cout << str[i];
            }
         }
      };
      if (seconds == 0) {
         print_table(table, [](u64 line_n) { return (line_n < 3) || (line_n == 4); });
      } else {
         print_table(table, [](u64 line_n) { return line_n == 4; });
      }
      // -------------------------------------------------------------------------------------
      this_thread::sleep_for(chrono::milliseconds(1000));
      locale::global(locale::classic());
   }
}
void LeanStore::printTable(profiling::ProfilingTable* table, basic_ofstream<char>& csv, u64 seconds, bool print_seconds) const
{
   table->next();
   if (table->size() == 0)
      return;
   // -------------------------------------------------------------------------------------
   // CSV
   for (u64 r_i = 0; r_i < table->size(); r_i++) {
      if(print_seconds)
         csv << seconds << ",";
      csv << config_hash;
      for (auto& c : table->getColumns()) {
         csv << "," << c.second.values[r_i];
      }
      csv << endl;
   }
}
// -------------------------------------------------------------------------------------
void LeanStore::printStats(bool reset)
{
   if (reset) {
      cout << "total newPages: " << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::new_pages_counter) << endl;
      cout << "total misses: " << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::missed_hit_counter) << endl;
      cout << "total hits: "
           << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::hot_hit_counter) +
                  utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::cold_hit_counter)
           << endl;
      cout << "total jumps: " << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::jumps) << endl;
      cout << "total tx: " << utils::threadlocal::sum_reset(WorkerCounters::worker_counters, &WorkerCounters::tx_counter) << endl;
      cout << "total writes: " << utils::threadlocal::sum_reset(PPCounters::pp_counters, &PPCounters::total_writes) << endl;
      cout << "total evictions: " << utils::threadlocal::sum_reset(PPCounters::pp_counters, &PPCounters::total_evictions) << endl;
   }else{
      cout << "no reset" << endl;
      cout << "total newPages: " << utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::new_pages_counter) << endl;
      cout << "total misses: " << utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::missed_hit_counter) << endl;
      cout << "total hits: "
           << utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::hot_hit_counter) +
                  utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::cold_hit_counter)
           << endl;
      cout << "total jumps: " << utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::jumps) << endl;
      cout << "total tx: " << utils::threadlocal::sum_no_reset(WorkerCounters::worker_counters, &WorkerCounters::tx_counter) << endl;
      cout << "total writes: " << utils::threadlocal::sum_no_reset(PPCounters::pp_counters, &PPCounters::total_writes) << endl;
      cout << "total evictions: " << utils::threadlocal::sum_no_reset(PPCounters::pp_counters, &PPCounters::total_evictions) << endl;

   }
}
storage::btree::BTreeLL& LeanStore::registerBTreeLL(string name)
{
   assert(btrees_ll.find(name) == btrees_ll.end());
   auto& btree = btrees_ll[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), name);
   btree.create(dtid);
   return btree;
}

// -------------------------------------------------------------------------------------
storage::btree::BTreeVW& LeanStore::registerBTreeVW(string name)
{
   assert(btrees_vw.find(name) == btrees_vw.end());
   auto& btree = btrees_vw[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(1, reinterpret_cast<void*>(&btree), name);
   btree.create(dtid);
   return btree;
}
// -------------------------------------------------------------------------------------
storage::btree::BTreeVI& LeanStore::registerBTreeVI(string name)
{
   assert(btrees_vi.find(name) == btrees_vi.end());
   auto& btree = btrees_vi[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), name);
   btree.create(dtid);
   return btree;
}
// -------------------------------------------------------------------------------------
u64 LeanStore::getConfigHash()
{
   return config_hash;
}
// -------------------------------------------------------------------------------------
LeanStore::GlobalStats LeanStore::getGlobalStats()
{
   return global_stats;
}
// -------------------------------------------------------------------------------------
void LeanStore::serializeState()
{
   // Serialize data structure instances
   std::ofstream json_file;
   json_file.open(FLAGS_persist_file, ios::trunc);
   rapidjson::Document d;
   rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
   d.SetObject();
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialized_bm_map = buffer_manager->serialize();
   rs::Value bm_serialized(rs::kObjectType);
   for (const auto& [key, value] : serialized_bm_map) {
      rs::Value k, v;
      k.SetString(key.c_str(), key.length(), allocator);
      v.SetString(value.c_str(), value.length(), allocator);
      bm_serialized.AddMember(k, v, allocator);
   }
   d.AddMember("buffer_manager", bm_serialized, allocator);
   // -------------------------------------------------------------------------------------
   rapidjson::Value dts(rapidjson::kArrayType);
   for (auto& dt : DTRegistry::global_dt_registry.dt_instances_ht) {
      rs::Value dt_json_object(rs::kObjectType);
      const DTID dt_id = dt.first;
      rs::Value name;
      name.SetString(std::get<2>(dt.second).c_str(), std::get<2>(dt.second).length(), allocator);
      dt_json_object.AddMember("name", name, allocator);
      dt_json_object.AddMember("type", rs::Value(std::get<0>(dt.second)), allocator);
      dt_json_object.AddMember("id", rs::Value(dt_id), allocator);
      // -------------------------------------------------------------------------------------
      std::unordered_map<std::string, std::string> serialized_dt_map = DTRegistry::global_dt_registry.serialize(dt_id);
      rs::Value dt_serialized(rs::kObjectType);
      for (const auto& [key, value] : serialized_dt_map) {
         rs::Value k, v;
         k.SetString(key.c_str(), key.length(), allocator);
         v.SetString(value.c_str(), value.length(), allocator);
         dt_serialized.AddMember(k, v, allocator);
      }
      dt_json_object.AddMember("serialized", dt_serialized, allocator);
      // -------------------------------------------------------------------------------------
      dts.PushBack(dt_json_object, allocator);
   }
   d.AddMember("registered_datastructures", dts, allocator);
   // -------------------------------------------------------------------------------------
   serializeFlags(d);
   rapidjson::StringBuffer sb;
   rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
   d.Accept(writer);
   json_file << sb.GetString();
}
void LeanStore::serializeFlags(rapidjson::Document& d)
{
   rs::Value flags_serialized(rs::kObjectType);
   rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
   for (auto flags : persistFlagsString()) {
      string f_name = std::get<0>(flags);
      string f_value = *std::get<1>(flags);
      rapidjson::Value name(f_name.c_str(), f_name.length(), allocator);
      rapidjson::Value value(f_value.c_str(), f_value.length(), allocator);
      flags_serialized.AddMember(name, value, allocator);
   }
   for (auto flags : persistFlagsS64()) {
      rapidjson::Value name(std::get<0>(flags).c_str(), std::get<0>(flags).length(), allocator);
      string value_string = std::to_string(*std::get<1>(flags));
      rapidjson::Value value(value_string.c_str(), value_string.length(), allocator);
      flags_serialized.AddMember(name, value, allocator);
   }
   d.AddMember("flags", flags_serialized, allocator);
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeState()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& bm = d["buffer_manager"];
   std::unordered_map<std::string, std::string> serialized_bm_map;
   for (rs::Value::ConstMemberIterator itr = bm.MemberBegin(); itr != bm.MemberEnd(); ++itr) {
      serialized_bm_map[itr->name.GetString()] = itr->value.GetString();
   }
   buffer_manager->deserialize(serialized_bm_map);
   // -------------------------------------------------------------------------------------
   const rs::Value& dts = d["registered_datastructures"];
   assert(dts.IsArray());
   for (auto& dt : dts.GetArray()) {
      assert(dt.IsObject());
      const DTID dt_id = dt["id"].GetInt();
      const DTType dt_type = dt["type"].GetInt();
      const std::string dt_name = dt["name"].GetString();
      std::unordered_map<std::string, std::string> serialized_dt_map;
      const rs::Value& serialized_object = dt["serialized"];
      for (rs::Value::ConstMemberIterator itr = serialized_object.MemberBegin(); itr != serialized_object.MemberEnd(); ++itr) {
         serialized_dt_map[itr->name.GetString()] = itr->value.GetString();
      }
      // -------------------------------------------------------------------------------------
      if (dt_type == 0) {
         auto& btree = btrees_ll[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else if (dt_type == 1) {
         auto& btree = btrees_vw[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(1, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else if (dt_type == 2) {
         auto& btree = btrees_vi[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else {
         ensure(false);
      }
      DTRegistry::global_dt_registry.deserialize(dt_id, serialized_dt_map);
   }
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeFlags()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& flags = d["flags"];
   std::unordered_map<std::string, std::string> flags_serialized;
   for (rs::Value::ConstMemberIterator itr = flags.MemberBegin(); itr != flags.MemberEnd(); ++itr) {
      flags_serialized[itr->name.GetString()] = itr->value.GetString();
   }
   for (auto flags : persistFlagsString()) {
      *std::get<1>(flags) = flags_serialized[std::get<0>(flags)];
   }
   for (auto flags : persistFlagsS64()) {
      *std::get<1>(flags) = atoi(flags_serialized[std::get<0>(flags)].c_str());
   }
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
