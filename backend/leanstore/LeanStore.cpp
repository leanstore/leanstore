#include "LeanStore.hpp"

#include "Time.hpp"
#include "leanstore/io/IoChannel.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/profiling/tables/SSDTable.hpp"
#include "leanstore/profiling/tables/ThreadTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/Swip.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "leanstore/concurrency/Mean.hpp"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <chrono>
#include <locale>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace tabulate;
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   // Set the default logger to file logger
   // -------------------------------------------------------------------------------------
   buffer_manager = make_unique<storage::BufferManager>();
   BMC::global_bf = buffer_manager.get();
   DTRegistry::global_dt_registry.registerDatastructureType(0, storage::btree::BTreeLL::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(1, storage::btree::BTreeVW::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(2, storage::btree::BTreeVI::getMeta());
   // -------------------------------------------------------------------------------------
   // u64 end_of_block_device;
   // if (FLAGS_wal_offset_gib == 0) {
   //    end_of_block_device = mean::IoInterface::instance().storageSize();
   // } else {
   //    end_of_block_device = FLAGS_wal_offset_gib * 1024 * 1024 * 1024;
   // }
   // cr_manager = make_unique<cr::CRManager>(ssd_fd, end_of_block_device);
   // cr::CRManager::global = cr_manager.get();
}  // namespace leanstore
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
      MYPAUSE();
   }
   //  close(ssd_fd);
}
// -------------------------------------------------------------------------------------
void LeanStore::printObjStats() {
   int sample_size = 1000;

   std::map<DTID, int> leafTypes;
   std::map<DTID, int> innerTypes;
   std::map<int, int> states;
   for (auto& t: DTRegistry::global_dt_registry.dt_instances_ht) {
      leafTypes[t.first] = 0;
      innerTypes[t.first] = 0;
   }
   for (int i = 0; i < (int)BufferFrame::STATE::COUNT; i++) {
      leafTypes[i] = 0;
   }

   for (int i = 0; i < sample_size; i++) {
      auto& bf =  buffer_manager->randomBufferFrame();
      // this cast is a bit... hacky
      auto& c_node = *reinterpret_cast<storage::btree::BTreeNode*>(bf.page.dt);
      if (c_node.is_leaf) {
         leafTypes[bf.page.dt_id]++;
      } else {
         innerTypes[bf.page.dt_id]++;
      }
      states[(int)bf.header.state]++;
   }
   std::cout << "";
   for (auto& h: innerTypes) {
      int percent = (int)((float)h.second / sample_size * 100);
      for (int i = 0; i < percent; i++) {
         std::cout << (char)std::toupper(std::get<2>(DTRegistry::global_dt_registry.dt_instances_ht[h.first])[0]);
      }
   }
   std::cout << "|";
   for (auto& h: leafTypes) {
      int percent = (int)((float)h.second / sample_size * 100);
      for (int i = 0; i < percent; i++) {
         std::cout << std::get<3>(DTRegistry::global_dt_registry.dt_instances_ht[h.first]) ;
      }
   }
   std::cout << std::endl;
   for (int s = 0; s < (int)BufferFrame::STATE::COUNT; s++) {
      int percent = (int)((float)states[s] / sample_size * 100);
      for (int i = 0; i < percent; i++) {
         std::cout << s;
      }
   }
   std::cout << std::endl;
}
// -------------------------------------------------------------------------------------
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([&]() {
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(63, &cpuset);
      auto thread = pthread_self();
      int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
      if (s != 0) {
         ensure(false, "Affinity could not be set.");
      }
      s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
      if (s != 0) {
         ensure(false, "Affinity could not be set.");
      }

      posix_check(pthread_setname_np(pthread_self(), "profiling") == 0);
      // -------------------------------------------------------------------------------------
      profiling::BMTable bm_table(*buffer_manager.get());
      profiling::DTTable dt_table(*buffer_manager.get());
      profiling::CPUTable cpu_table;
      profiling::CRTable cr_table;
      profiling::SSDTable ssd_table;
      profiling::ThreadTable thread_table;
      std::vector<profiling::ProfilingTable*> tables = {&configs_table, &bm_table, &dt_table, &cpu_table, &cr_table, &ssd_table, &thread_table};
      // -------------------------------------------------------------------------------------
      std::vector<std::ofstream> csvs;
      std::ofstream::openmode open_flags;
      if (FLAGS_csv_truncate) {
         open_flags = ios::trunc;
      } else {
         open_flags = ios::app;
      }
      for (u64 t_i = 0; t_i < tables.size(); t_i++) {
         tables[t_i]->open();
         // -------------------------------------------------------------------------------------
         csvs.emplace_back();
         auto& csv = csvs.back();
         csv.open(FLAGS_csv_path + "_" + tables[t_i]->getName() + ".csv", open_flags);
         csv.seekp(0, ios::end);
         csv << std::setprecision(2) << std::fixed;
         if (csv.tellp() == 0) {
            csv << "t,c_hash";
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.first;
            }
            csv << endl;
         }
      }
      // -------------------------------------------------------------------------------------
      config_hash = configs_table.hash();
      // -------------------------------------------------------------------------------------
      u64 seconds = 0;
      auto lastTime = mean::getTimePoint();
      auto lastTimePrint = mean::getTimePoint();
      while (bg_threads_keep_running) {
         for (u64 t_i = 0; t_i < tables.size(); t_i++) {
            tables[t_i]->next();
            if (tables[t_i]->size() == 0)
               continue;
            // -------------------------------------------------------------------------------------
            // CSV
            auto& csv = csvs[t_i];
            for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
               csv << seconds << "," << config_hash;
               for (auto& c : tables[t_i]->getColumns()) {
                  csv << "," << c.second.values[r_i];
               }
               csv << endl;
            }
            // -------------------------------------------------------------------------------------
            // TODO: Websocket, CLI
         }
         // -------------------------------------------------------------------------------------
         const u64 tx = std::stoi(cr_table.get("0", "tx"));
         // Global Stats
         global_stats.accumulated_tx_counter += tx;
         // -------------------------------------------------------------------------------------
         // Console
         // -------------------------------------------------------------------------------------
         const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
         const double cycles_per_tx = cpu_table.workers_agg_events["cycle"] / tx;
         const double l1_per_tx = cpu_table.workers_agg_events["L1-miss"] / tx;
         // using RowType = std::vector<variant<std::string, const char*, Table>>;
         if (FLAGS_print_tx_console) {
            
            tabulate::Table table;
            table.add_row({"t", "wt","TX P [M]", "TX A", "TX C", "W MiB", "R MiB", "Instrs/TX", "Cycles/TX", "CPUs", "L1/TX", "WAL T", "WAL R G", "WAL W G",
                           "GCT Rounds"});
            table.add_row({std::to_string(seconds),
                           std::to_string(mean::timePointDifferenceMs(mean::getTimePoint(), lastTimePrint)/(float)1000),
                           std::to_string(stol(cr_table.get("0", "tx"))/(float)1000/1000), cr_table.get("0", "tx_abort"), cr_table.get("0", "gct_committed_tx"),
                           bm_table.get("0", "w_mib"), bm_table.get("0", "r_mib"), std::to_string(instr_per_tx), std::to_string(cycles_per_tx),
                           std::to_string(cpu_table.workers_agg_events["CPU"]), std::to_string(l1_per_tx), cr_table.get("0", "wal_total"),
                           cr_table.get("0", "wal_read_gib"), cr_table.get("0", "wal_write_gib"), cr_table.get("0", "gct_rounds")});
            lastTimePrint = mean::getTimePoint();
            // -------------------------------------------------------------------------------------
            table.format().width(10);
            table.column(0).format().width(6);
            table.column(1).format().width(10);
            table.column(2).format().width(10);
            // -------------------------------------------------------------------------------------
            auto print_table = [](tabulate::Table& table, std::function<bool(u64)> predicate) {
               std::stringstream ss;
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
            if (FLAGS_print_obj_stats) {
               std::cout << std::endl;
               printObjStats();
            }
         }
         auto now = mean::getTimePoint();
         auto diff = mean::timePointDifferenceUs(now, lastTime);
         std::this_thread::sleep_for(std::chrono::microseconds(1000*1000 - diff));
         seconds += 1;
         std::locale::global(std::locale::classic());
         lastTime = mean::getTimePoint();
      }
      bg_threads_counter--;
   });
   bg_threads_counter++;
   profiling_thread.detach();
}
// -------------------------------------------------------------------------------------
storage::btree::BTreeLL& LeanStore::registerBTreeLL(string name, string short_name)
{
   assert(btrees_ll.find(name) == btrees_ll.end());
   auto& btree = btrees_ll[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), name, short_name);
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);
   return btree;
}

// -------------------------------------------------------------------------------------
storage::btree::BTreeVW& LeanStore::registerBTreeVW(string name, string short_name)
{
   assert(btrees_vw.find(name) == btrees_vw.end());
   auto& btree = btrees_vw[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(1, reinterpret_cast<void*>(&btree), name, short_name);
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);
   return btree;
}
// -------------------------------------------------------------------------------------
storage::btree::BTreeVI& LeanStore::registerBTreeVI(string name, string short_name)
{
   assert(btrees_vi.find(name) == btrees_vi.end());
   auto& btree = btrees_vi[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), name, short_name);
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);
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
void LeanStore::persist()
{
   // TODO
}
// -------------------------------------------------------------------------------------
void LeanStore::restore()
{
   // TODO
}
// -------------------------------------------------------------------------------------
}  // namespace leanstore
// -------------------------------------------------------------------------------------
