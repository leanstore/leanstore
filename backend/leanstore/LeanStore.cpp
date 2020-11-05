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
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>

#include <sstream>
// -------------------------------------------------------------------------------------
using namespace tabulate;
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   // Set the default logger to file logger
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
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
   buffer_manager->registerDatastructureType(99, storage::btree::BTree::getMeta());
   // -------------------------------------------------------------------------------------
   u64 end_of_block_device;
   if (FLAGS_wal_offset == 0) {
      ioctl(ssd_fd, BLKGETSIZE64, &end_of_block_device);
   } else {
      end_of_block_device = FLAGS_wal_offset * 1024 * 1024 * 1024;
   }
   cr_manager = make_unique<cr::CRManager>(ssd_fd, end_of_block_device);
}
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
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([&]() {
      profiling::BMTable bm_table(*buffer_manager.get());
      profiling::DTTable dt_table(*buffer_manager.get());
      profiling::CPUTable cpu_table;
      profiling::CRTable cr_table;
      std::vector<profiling::ProfilingTable*> tables = {&configs_table, &bm_table, &dt_table, &cpu_table, &cr_table};
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
         const u64 tx = std::stoi(bm_table.get("0", "tx"));
         // Global Stats
         global_stats.accumulated_tx_counter += tx;
         // -------------------------------------------------------------------------------------
         // Console
         // -------------------------------------------------------------------------------------
         const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
         // using RowType = std::vector<variant<std::string, const char*, Table>>;
         {
            tabulate::Table table;
            table.add_row({"t", "TX P", "TX C", "w_mib", "r_mib", "instr_tx", "workers_cpus", "GCT W%", "GCT 1%", "GCT 2%", "GCT GiB", "GCT Rounds"});
            table.add_row({std::to_string(seconds), bm_table.get("0", "tx"), cr_table.get("0", "gct_committed_tx"), bm_table.get("0", "w_mib"),
                           bm_table.get("0", "r_mib"), std::to_string(instr_per_tx), std::to_string(cpu_table.workers_agg_events["CPU"]),
                           cr_table.get("0", "gct_write_pct"), cr_table.get("0", "gct_phase_1_pct"), cr_table.get("0", "gct_phase_2_pct"),
                           cr_table.get("0", "gct_write_gib"), cr_table.get("0", "gct_rounds")});
            // -------------------------------------------------------------------------------------
            table.format().width(10);
            table.column(0).format().width(5);
            table.column(1).format().width(10);
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
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            seconds += 1;
         }
      }
      bg_threads_counter--;
   });
   bg_threads_counter++;
   profiling_thread.detach();
}
// -------------------------------------------------------------------------------------
storage::btree::BTree& LeanStore::registerBTree(string name)
{
   assert(btrees.find(name) == btrees.end());
   auto& btree = btrees[name];
   DTID dtid = buffer_manager->registerDatastructureInstance(99, reinterpret_cast<void*>(&btree), name);
   auto& bf = buffer_manager->allocatePage();
   Guard guard(bf.header.latch, GUARD_STATE::EXCLUSIVE);
   bf.header.keep_in_memory = true;
   bf.page.dt_id = dtid;
   guard.unlock();
   btree.create(dtid, &bf);
   return btree;
}
// -------------------------------------------------------------------------------------
storage::btree::BTree& LeanStore::retrieveBTree(string name)
{
   return btrees[name];
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
