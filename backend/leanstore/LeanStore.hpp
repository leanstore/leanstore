#pragma once
#include "Config.hpp"
#include "leanstore/concurrency-recovery/HistoryTree.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/storage/btree/BTreeVI.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "rapidjson/document.h"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
class LeanStore
{
   using statCallback = std::function<void(ostream&)>;
   struct GlobalStats {
      u64 accumulated_tx_counter = 0;
   };
   // -------------------------------------------------------------------------------------
  public:
   // Poor man catalog
   std::unordered_map<string, storage::btree::BTreeLL> btrees_ll;
   std::unordered_map<string, storage::btree::BTreeVI> btrees_vi;
   // -------------------------------------------------------------------------------------
   s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   unique_ptr<storage::BufferManager> buffer_manager;
   unique_ptr<cr::CRManager> cr_manager;
   // -------------------------------------------------------------------------------------
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   profiling::ConfigsTable configs_table;
   u64 config_hash = 0;
   GlobalStats global_stats;
   // -------------------------------------------------------------------------------------
   std::unique_ptr<cr::HistoryTree> history_tree;
   // -------------------------------------------------------------------------------------
   void persist(string key, string value);
   string recover(string key, string default_value);
  private:
   static std::list<std::tuple<string, fLS::clstring*>> persisted_string_flags;
   static std::list<std::tuple<string, s64*>> persisted_s64_flags;
   std::unordered_map<string, string> persist_values;
   void serializeFlags(rapidjson::Document& d);
   void deserializeFlags();
   void serializeState();
   void deserializeState();

  public:
   LeanStore();
   ~LeanStore();
   // -------------------------------------------------------------------------------------
   template <typename T>
   void registerConfigEntry(string name, T value)
   {
      configs_table.add(name, std::to_string(value));
   }
   u64 getConfigHash();
   GlobalStats getGlobalStats();
   // -------------------------------------------------------------------------------------
   storage::btree::BTreeLL& registerBTreeLL(string name, const storage::btree::BTreeLL::Config config);
   storage::btree::BTreeLL& retrieveBTreeLL(string name) { return btrees_ll[name]; }
   storage::btree::BTreeVI& registerBTreeVI(string name, const storage::btree::BTreeLL::Config config);
   storage::btree::BTreeVI& retrieveBTreeVI(string name)
   {
      auto& btree_vi = btrees_vi[name];
      if (btree_vi.graveyard == nullptr) {
         auto& graveyard_btree = registerBTreeLL("_" + name + "_graveyard", {.enable_wal = false, .use_bulk_insert = false});
         btree_vi.graveyard = &graveyard_btree;
      }
      return btree_vi;
   }
   // -------------------------------------------------------------------------------------
   storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::CRManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread();
   // -------------------------------------------------------------------------------------
   static void addStringFlag(string name, fLS::clstring* flag) { persisted_string_flags.push_back(std::make_tuple(name, flag)); }
   static void addS64Flag(string name, s64* flag) { persisted_s64_flags.push_back(std::make_tuple(name, flag)); }
  private:
   static void printStats(bool reset = true);
   void doProfiling();
   void printTable(profiling::ProfilingTable* table, basic_ofstream<char>& csv, u64 seconds, bool print_seconds = true) const;
   void print_tx_console(profiling::BMTable& bm_table,
                         profiling::CPUTable& cpu_table,
                         profiling::CRTable& cr_table,
                         u64 seconds,
                         const u64 tx,
                         ofstream& console_csv) const;
   void prepareCSV(profiling::ProfilingTable* table, ofstream& csv, bool print_seconds = true) const;
};

// -------------------------------------------------------------------------------------
}  // namespace leanstore
