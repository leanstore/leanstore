#pragma once
#include "Config.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BTreeLL.hpp"
#include "storage/btree/BTreeVI.hpp"
#include "storage/btree/BTreeVW.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
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
   std::unordered_map<string, storage::btree::BTreeVW> btrees_vw;
   std::unordered_map<string, storage::btree::BTreeVI> btrees_vi;
   // -------------------------------------------------------------------------------------
   s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   unique_ptr<cr::CRManager> cr_manager;
   unique_ptr<storage::BufferManager> buffer_manager;
   // -------------------------------------------------------------------------------------
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   profiling::ConfigsTable configs_table;
   u64 config_hash = 0;
   GlobalStats global_stats;
   // -------------------------------------------------------------------------------------
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
   storage::btree::BTreeLL& registerBTreeLL(string name);
   storage::btree::BTreeLL& retrieveBTreeLL(string name) { return btrees_ll[name]; }
   storage::btree::BTreeVW& registerBTreeVW(string name);
   storage::btree::BTreeVW& retrieveBTreeVW(string name) { return btrees_vw[name]; }
   storage::btree::BTreeVI& registerBTreeVI(string name);
   storage::btree::BTreeVI& retrieveBTreeVI(string name) { return btrees_vi[name]; }
   // -------------------------------------------------------------------------------------
   storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::CRManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread();
   // -------------------------------------------------------------------------------------
   static std::list<std::tuple<string, fLS::clstring*>>& persistFlagsString()
   {
      static std::list<std::tuple<string, fLS::clstring*>> list = {};
      return list;
   };
   static std::list<std::tuple<string, s64*>>& persistFlagsS64()
   {
      static std::list<std::tuple<string, s64*>> list = {};
      return list;
   };
   static void addStringFlag(string name, fLS::clstring* flag) { LeanStore::persistFlagsString().push_back(std::make_tuple(name, flag)); }
   static void addS64Flag(string name, s64* flag) { LeanStore::persistFlagsS64().push_back(std::make_tuple(name, flag)); }

  private:
   void serializeState();
   void deserializeState();
};

// -------------------------------------------------------------------------------------
}  // namespace leanstore
