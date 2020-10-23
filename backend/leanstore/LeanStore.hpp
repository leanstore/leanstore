#pragma once
#include "Config.hpp"
#include "concurrency-recovery/WALWriter.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "storage/btree/BTreeVS.hpp"
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
   std::unordered_map<string, storage::btree::BTree> btrees;
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
   // -------------------------------------------------------------------------------------
   template <typename T>
   void registerConfigEntry(string name, T value)
   {
      configs_table.add(name, std::to_string(value));
   }
   u64 getConfigHash();
   GlobalStats getGlobalStats();
   // -------------------------------------------------------------------------------------
   storage::btree::BTree& registerBTree(string name);
   storage::btree::BTree& retrieveBTree(string name);
   // -------------------------------------------------------------------------------------
  storage::BufferManager& getBufferManager() { return *buffer_manager; }
   cr::CRManager& getCRManager() { return *cr_manager; }
   // -------------------------------------------------------------------------------------
   void startProfilingThread();
   void persist();
   void restore();
   // -------------------------------------------------------------------------------------
   ~LeanStore();
};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
