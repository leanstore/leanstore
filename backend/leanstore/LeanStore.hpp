#pragma once
#include "Config.hpp"
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
  struct StatEntry {
    string name;
    statCallback callback;
    StatEntry(string&& n, statCallback b) : name(std::move(n)), callback(b) {}
  };
  struct GlobalStats {
    u64 accumulated_tx_counter = 0;
  };
  // -------------------------------------------------------------------------------------
 private:
  // Poor man catalog
  std::unordered_map<string, btree::vs::BTree> vs_btrees;
  buffermanager::BufferManager buffer_manager;
  // -------------------------------------------------------------------------------------
  std::mutex debugging_mutex; // protect all counters
  void debuggingThread();
  vector<StatEntry> stat_entries, config_entries, dt_entries;
  atomic<u64> bg_threads_counter = 0;
  atomic<bool> bg_threads_keep_running = true;
  u64 config_hash = 0;
  GlobalStats global_stats;
  // -------------------------------------------------------------------------------------
 public:
  LeanStore();
  // -------------------------------------------------------------------------------------
  void registerConfigEntry(string name, statCallback b);
  u64 getConfigHash();
  GlobalStats getGlobalStats();
  void registerThread(string name);
  // -------------------------------------------------------------------------------------
  btree::vs::BTree& registerVSBTree(string name);
  btree::vs::BTree& retrieveVSBTree(string name);
  // -------------------------------------------------------------------------------------
  BufferManager& getBufferManager() { return buffer_manager; }
  // -------------------------------------------------------------------------------------
  void startDebuggingThread();
  void persist();
  void restore();
  // -------------------------------------------------------------------------------------
  ~LeanStore();
};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
