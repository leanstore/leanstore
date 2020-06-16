#pragma once
#include "Config.hpp"
#include "storage/btree/fs/BTreeOptimistic.hpp"
#include "storage/btree/vs/BTreeVS.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
// -------------------------------------------------------------------------------------
constexpr auto btree_size = sizeof(btree::fs::BTree<void*, void*>);
class LeanStore
{
  using statCallback = std::function<void(ostream&)>;
  struct StatEntry {
    string name;
    statCallback callback;
    StatEntry(string&& n, statCallback b) : name(std::move(n)), callback(b) {}
  };
  // -------------------------------------------------------------------------------------
 private:
  // Poor man catalog
  std::unordered_map<string, std::unique_ptr<u8[]>> fs_btrees;
  std::unordered_map<string, btree::vs::BTree> vs_btrees;
  buffermanager::BufferManager buffer_manager;
  // -------------------------------------------------------------------------------------
  std::mutex debugging_mutex; // protect all counters
  void debuggingThread();
  vector<StatEntry> stat_entries, config_entries, dt_entries;
  atomic<u64> bg_threads_counter = 0;
  atomic<bool> bg_threads_keep_running = true;
  u64 config_hash = 0;
  // -------------------------------------------------------------------------------------
 public:
  LeanStore();
  void registerConfigEntry(string name, statCallback b);
  u64 getConfigHash();
  void registerThread(string name);
  // -------------------------------------------------------------------------------------
  template <typename Key, typename Value>
  btree::fs::BTree<Key, Value>& registerFSBTree(string name, DTType type_id = 0)
  {
    // buffer_manager
    auto iter = fs_btrees.emplace(name, std::make_unique<u8[]>(btree_size));
    u8* btree_ptr = iter.first->second.get();
    auto btree = new (btree_ptr) btree::fs::BTree<Key, Value>();
    buffer_manager.registerDatastructureType(type_id, btree->getMeta());
    DTID dtid = buffer_manager.registerDatastructureInstance(type_id, btree, name);
    btree->init(dtid);
    return *btree;
  }
  // -------------------------------------------------------------------------------------
  template <typename Key, typename Value>
  btree::fs::BTree<Key, Value>& retrieveFSBTree(string name, DTType type_id = 0)
  {
    auto btree = reinterpret_cast<btree::fs::BTree<Key, Value>*>(fs_btrees[name].get());
    buffer_manager.registerDatastructureType(type_id, btree->getMeta());
    btree->dtid = buffer_manager.registerDatastructureInstance(type_id, btree, name);
    return *btree;
  }
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
