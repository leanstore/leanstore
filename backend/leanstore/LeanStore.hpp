#pragma once
#include "Config.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/btree/BTreeOptimistic.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore{
// -------------------------------------------------------------------------------------
class LeanStore{
private:
   // Poor man catalog
   std::unordered_map<string, std::unique_ptr<u8[]>> btrees;
   buffermanager::BufferManager buffer_manager;
public:
   Config config;
   LeanStore(Config config = {});
   // -------------------------------------------------------------------------------------
   template<typename Key, typename Value>
   btree::BTree<Key,Value> &registerBTree(string name) {
      //buffer_manager
      constexpr auto btree_size = sizeof(btree::BTree<void*,void*>);
      auto iter = btrees.emplace(name, std::make_unique<u8[]>(btree_size));
      u8 *btree_ptr = iter.first->second.get();
      new(btree_ptr) btree::BTree<Key, Value>(buffer_manager);
      return *reinterpret_cast<btree::BTree<Key,Value> *>(btree_ptr);
   }
   // -------------------------------------------------------------------------------------
   BufferManager &getBufferManager() {
      return buffer_manager;
   }
   ~LeanStore();
};
// -------------------------------------------------------------------------------------
}