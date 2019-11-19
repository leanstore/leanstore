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
constexpr auto btree_size = sizeof(btree::BTree<void*,void*>);
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
   btree::BTree<Key,Value> &registerBTree(string name, DTType type_id = 0) {
      //buffer_manager
      auto iter = btrees.emplace(name, std::make_unique<u8[]>(btree_size));
      u8 *btree_ptr = iter.first->second.get();
      auto btree = new(btree_ptr) btree::BTree<Key, Value>(buffer_manager);
      buffer_manager.registerDatastructureType(type_id, btree->getMeta());
      btree->dtid = buffer_manager.registerDatastructureInstance(type_id, btree);
      btree->init();
      return *btree;
   }
   // -------------------------------------------------------------------------------------
   template<typename Key, typename Value>
   btree::BTree<Key,Value> &retrieveBTree(string name, DTType type_id = 0) {
      auto btree = reinterpret_cast<btree::BTree<Key,Value> *>(btrees[name].get());
      buffer_manager.registerDatastructureType(type_id, btree->getMeta());
      btree->dtid = buffer_manager.registerDatastructureInstance(type_id, btree);
      return *btree;
   }
   // -------------------------------------------------------------------------------------
   BufferManager &getBufferManager() {
      return buffer_manager;
   }
   // -------------------------------------------------------------------------------------
   void persist();
   void restore();
   // -------------------------------------------------------------------------------------
   ~LeanStore();
};
// -------------------------------------------------------------------------------------
}