#pragma once
#include "Config.hpp"
#include "storage/buffer-manager/BufferManager.hpp"
#include "storage/btree/fs/BTreeOptimistic.hpp"
#include "storage/btree/vs/BTreeVS.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore{
// -------------------------------------------------------------------------------------
constexpr auto btree_size = sizeof(btree::fs::BTree<void*,void*>);
class LeanStore{
private:
   // Poor man catalog
   std::unordered_map<string, std::unique_ptr<u8[]>> fs_btrees;
   std::unordered_map<string, btree::vs::BTree> vs_btrees;
   buffermanager::BufferManager buffer_manager;
public:
   LeanStore();
   // -------------------------------------------------------------------------------------
   template<typename Key, typename Value>
   btree::fs::BTree<Key,Value> &registerFSBTree(string name, DTType type_id = 0) {
      //buffer_manager
      auto iter = fs_btrees.emplace(name, std::make_unique<u8[]>(btree_size));
      u8 *btree_ptr = iter.first->second.get();
      auto btree = new(btree_ptr) btree::fs::BTree<Key, Value>();
      buffer_manager.registerDatastructureType(type_id, btree->getMeta());
      DTID dtid = buffer_manager.registerDatastructureInstance(type_id, btree);
      btree->init(dtid);
      return *btree;
   }
   // -------------------------------------------------------------------------------------
   template<typename Key, typename Value>
   btree::fs::BTree<Key,Value> &retrieveFSBTree(string name, DTType type_id = 0) {
      auto btree = reinterpret_cast<btree::fs::BTree<Key,Value> *>(fs_btrees[name].get());
      buffer_manager.registerDatastructureType(type_id, btree->getMeta());
      btree->dtid = buffer_manager.registerDatastructureInstance(type_id, btree);
      return *btree;
   }
   // -------------------------------------------------------------------------------------
   btree::vs::BTree &registerVSBTree(string name);
   btree::vs::BTree &retrieveVSBTree(string name);
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