#pragma once
#include "BufferFrame.hpp"
#include "DTTypes.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <tuple>
#include <unordered_map>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame>* swip;
   Guard parent_guard;
   BufferFrame* parent_bf;
   s64 pos = -2;  // meaning it is the root bf in the dt
   bool is_bf_updated = false;
   // -------------------------------------------------------------------------------------
   template <typename T>
   HybridPageGuard<T> getParentReadPageGuard()
   {
      return HybridPageGuard<T>(parent_guard, parent_bf);
   }
   void reset() {
      parent_bf = nullptr;
      is_bf_updated = false;
   }
};
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct DTMeta {
      std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
      void (*find_parent)(void*, BufferFrame&, ParentSwipHandler& ph);
      bool (*find_parent_no_jump)(void*, BufferFrame&, ParentSwipHandler& ph);
      std::function<bool(void*, BufferFrame&, OptimisticGuard&, ParentSwipHandler&)> check_space_utilization;
      std::function<void(void* btree_object, BufferFrame& bf, u8* dest)> checkpoint;
      // -------------------------------------------------------------------------------------
      // MVCC / SI
      std::function<void(void* btree_object, const u8* entry, u64 tts)> undo;
      std::function<void(void* btree_object, const u8* entry, u64 tts)> todo;
   };
   // -------------------------------------------------------------------------------------
   // TODO: Not syncrhonized
   std::mutex mutex;
   u64 instances_counter = 0;
   static DTRegistry global_dt_registry;
   // -------------------------------------------------------------------------------------
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void*, string, string>> dt_instances_ht;
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta);
   DTID registerDatastructureInstance(DTType type, void* root_object, string name, string short_name);
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
   // -------------------------------------------------------------------------------------
   inline void findParent(DTID dtid, BufferFrame& bf, ParentSwipHandler& parent_handler)
   {
      auto& dt_meta = dt_instances_ht[dtid];
      dt_types_ht[std::get<0>(dt_meta)].find_parent(std::get<1>(dt_meta), bf, parent_handler);
   }
   inline bool findParentNoJump(DTID dtid, BufferFrame& bf, ParentSwipHandler& parent_handler)
   {
      auto dt_meta_it = dt_instances_ht.find(dtid);
      return dt_types_ht[std::get<0>(dt_meta_it->second)].find_parent_no_jump(std::get<1>(dt_meta_it->second), bf, parent_handler);
   }
   bool checkSpaceUtilization(DTID dtid, BufferFrame&, OptimisticGuard&, ParentSwipHandler&);
   // Pre: bf is shared/exclusive latched
   void checkpoint(DTID dt_id, BufferFrame& bf, u8*);
   // -------------------------------------------------------------------------------------
   // Recovery / SI
   void undo(DTID dt_id, const u8* wal_entry, u64 tts);
   void todo(DTID dt_id, const u8* wal_entry, u64 tts);
};

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
