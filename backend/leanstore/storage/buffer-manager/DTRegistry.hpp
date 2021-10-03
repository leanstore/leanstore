#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <mutex>
#include <tuple>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
using DTType = u8;
namespace storage
{
// -------------------------------------------------------------------------------------
struct ParentSwipHandler {
   Swip<BufferFrame>& swip;
   Guard parent_guard;
   BufferFrame* parent_bf;
   s32 pos = -2;  // meaning it is the root bf in the dt
   // -------------------------------------------------------------------------------------
   template <typename T>
   HybridPageGuard<T> getParentReadPageGuard()
   {
      return HybridPageGuard<T>(std::move(parent_guard), parent_bf);
   }
};
// -------------------------------------------------------------------------------------
enum class SpaceCheckResult : u8 { NOTHING, PICK_ANOTHER_BF, RESTART_SAME_BF };
// -------------------------------------------------------------------------------------
struct DTRegistry {
   struct DTMeta {
      std::function<void(void*, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>)> iterate_children;
      std::function<ParentSwipHandler(void*, BufferFrame&)> find_parent;
      std::function<SpaceCheckResult(void*, BufferFrame&)> check_space_utilization;
      std::function<void(void* dt_object, BufferFrame& bf, u8* dest)> checkpoint;
      // -------------------------------------------------------------------------------------
      // MVCC / SI
      std::function<void(void* dt_object, const u8* entry, u64 tx_id)> undo;
      std::function<void(void* dt_object, const u8* entry, const u64 version_worker_id, u64 version_tx_id, const bool called_before)> todo;
      std::function<void(void* dt_object, const u8* entry)> unlock;
      // -------------------------------------------------------------------------------------
      // Serialization
      std::function<std::unordered_map<std::string, std::string>(void* btree_boject)> serialize;
      std::function<void(void* btree_boject, std::unordered_map<std::string, std::string>)> deserialize;
   };
   // -------------------------------------------------------------------------------------
   // TODO: synchronize properly
   std::mutex mutex;
   s64 instances_counter = 0;
   std::unordered_map<DTType, DTMeta> dt_types_ht;
   std::unordered_map<u64, std::tuple<DTType, void*, string>> dt_instances_ht;
   static DTRegistry global_dt_registry;
   // -------------------------------------------------------------------------------------
   void registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta);
   DTID registerDatastructureInstance(DTType type, void* root_object, string name);
   void registerDatastructureInstance(DTType type, void* root_object, string name, DTID dt_id);
   // -------------------------------------------------------------------------------------
   void iterateChildrenSwips(DTID dtid, BufferFrame&, std::function<bool(Swip<BufferFrame>&)>);
   ParentSwipHandler findParent(DTID dtid, BufferFrame&);
   SpaceCheckResult checkSpaceUtilization(DTID dtid, BufferFrame&);
   // Pre: bf is shared/exclusive latched
   void checkpoint(DTID dt_id, BufferFrame& bf, u8*);
   // Recovery / SI
   void undo(DTID dt_id, const u8* wal_entry, u64 tts);
   void todo(DTID dt_id, const u8* entry, const u64 version_worker_id, u64 version_tts, const bool called_before);
   void unlock(DTID dt_id, const u8* entry);
   // Serialization
   std::unordered_map<std::string, std::string> serialize(DTID dt_id);
   void deserialize(DTID dt_id, std::unordered_map<std::string, std::string> map);
};

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
