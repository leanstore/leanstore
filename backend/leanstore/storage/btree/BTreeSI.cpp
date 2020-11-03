#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
s16 BTree::findLatestVerionPositionSI(HybridPageGuard<BTreeNode>& target_guard, u8* k, u16 kl)
{
   u8 key[kl + 8];
   u16 key_length = kl + 8;
   std::memcpy(key, k, kl);
   *reinterpret_cast<u64*>(key + kl) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   findLeaf<OP_TYPE::POINT_UPDATE>(target_guard, key, key_length);
   s16 pos = target_guard->lowerBound<false>(key, key_length);
   if (pos == 0) {
      if (target_guard->lower_fence.offset == 0) {
         // Key does not exist
         return pos;
      } else {
         // Go left
         u16 lower_fence_length = target_guard->lower_fence.length;
         u8 lower_fence[lower_fence_length];
         findLeaf<OP_TYPE::POINT_UPDATE>(target_guard, lower_fence, lower_fence_length);
         pos = target_guard->count - 1;

      }
   }
   return -1;
}
// -------------------------------------------------------------------------------------
bool BTree::lookupSI(u8* k, u16 kl, function<void(const u8*, u16)> payload_callback)
{
   u8 key[kl + 8];
   u16 key_length = kl + 8;
   std::memcpy(key, k, kl);
   *reinterpret_cast<u64*>(key + kl) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   bool found = false;
   u16 version_nr = 0;
   s16 payload_length = -1;
   u8* payload = nullptr;
   scanDesc(
       key, key_length,
       [&](u8* s_key, u16 s_key_length, u8* s_payload, u16 s_payload_length) {
          if (s_key_length != key_length) {
             return false;
          }
          ensure(s_key_length > 8);
          u64 version = *reinterpret_cast<u64*>(s_key + s_key_length - 8);
          if (version_nr == 0) {  // latest version
             payload_length = s_payload_length;
             payload = new u8[payload_length];
             std::memcpy(payload, s_payload, payload_length);
             if (cr::Worker::my().isVisibleForMe(version)) {
                found = true;
                return false;
             }
          } else {
             u16 delta_ptr = 0;
             while (delta_ptr < s_payload_length) {
                const u16 offset = *reinterpret_cast<u16*>(s_payload + delta_ptr);
                delta_ptr += 2;
                const u16 size = *reinterpret_cast<u16*>(s_payload + delta_ptr);
                delta_ptr += 2;
                std::memcpy(payload + offset, s_payload + delta_ptr, size);
                delta_ptr += size;
             }
             if (cr::Worker::my().isVisibleForMe(version)) {
                found = true;
                return false;
             }
          }
          version_nr++;
          return true;
       },
       [&]() {});
   if (payload) {
      payload_callback(payload, payload_length);
      return true;
   } else {
      return false;
   }
}
// -------------------------------------------------------------------------------------
bool BTree::updateSI(u8* k, u16 kl, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key[kl + 8];
   u16 key_length = kl + 8;
   std::memcpy(key, k, kl);
   *reinterpret_cast<u64*>(key + kl) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> c_guard;
         findLeafCanJump<OP_TYPE::POINT_UPDATE>(c_guard, key, key_length);
         auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
         ensure(c_x_guard->count > 0);
         // Assume that max version will not be reached
         // Also assume that no empty page will be left in the system
         assert(c_x_guard->lowerBound<true>(key, key_length) == -1);
         s16 pos = c_x_guard->lowerBound<false>(key, key_length);
         ensure(pos > 0);
         pos -= 1;
         u64 version = *reinterpret_cast<u64*>(c_x_guard->getKey(pos) + kl);
         if (!isVisibleForMe(version)) {
            // TODO: write-write conflict, transaction abort
            ensure(false);
         }
         // -------------------------------------------------------------------------------------
         const u16 payload_length = c_x_guard->getPayloadLength(pos);
         if (!c_x_guard->canInsert(key_length, sizeToVT(payload_length))) {
            c_guard = std::move(c_x_guard);
            c_guard.kill();
            trySplit(*c_guard.bf);
            jumpmu_continue;
         }
         // -------------------------------------------------------------------------------------
         // Enough space in the page
         u16 delta_size = wal_update_generator.entry_size;
         u8 delta[delta_size];
         u8 new_payload[payload_length];
         std::memcpy(new_payload, c_x_guard->getPayload(pos), payload_length);
         wal_update_generator.before(c_x_guard->getPayload(pos), delta);
         c_x_guard->getPayloadLength(delta_size);
         c_x_guard->space_used -= payload_length - delta_size;
         if (FLAGS_wal) {
            // if it is a secondary index, then we can not use updateSameSize
            assert(wal_update_generator.entry_size > 0);
            // -------------------------------------------------------------------------------------
            auto wal_entry = c_x_guard.reserveWALEntry<WALUpdate>(key_length + wal_update_generator.entry_size);
            wal_entry->type = WAL_LOG_TYPE::WALUpdate;
            wal_entry->key_length = key_length;
            std::memcpy(wal_entry->payload, key, key_length);
            std::memcpy(wal_entry->payload + key_length, delta, delta_size);
            wal_entry.submit();
         }
         // The actual update by the client
         callback(new_payload, payload_length);
         *reinterpret_cast<u64*>(key + kl) = myVersion();
         c_x_guard->insert(key, key_length, sizeToVT(payload_length), new_payload);
      }
      jumpmuCatch() {}
   }
   return true;
}
// -------------------------------------------------------------------------------------
bool BTree::insertSI(u8* k, u16 kl, u64 value_length, u8* value)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key[kl + 8];
   u16 key_length = kl + 8;
   std::memcpy(key, k, kl);
   *reinterpret_cast<u64*>(key + kl) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   // while (true) {
   //    jumpmuTry()
   //    {
   //       HybridPageGuard<BTreeNode> c_guard;
   //       findLeafCanJump<OP_TYPE::POINT_UPDATE>(c_guard, key, key_length);
   //       auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
   //       ensure(c_x_guard->count > 0);
   //       // Assume that max version will not be reached
   //       // Also assume that no empty page will be left in the system
   //       assert(c_x_guard->lowerBound<true>(key, key_length) == -1);
   //       s16 pos = c_x_guard->lowerBound<false>(key, key_length);
   //       ensure(pos > 0);
   //       pos -= 1;
   //       u64 version = *reinterpret_cast<u64*>(c_x_guard->getKey(pos) + kl);
   //       if (!isVisibleForMe(version)) {
   //          // TODO: write-write conflict, transaction abort
   //          ensure(false);
   //       }
   //       // -------------------------------------------------------------------------------------
   //       const u16 payload_length = c_x_guard->getPayloadLength(pos);
   //       if (!c_x_guard->canInsert(key_length, sizeToVT(payload_length))) {
   //          c_guard = std::move(c_x_guard);
   //          c_guard.kill();
   //          trySplit(*c_guard.bf);
   //          jumpmu_continue;
   //       }
   //       // -------------------------------------------------------------------------------------
   //       // Enough space in the page
   //       u16 delta_size = wal_update_generator.entry_size;
   //       u8 delta[delta_size];
   //       u8 new_payload[payload_length];
   //       std::memcpy(new_payload, c_x_guard->getPayload(pos), payload_length);
   //       wal_update_generator.before(c_x_guard->getPayload(pos), delta);
   //       c_x_guard->getPayloadLength(delta_size);
   //       c_x_guard->space_used -= payload_length - delta_size;
   //       if (FLAGS_wal) {
   //          // if it is a secondary index, then we can not use updateSameSize
   //          assert(wal_update_generator.entry_size > 0);
   //          // -------------------------------------------------------------------------------------
   //          auto wal_entry = c_x_guard.reserveWALEntry<WALUpdate>(key_length + wal_update_generator.entry_size);
   //          wal_entry->type = WAL_LOG_TYPE::WALUpdate;
   //          wal_entry->key_length = key_length;
   //          std::memcpy(wal_entry->payload, key, key_length);
   //          std::memcpy(wal_entry->payload + key_length, delta, delta_size);
   //          wal_entry.submit();
   //       }
   //       // The actual update by the client
   //       callback(new_payload, payload_length);
   //       *reinterpret_cast<u64*>(key + kl) = myVersion();
   //       c_x_guard->insert(key, key_length, sizeToVT(payload_length), new_payload);
   //    }
   //    jumpmuCatch() {}
   // }
   return true;
}
// -------------------------------------------------------------------------------------
bool BTree::removeSI(u8* k, u16 kl) {}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
