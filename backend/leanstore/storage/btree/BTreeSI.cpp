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
         return pos;
      }
   } else {
      return pos - 1;
   }
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
   s16 payload_length = -1;
   std::unique_ptr<u8[]> payload(nullptr);
   scanDesc(
       key, key_length,
       [&](u8* s_key, u16 s_key_length, u8* s_payload, u16 s_payload_length) {
          if (s_key_length != key_length) {
             return false;
          }
          ensure(s_key_length > 8);
          const u64 version = *reinterpret_cast<u64*>(s_key + s_key_length - 8);
          if (s_payload_length == 0) {
             if (isVisibleForMe(version)) {
                // it has been deleted
                jumpmu_return false;
             }
          } else {
             // complete tuple or delta
             if (payload == nullptr) {
                payload_length = s_payload_length;
                payload = std::make_unique<u8[]>(payload_length);
                std::memcpy(payload.get(), s_payload, payload_length);
             } else {
                // Apply delta
                u16 delta_ptr = 0;
                while (delta_ptr < s_payload_length) {
                   const u16 offset = *reinterpret_cast<u16*>(s_payload + delta_ptr);
                   delta_ptr += 2;
                   const u16 size = *reinterpret_cast<u16*>(s_payload + delta_ptr);
                   delta_ptr += 2;
                   std::memcpy(payload.get() + offset, s_payload + delta_ptr, size);
                   delta_ptr += size;
                }
             }
             if (isVisibleForMe(version)) {
                found = true;
                return false;
             }
          }
          return true;
       },
       [&]() {});
   if (found) {
      payload_callback(payload.get(), payload_length);
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
   // Four possible scenarios:
   // 1) key not found -> return false
   // 2) key found, version not visible -> abort transaction
   // 3) key found, version visible -> insert delta record (write delta to the latest visible version and insert the new tuple)
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         s16 pos = findLatestVerionPositionSI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            const u64 version = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
            if (!isVisibleForMe(version)) {
               ensure(false);  // TODO: abort tx
            }
            if (ex_leaf->getPayloadLength(pos) == 0) {
               // deleted
               jumpmu_return false;
            }
            const u16 full_payload_length = ex_leaf->getPayloadLength(pos);
            if (ex_leaf->canInsert(key_length, full_payload_length)) {
               u16 delta_size = wal_update_generator.entry_size;
               u8 updated_payload[full_payload_length];
               std::memcpy(updated_payload, ex_leaf->getPayload(pos), full_payload_length);
               wal_update_generator.before(updated_payload, ex_leaf->getPayload(pos));
               ex_leaf->space_used -= full_payload_length - delta_size;
               ex_leaf->setPayloadLength(pos, delta_size);
               // -------------------------------------------------------------------------------------
               callback(updated_payload, full_payload_length);
               *reinterpret_cast<u64*>(key + kl) = myVersion();
               ex_leaf->insert(key, key_length, updated_payload, full_payload_length);
               // -------------------------------------------------------------------------------------
               // TODO: WAL
               // -------------------------------------------------------------------------------------
               jumpmu_return true;
            } else {
               leaf = std::move(ex_leaf);
               leaf.kill();
               trySplit(*leaf.bf);
            }
         } else {
            jumpmu_return false;
         }
      }
      jumpmuCatch() {}
   }
   ensure(false);
   return false;
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
   // Four possible scenarios:
   // 1) Nothing inserted -> insert
   // 2) Not visible delete or version -> write-write conflict -> abort
   // 3) Visible version -> return false (primary key violation)
   // 4) Visible delete -> insert
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         s16 pos = findLatestVerionPositionSI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            // key exists, check version
            u64 delete_tts = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
            if (!isVisibleForMe(delete_tts)) {
               // TODO: 2) write-write conflict -> tx abort
               ensure(false);
            }
            if (ex_leaf->getPayloadLength(pos) == 0) {
               // was deleted
            } else {
               // 3) not deleted!
               jumpmu_return false;
            }
         }
         if (ex_leaf->canInsert(key_length, value_length)) {
            // 1+4)
            *reinterpret_cast<u64*>(key + kl) = myVersion();
            ex_leaf->insert(key, key_length, value, value_length);
            if (FLAGS_wal) {
               auto wal_entry = ex_leaf.reserveWALEntry<WALInsert>(key_length + value_length);
               wal_entry->type = WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = key_length;
               wal_entry->value_length = value_length;
               std::memcpy(wal_entry->payload, key, key_length);
               std::memcpy(wal_entry->payload + key_length, value, value_length);
               wal_entry.submit();
            }
            jumpmu_return true;
         } else {
            leaf = std::move(ex_leaf);
            leaf.kill();
            trySplit(*leaf.bf);
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
bool BTree::removeSI(u8* k_wo_version, u16 kl_wo_version)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   u8 key[kl_wo_version + 8];
   u16 key_length = kl_wo_version + 8;
   std::memcpy(key, k_wo_version, kl_wo_version);
   *reinterpret_cast<u64*>(key + kl_wo_version) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         s16 pos = findLatestVerionPositionSI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            // key exists, check version
            if (ex_leaf->getPayloadLength(pos) == 0) {
               u64 delete_tts = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
               if (isVisibleForMe(delete_tts)) {
                  // Already deleted
                  jumpmu_return false;
               } else {
                  // TODO: tx abort
                  ensure(false);
               }
            } else {
               // Insert delete version
               if (ex_leaf->canInsert(key_length, 0)) {
                  *reinterpret_cast<u64*>(key + kl_wo_version) = myVersion();
                  ex_leaf->insert(key, key_length, nullptr, 0);
                  if (FLAGS_wal) {
                     // TODO:
                  }
                  jumpmu_return true;
               } else {
                  leaf = std::move(ex_leaf);
                  leaf.kill();
                  trySplit(*leaf.bf);
                  jumpmu_return false;
               }
            }
         } else {
            // entry not found
            jumpmu_return false;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
