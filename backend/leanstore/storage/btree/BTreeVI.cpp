#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::BTree::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
namespace vi
{
enum class WAL_LOG_TYPE : u8 { WALInsert, WALUpdate, WALRemove, WALAfterBeforeImage, WALAfterImage, WALLogicalSplit, WALInitPage };
struct WALEntry {
   WAL_LOG_TYPE type;
};
struct WALBeforeAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
struct WALInsert : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
struct WALUpdate : WALEntry {
   u16 key_length;
   u8 payload[];
};
struct WALRemove : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
}  // namespace nocc
// -------------------------------------------------------------------------------------
s16 BTree::findLatestVersionPositionVI(HybridPageGuard<BTreeNode>& target_guard, u8* k, u16 kl)
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
OP_RESULT BTree::lookupVI(u8* k, u16 kl, function<void(const u8*, u16)> payload_callback)
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
      return OP_RESULT::OK;
   } else {
      return OP_RESULT::NOT_FOUND;
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::updateVI(u8* k, u16 kl, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator wal_update_generator)
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
         s16 pos = findLatestVersionPositionVI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            const u64 version = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
            if (!isVisibleForMe(version)) {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
            if (ex_leaf->getPayloadLength(pos) == 0) {
               // deleted
               jumpmu_return OP_RESULT::NOT_FOUND;
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
               *reinterpret_cast<u64*>(key + kl) = myTTS();
               ex_leaf->insert(key, key_length, updated_payload, full_payload_length);
               // -------------------------------------------------------------------------------------
               // TODO: WAL
               // -------------------------------------------------------------------------------------
               jumpmu_return OP_RESULT::OK;
            } else {
               leaf = std::move(ex_leaf);
               leaf.kill();
               trySplit(*leaf.bf);
            }
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
   ensure(false);
   jumpmu_return OP_RESULT::NOT_FOUND;
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::insertVI(u8* k, u16 kl, u16 value_length, u8* value)
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
         s16 pos = findLatestVersionPositionVI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            // key exists, check version
            u64 delete_tts = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
            if (!isVisibleForMe(delete_tts)) {
               // TODO: 2) write-write conflict -> tx abort
               jumpmu_return OP_RESULT::ABORT_TX;
            }
            if (ex_leaf->getPayloadLength(pos) == 0) {
               // was deleted
            } else {
               // 3) not deleted!
               jumpmu_return OP_RESULT::DUPLICATE;
            }
         }
         if (ex_leaf->canInsert(key_length, value_length)) {
            // 1+4)
            *reinterpret_cast<u64*>(key + kl) = myTTS();
            ex_leaf->insert(key, key_length, value, value_length);
            if (FLAGS_wal) {
               auto wal_entry = ex_leaf.reserveWALEntry<vi::WALInsert>(key_length + value_length);
               wal_entry->type = vi::WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = kl;
               wal_entry->value_length = value_length;
               std::memcpy(wal_entry->payload, k, kl);
               std::memcpy(wal_entry->payload + key_length, value, value_length);
               wal_entry.submit();
            }
            jumpmu_return OP_RESULT::OK;
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
OP_RESULT BTree::removeVI(u8* k_wo_version, u16 kl_wo_version)
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
         s16 pos = findLatestVersionPositionVI(leaf, key, key_length);
         ExclusivePageGuard ex_leaf(std::move(leaf));
         if (ex_leaf->getFullKeyLen(pos) == key_length &&
             ex_leaf->cmpKeys(ex_leaf->getKey(pos), key + ex_leaf->prefix_length, ex_leaf->getKeyLen(pos) - 8,
                              key_length - ex_leaf->prefix_length - 8) == 0) {
            // key exists, check version
            if (ex_leaf->getPayloadLength(pos) == 0) {
               u64 delete_tts = *reinterpret_cast<u64*>(ex_leaf->getKey(pos) + ex_leaf->getKeyLen(pos) - 8);
               if (isVisibleForMe(delete_tts)) {
                  // Already deleted
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  jumpmu_return OP_RESULT::ABORT_TX;
               }
            } else {
               // Insert delete version
               if (ex_leaf->canInsert(key_length, 0)) {
                  *reinterpret_cast<u64*>(key + kl_wo_version) = myTTS();
                  ex_leaf->insert(key, key_length, nullptr, 0);
                  if (FLAGS_wal) {
                     // TODO:
                  }
                  jumpmu_return OP_RESULT::OK;
               } else {
                  leaf = std::move(ex_leaf);
                  leaf.kill();
                  trySplit(*leaf.bf);
                  jumpmu_continue;
               }
            }
         } else {
            // entry not found
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTree::undoVI(void* btree_object, const u8* wal_entry_ptr, const u64 tts)
{
   auto& btree = *reinterpret_cast<BTree*>(btree_object);
   const vi::WALEntry& entry = *reinterpret_cast<const vi::WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case vi::WAL_LOG_TYPE::WALInsert: {
         auto& insert_entry = *reinterpret_cast<const vi::WALInsert*>(&entry);
         u16 key_length = insert_entry.key_length + 8;
         u8 key[key_length];
         std::memcpy(key, insert_entry.payload, insert_entry.key_length);
         *reinterpret_cast<u64*>(key + insert_entry.key_length) = tts;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(c_guard, key, key_length);
               auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
               const bool ret = c_x_guard->remove(key, key_length);
               ensure(ret);
               // -------------------------------------------------------------------------------------
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      case vi::WAL_LOG_TYPE::WALUpdate: {
         const auto& update_entry = *reinterpret_cast<const vi::WALUpdate*>(&entry);
         u16 v_key_length = update_entry.key_length + 8;
         u8 v_key[v_key_length];
         std::memcpy(v_key, update_entry.payload, update_entry.key_length);
         *reinterpret_cast<u64*>(v_key + update_entry.key_length) = tts;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(c_guard, v_key, v_key_length);
               auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
               const s16 pos = c_x_guard->lowerBound<true>(v_key, v_key_length);
               ensure(pos > 0);
               u16 old_payload_length = c_x_guard->getPayloadLength(pos);
               u8 old_payload[old_payload_length];
               // -------------------------------------------------------------------------------------
               // Apply delta
               const s16 delta_pos = pos - 1;
               const u16 delta_size = c_x_guard->getPayloadLength(delta_pos);
               u8* delta_beginning = c_x_guard->getPayload(delta_pos);
               applyDelta(old_payload, delta_beginning, delta_size);
               // -------------------------------------------------------------------------------------
               const u64 delta_version = *reinterpret_cast<u64*>(c_x_guard->getKey(delta_pos) + c_x_guard->getKeyLen(delta_pos) - 8);
               *reinterpret_cast<u64*>(v_key + update_entry.key_length) = delta_version;
               // -------------------------------------------------------------------------------------
               bool ret = c_x_guard->removeSlot(pos);
               ensure(ret);
               ret = c_x_guard->removeSlot(delta_pos);
               ensure(ret);
               // -------------------------------------------------------------------------------------
               c_x_guard->requestSpaceFor(old_payload_length + v_key_length);
               c_x_guard->insert(v_key, v_key_length, old_payload, old_payload_length);
               // -------------------------------------------------------------------------------------
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      case vi::WAL_LOG_TYPE::WALRemove: {
         const auto& remove_entry = *reinterpret_cast<const vi::WALRemove*>(&entry);
         u16 v_key_length = remove_entry.key_length + 8;
         u8 v_key[v_key_length];
         std::memcpy(v_key, remove_entry.payload, remove_entry.key_length);
         *reinterpret_cast<u64*>(v_key + remove_entry.key_length) = tts;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> c_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(c_guard, v_key, v_key_length);
               auto c_x_guard = ExclusivePageGuard(std::move(c_guard));
               const bool ret = c_x_guard->remove(v_key, v_key_length);
               ensure(ret);
               // -------------------------------------------------------------------------------------
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      default: {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::iterateDescVI(u8* start_key, u16 key_length, function<bool(HybridPageGuard<BTreeNode>&, s16)> callback)
{
   u8* volatile next_key = start_key;
   volatile u16 next_key_length = key_length;
   volatile bool is_heap_freed = true;  // because at first we reuse the start_key
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         while (true) {
            findLeafCanJump<OP_TYPE::SCAN>(leaf, next_key, next_key_length);
            SharedPageGuard s_leaf(std::move(leaf));
            // -------------------------------------------------------------------------------------
            if (s_leaf->count == 0) {
               jumpmu_return;
            }
            s16 cur;
            if (next_key == start_key) {
               cur = s_leaf->lowerBound<false>(start_key, key_length);
               if (s_leaf->lowerBound<true>(start_key, key_length) == -1) {
                  cur--;
               }
            } else {
               cur = s_leaf->count - 1;
            }
            // -------------------------------------------------------------------------------------
            while (cur >= 0) {
               if (!callback(leaf, cur)) {
                  if (!is_heap_freed) {
                     delete[] next_key;
                     is_heap_freed = true;
                  }
                  jumpmu_return;
               }
               cur--;
            }
            // -------------------------------------------------------------------------------------
            if (!is_heap_freed) {
               delete[] next_key;
               is_heap_freed = true;
            }
            if (s_leaf->isLowerFenceInfinity()) {
               jumpmu_return;
            }
            // -------------------------------------------------------------------------------------
            next_key_length = s_leaf->lower_fence.length;
            next_key = new u8[next_key_length];
            is_heap_freed = false;
            memcpy(next_key, s_leaf->getLowerFenceKey(), s_leaf->lower_fence.length);
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTree::scanDescVI(u8* k, u16 kl, function<bool(u8*, u16, u8*, u16)> callback)
{
   u8 key[kl + 8];
   u16 key_length = kl + 8;
   std::memcpy(key, k, kl);
   *reinterpret_cast<u64*>(key + kl) = std::numeric_limits<u64>::max();
   // -------------------------------------------------------------------------------------
   bool skip_delta = false;
   s16 payload_length = -1;
   std::unique_ptr<u8[]> payload(nullptr);
   iterateDescVI(key, key_length, [&](HybridPageGuard<BTreeNode>& leaf, s16 pos) {
      if (!skip_delta || leaf->isDelta(pos)) {
      }
      return true;
   });
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
