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
// -------------------------------------------------------------------------------------
namespace vw
{
enum class WAL_LOG_TYPE : u8 { WALInsert, WALUpdate, WALRemove, WALAfterBeforeImage, WALAfterImage, WALLogicalSplit, WALInitPage };
struct WALEntry {
   vw::WAL_LOG_TYPE type;
   cr::WLSN tuple_prev;
   cr::WTTS wtts;
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
   u16 delta_length;
   u8 payload[];
};
struct WALRemove : WALEntry {
   u16 key_length;
   u16 payload_length;
   u8 payload[];
};
struct __attribute__((packed)) Version {
   u8 worker_id : 8;
   u64 tts : 56;
   u64 lsn : 56;
   u8 is_removed : 1;
   u8 is_final : 1;
   Version(u8 worker_id, u64 tts, u64 lsn, bool is_deleted, bool is_final)
       : worker_id(worker_id), tts(tts), lsn(lsn), is_removed(is_deleted), is_final(is_final)
   {
   }
};
static_assert(sizeof(Version) == (2 * sizeof(u64)), "");
}  // namespace vw
// -------------------------------------------------------------------------------------
// Plan: Value gets an 8-byte version
OP_RESULT BTree::insertVW(u8* key, u16 key_length, u16 value_length_orig, u8* value_orig)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 value_length = value_length_orig + sizeof(vw::Version);
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf_guard;
         findLeafCanJump<OP_TYPE::POINT_INSERT>(leaf_guard, key, key_length);
         // -------------------------------------------------------------------------------------
         auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
         s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
         if (pos == -1) {
            // Really new
            if (leaf_ex_guard->canInsert(key_length, value_length)) {
               // -------------------------------------------------------------------------------------
               // WAL
               auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALInsert>(key_length + value_length_orig);
               wal_entry->type = vw::WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = key_length;
               wal_entry->value_length = value_length_orig;
               wal_entry->wtts.worker_id = myWorkerID();
               wal_entry->wtts.tts = myTTS();
               wal_entry->tuple_prev.lsn = 0;
               std::memcpy(wal_entry->payload, key, key_length);
               std::memcpy(wal_entry->payload + key_length, value_orig, value_length_orig);
               wal_entry.submit();
               // -------------------------------------------------------------------------------------
               u8 value[value_length];
               std::memcpy(value + sizeof(vw::Version), value_orig, value_length_orig);
               new (value) vw::Version(myWorkerID(), myTTS(), wal_entry.lsn, false, true);
               // -------------------------------------------------------------------------------------
               leaf_ex_guard->insert(key, key_length, value, value_length);
               jumpmu_return OP_RESULT::OK;
            }
         } else {
            auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  if (leaf_ex_guard->canInsert(key_length, value_length)) {
                     // -------------------------------------------------------------------------------------
                     // WAL
                     auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALInsert>(key_length + value_length_orig);
                     wal_entry->type = vw::WAL_LOG_TYPE::WALInsert;
                     wal_entry->key_length = key_length;
                     wal_entry->value_length = value_length_orig;
                     wal_entry->wtts.worker_id = myWorkerID();
                     wal_entry->wtts.tts = myTTS();
                     // -------------------------------------------------------------------------------------
                     // Link to the previous LSN
                     wal_entry->tuple_prev.lsn = version.lsn;
                     wal_entry->tuple_prev.worker_id = version.worker_id;
                     // -------------------------------------------------------------------------------------
                     std::memcpy(wal_entry->payload, key, key_length);
                     std::memcpy(wal_entry->payload + key_length, value_orig, value_length_orig);
                     wal_entry.submit();
                     // -------------------------------------------------------------------------------------
                     u8 value[value_length];
                     std::memcpy(value + sizeof(vw::Version), value_orig, value_length_orig);
                     new (value) vw::Version(myWorkerID(), myTTS(), wal_entry.lsn, false, true);
                     // -------------------------------------------------------------------------------------
                     leaf_ex_guard->insert(key, key_length, value, value_length);
                     jumpmu_return OP_RESULT::OK;
                  }
               } else {
                  jumpmu_return OP_RESULT::DUPLICATE;
               }
            } else {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
         }
         // -------------------------------------------------------------------------------------
         // Release lock
         leaf_guard = std::move(leaf_ex_guard);
         leaf_guard.kill();
         // -------------------------------------------------------------------------------------
         trySplit(*leaf_guard.bf);
         // -------------------------------------------------------------------------------------
         jumpmu_continue;
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::lookupVW(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump<OP_TYPE::POINT_READ>(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            auto& version = *reinterpret_cast<vw::Version*>(leaf->getPayload(pos));
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  leaf.recheck_done();
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
                  leaf.recheck_done();
                  jumpmu_return OP_RESULT::OK;
               }
            } else {
               cr::WLSN prev_wsln(version.worker_id, version.lsn);
               u16 payload_length = leaf->getPayloadLength(pos);
               JMUW<std::unique_ptr<u8[]>> payload = std::make_unique<u8[]>(payload_length);
               leaf.recheck();
               reconstructTupleVW(payload.obj, payload_length, prev_wsln.worker_id, prev_wsln.lsn);
               if (payload_length == 0) {
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  payload_callback(payload->get(), payload_length);
                  jumpmu_return OP_RESULT::OK;
               }
            }
         } else {
            leaf.recheck_done();
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTree::reconstructTupleVW(std::unique_ptr<u8[]>& payload, u16& payload_length, u8 next_worker_id, u64 next_lsn)
{
   bool flag = true;
   while (flag) {
      cr::Worker::my().getWALDTEntry(next_worker_id, next_lsn, [&](u8* entry) {
         auto& wal_entry = *reinterpret_cast<vw::WALEntry*>(entry);
         switch (wal_entry.type) {
            case vw::WAL_LOG_TYPE::WALRemove: {
               auto& remove_entry = *reinterpret_cast<vw::WALRemove*>(entry);
               payload_length = remove_entry.payload_length;
               payload.reset(new u8[payload_length]);
               std::memcpy(payload.get(), remove_entry.payload, payload_length);
               break;
            }
            case vw::WAL_LOG_TYPE::WALInsert: {
               payload.reset();
               payload_length = 0;
               break;
            }
            case vw::WAL_LOG_TYPE::WALUpdate: {
               auto& update_entry = *reinterpret_cast<vw::WALUpdate*>(entry);
               applyDelta(payload.get(), update_entry.payload, update_entry.delta_length);
               break;
            }
            default: {
               ensure(false);
            }
         }
         if (wal_entry.tuple_prev.lsn == 0) {
            flag = false;
         } else {
            next_worker_id = wal_entry.tuple_prev.worker_id;
            next_lsn = wal_entry.tuple_prev.lsn;
         }
      });
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::updateVW(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator)
{
   return OP_RESULT::ABORT_TX;
}
OP_RESULT BTree::removeVW(u8* key, u16 key_length)
{
   return OP_RESULT::ABORT_TX;
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
