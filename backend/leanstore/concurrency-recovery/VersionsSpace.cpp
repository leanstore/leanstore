#include "VersionsSpace.hpp"

#include "Units.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/btree/core/BTreeGenericIterator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
using namespace leanstore::storage::btree;
// -------------------------------------------------------------------------------------
void VersionsSpace::insertVersion(WORKERID session_id,
                                  TXID tx_id,
                                  COMMANDID command_id,
                                  DTID dt_id,
                                  bool is_remove,
                                  u64 payload_length,
                                  std::function<void(u8*)> cb,
                                  bool same_thread)
{
   const u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key_buffer[key_length];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, tx_id);
   offset += utils::fold(key_buffer + offset, command_id);
   Slice key(key_buffer, key_length);
   payload_length += sizeof(VersionMeta);
   // -------------------------------------------------------------------------------------
   BTreeLL* volatile btree = (is_remove) ? remove_btrees[session_id] : update_btrees[session_id];
   Session* volatile session = nullptr;
   if (same_thread) {
      session = (is_remove) ? &remove_sessions[session_id] : &update_sessions[session_id];
   }
   if (session != nullptr && session->init) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)), session->bf, session->version);
         iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
            if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
               iterator.cleanUpCallback([&, to_find = leaf.bf] {
                  jumpmuTry() { btree->tryMerge(*to_find); }
                  jumpmuCatch() {}
               });
            }
         });
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::OK && iterator.keyInCurrentBoundaries(key)) {
            if (session->last_tx_id == tx_id) {
               iterator.leaf->insertDoNotCopyPayload(key.data(), key.length(), payload_length, session->pos);
               iterator.cur = session->pos;
            } else {
               iterator.insertInCurrentNode(key, payload_length);
            }
            auto& version_meta = *new (iterator.mutableValue().data()) VersionMeta();
            version_meta.dt_id = dt_id;
            cb(version_meta.payload);
            iterator.markAsDirty();
            COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_inserted_opt++; }
            iterator.leaf.unlock();
            jumpmu_return;
         }
      }
      jumpmuCatch() {}
   }
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
         iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
            if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
               iterator.cleanUpCallback([&, to_find = leaf.bf] {
                  jumpmuTry() { btree->tryMerge(*to_find); }
                  jumpmuCatch() {}
               });
            }
         });
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            iterator.markAsDirty();
            jumpmu_continue;
         }
         ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         iterator.insertInCurrentNode(key, payload_length);
         auto& version_meta = *new (iterator.mutableValue().data()) VersionMeta();
         version_meta.dt_id = dt_id;
         cb(version_meta.payload);
         iterator.markAsDirty();
         // -------------------------------------------------------------------------------------
         if (session != nullptr) {
            session->bf = iterator.leaf.bf;
            session->version = iterator.leaf.guard.version + 1;
            session->pos = iterator.cur + 1;
            session->last_tx_id = tx_id;
            session->init = true;
         }
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_inserted++; }
         jumpmu_return;
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
bool VersionsSpace::retrieveVersion(WORKERID worker_id,
                                    TXID tx_id,
                                    COMMANDID command_id,
                                    const bool is_remove,
                                    std::function<void(const u8*, u64)> cb)
{
   BTreeLL* volatile btree = (is_remove) ? remove_btrees[worker_id] : update_btrees[worker_id];
   // -------------------------------------------------------------------------------------
   const u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key_buffer[key_length];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, tx_id);
   offset += utils::fold(key_buffer + offset, command_id);
   // -------------------------------------------------------------------------------------
   Slice key(key_buffer, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)), LATCH_FALLBACK_MODE::SHARED);
      OP_RESULT ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return false;
      }
      Slice payload = iterator.value();
      const auto& version_container = *reinterpret_cast<const VersionMeta*>(payload.data());
      cb(version_container.payload, payload.length() - sizeof(VersionMeta));
      jumpmu_return true;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return false;
}
// -------------------------------------------------------------------------------------
void VersionsSpace::purgeVersions(WORKERID worker_id, TXID from_tx_id, TXID to_tx_id, RemoveVersionCallback cb, const u64 limit)
{
   u16 key_length = sizeof(to_tx_id);
   u8 key_buffer[PAGE_SIZE];
   utils::fold(key_buffer, from_tx_id);
   Slice key(key_buffer, key_length);
   u8 payload[PAGE_SIZE];
   u16 payload_length;
   volatile u64 removed_versions = 0;
   BTreeLL* volatile btree = remove_btrees[worker_id];
   // -------------------------------------------------------------------------------------
   {
      jumpmuTry()
      {
      restartrem : {
         leanstore::storage::btree::BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
         iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
            if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
               iterator.cleanUpCallback([&, to_find = leaf.bf] {
                  jumpmuTry() { btree->tryMerge(*to_find); }
                  jumpmuCatch() {}
               });
            }
         });
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.seek(key);
         while (ret == OP_RESULT::OK && (limit == 0 || removed_versions < limit)) {
            iterator.assembleKey();
            TXID current_tx_id;
            utils::unfold(iterator.key().data(), current_tx_id);
            if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
               auto& version_container = *reinterpret_cast<VersionMeta*>(iterator.mutableValue().data());
               const DTID dt_id = version_container.dt_id;
               const bool called_before = version_container.called_before;
               version_container.called_before = true;
               key_length = iterator.key().length();
               std::memcpy(key_buffer, iterator.key().data(), key_length);
               payload_length = iterator.value().length() - sizeof(VersionMeta);
               std::memcpy(payload, version_container.payload, payload_length);
               key = Slice(key_buffer, key_length + 1);
               iterator.removeCurrent();
               removed_versions++;
               iterator.markAsDirty();
               iterator.reset();
               cb(current_tx_id, dt_id, payload, payload_length, called_before);
               goto restartrem;
            } else {
               break;
            }
         }
      }
      }
      jumpmuCatch() { UNREACHABLE(); }
   }
   // -------------------------------------------------------------------------------------
   btree = update_btrees[worker_id];
   utils::fold(key_buffer, from_tx_id);
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)));
      iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
         if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
            iterator.cleanUpCallback([&, to_find = leaf.bf] {
               jumpmuTry() { btree->tryMerge(*to_find); }
               jumpmuCatch() {}
            });
         }
      });
      // -------------------------------------------------------------------------------------
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK && (limit == 0 || removed_versions < limit)) {
         iterator.assembleKey();
         TXID current_tx_id;
         utils::unfold(iterator.key().data(), current_tx_id);
         if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
            ret = iterator.removeCurrent();
            removed_versions++;
            ensure(ret == OP_RESULT::OK);
            COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_removed++; }
            iterator.markAsDirty();
            if (iterator.cur == iterator.leaf->count) {
               ret = iterator.next();
            }
         } else {
            break;
         }
      }
   }
   jumpmuCatch() { UNREACHABLE(); }
}
// -------------------------------------------------------------------------------------
// Pre: TXID is unsigned integer
void VersionsSpace::visitRemoveVersions(WORKERID worker_id,
                                        TXID from_tx_id,
                                        TXID to_tx_id,
                                        std::function<void(const TXID, const DTID, const u8*, u64, const bool visited_before)> cb)
{
   // [from, to]
   BTreeLL* btree = remove_btrees[worker_id];
   u16 key_length = sizeof(to_tx_id);
   u8 key_buffer[PAGE_SIZE];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, from_tx_id);
   Slice key(key_buffer, key_length);
   u8 payload[PAGE_SIZE];
   u16 payload_length;
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
   restart : {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(btree));
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK) {
         iterator.assembleKey();
         TXID current_tx_id;
         utils::unfold(iterator.key().data(), current_tx_id);
         if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
            auto& version_container = *reinterpret_cast<VersionMeta*>(iterator.mutableValue().data());
            const DTID dt_id = version_container.dt_id;
            const bool called_before = version_container.called_before;
            ensure(called_before == false);
            version_container.called_before = true;
            key_length = iterator.key().length();
            std::memcpy(key_buffer, iterator.key().data(), key_length);
            payload_length = iterator.value().length() - sizeof(VersionMeta);
            std::memcpy(payload, version_container.payload, payload_length);
            key = Slice(key_buffer, key_length + 1);
            if (!called_before) {
               iterator.markAsDirty();
            }
            iterator.reset();
            cb(current_tx_id, dt_id, payload, payload_length, called_before);
            goto restart;
         } else {
            break;
         }
      }
   }
   }
   jumpmuCatch() {}
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
