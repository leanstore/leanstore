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
   if (!FLAGS_history_tree_inserts) {
      return;
   }
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
   if (session != nullptr && session->rightmost_init) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(const_cast<BTreeLL*>(btree)), session->rightmost_bf, session->rightmost_version);
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::OK && iterator.keyInCurrentBoundaries(key)) {
            if (session->last_tx_id == tx_id) {
               iterator.leaf->insertDoNotCopyPayload(key.data(), key.length(), payload_length, session->rightmost_pos);
               iterator.cur = session->rightmost_pos;
            } else {
               iterator.insertInCurrentNode(key, payload_length);
            }
            auto& version_meta = *new (iterator.mutableValue().data()) VersionMeta();
            version_meta.dt_id = dt_id;
            cb(version_meta.payload);
            iterator.markAsDirty();
            COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_versions_space_inserted_opt[dt_id]++; }
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
         // -------------------------------------------------------------------------------------
         OP_RESULT ret = iterator.seekToInsert(key);
         if (ret == OP_RESULT::DUPLICATE) {
            iterator.removeCurrent();  // TODO: verify, this implies upsert semantic
         } else {
            ensure(ret == OP_RESULT::OK);
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
            session->rightmost_bf = iterator.leaf.bf;
            session->rightmost_version = iterator.leaf.guard.version + 1;
            session->rightmost_pos = iterator.cur + 1;
            session->last_tx_id = tx_id;
            session->rightmost_init = true;
         }
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_versions_space_inserted[dt_id]++; }
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
   jumpmuCatch() { jumpmu::jump(); }
   UNREACHABLE();
   return false;
}
// -------------------------------------------------------------------------------------
void VersionsSpace::purgeVersions(WORKERID worker_id,
                                  TXID from_tx_id,
                                  TXID to_tx_id,
                                  RemoveVersionCallback cb,
                                  [[maybe_unused]] const u64 limit)  // [from, to]
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
         while (ret == OP_RESULT::OK) {
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
   Session* volatile session = &update_sessions[worker_id];  // Attention: no cross worker gc in sync
   volatile bool should_try = true;
   if (from_tx_id == 0) {
      jumpmuTry()
      {
         if (session->leftmost_init) {
            BufferFrame* bf = session->leftmost_bf;
            Guard bf_guard(bf->header.latch, session->leftmost_version);
            bf_guard.recheck();
            HybridPageGuard<BTreeNode> leaf(std::move(bf_guard), bf);
            // -------------------------------------------------------------------------------------
            if (leaf->lower_fence.length == 0) {
               u8 last_key[leaf->getFullKeyLen(leaf->count - 1)];
               leaf->copyFullKey(leaf->count - 1, last_key);
               TXID last_key_tx_id;
               utils::unfold(last_key, last_key_tx_id);
               if (last_key_tx_id > to_tx_id) {
                  should_try = false;
               }
            }
         }
      }
      jumpmuCatch() {}
   }
   while (should_try) {
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
         // ATTENTION: we use this also for purging the current aborted tx so we can not simply assume from_tx_id = 0
         bool did_purge_full_page = false;
         iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
            if (leaf->count == 0) {
               return;
            }
            u8 first_key[leaf->getFullKeyLen(0)];
            leaf->copyFullKey(0, first_key);
            TXID first_key_tx_id;
            utils::unfold(first_key, first_key_tx_id);
            // -------------------------------------------------------------------------------------
            u8 last_key[leaf->getFullKeyLen(leaf->count - 1)];
            leaf->copyFullKey(leaf->count - 1, last_key);
            TXID last_key_tx_id;
            utils::unfold(last_key, last_key_tx_id);
            if (first_key_tx_id >= from_tx_id && to_tx_id >= last_key_tx_id) {
               // Purge the whole page
               removed_versions += leaf->count;
               leaf->reset();
               did_purge_full_page = true;
            }
         });
         // -------------------------------------------------------------------------------------
         iterator.seek(key);
         if (did_purge_full_page) {
            did_purge_full_page = false;
            jumpmu_continue;
         } else {
            session->leftmost_bf = iterator.leaf.bf;
            session->leftmost_version = iterator.leaf.guard.version + 1;
            session->leftmost_init = true;
            jumpmu_break;
         }
      }
      jumpmuCatch() { UNREACHABLE(); }
   }
   COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_removed += removed_versions; }
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
