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
void VersionsSpace::insertVersion(WORKERID session_id, TXID tx_id, COMMANDID command_id, u64 payload_length, std::function<void(u8*)> cb)
{
   const u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key_buffer[key_length];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, tx_id);
   offset += utils::fold(key_buffer + offset, command_id);
   Slice key(key_buffer, key_length);
   // -------------------------------------------------------------------------------------
   BTreeLL* btree = btrees[session_id];
   auto& session = sessions[session_id];
   if (session.init) {
      jumpmuTry()
      {
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(btree), session.bf, session.version);
         OP_RESULT ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::OK && iterator.keyInCurrentBoundaries(key)) {
            if (session.last_tx_id == tx_id) {
               iterator.leaf->insertDoNotCopyPayload(key.data(), key.length(), payload_length, session.pos);
               iterator.cur = session.pos;
            } else {
               iterator.insertInCurrentNode(key, payload_length);
            }
            cb(iterator.mutableValue().data());
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
         BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(btree));
         OP_RESULT ret = iterator.seekToInsert(key);
         ensure(ret == OP_RESULT::OK);
         ret = iterator.enoughSpaceInCurrentNode(key, payload_length);
         if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
            iterator.splitForKey(key);
            jumpmu_continue;
         }
         iterator.insertInCurrentNode(key, payload_length);
         cb(iterator.mutableValue().data());
         iterator.markAsDirty();
         // -------------------------------------------------------------------------------------
         session.bf = iterator.leaf.bf;
         session.version = iterator.leaf.guard.version + 1;
         session.pos = iterator.cur + 1;
         session.last_tx_id = tx_id;
         session.init = true;
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_inserted++; }
         jumpmu_return;
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
bool VersionsSpace::retrieveVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64)> cb)
{
   BTreeLL* btree = btrees[worker_id];
   const u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key_buffer[key_length];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, tx_id);
   offset += utils::fold(key_buffer + offset, command_id);
   // -------------------------------------------------------------------------------------
   Slice key(key_buffer, key_length);
   jumpmuTry()
   {
      BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(btree), LATCH_FALLBACK_MODE::SHARED);
      OP_RESULT ret = iterator.seekExact(key);
      if (ret != OP_RESULT::OK) {
         jumpmu_return false;
      }
      Slice payload = iterator.value();
      cb(payload.data(), payload.length());
      jumpmu_return true;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return false;
}
// -------------------------------------------------------------------------------------
// Pre: TXID is unsigned integer
void VersionsSpace::purgeTXIDRange(WORKERID worker_id, TXID from_tx_id, TXID to_tx_id)
{
   // [from, to]
   BTreeLL* btree = btrees[worker_id];
   const u64 key_length = sizeof(to_tx_id);
   u8 key_buffer[key_length];
   u64 offset = 0;
   offset += utils::fold(key_buffer + offset, from_tx_id);
   Slice key(key_buffer, key_length);
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
   retry : {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(btree));
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK) {
         iterator.assembleKey();
         TXID current_tx_id;
         utils::unfold(iterator.key().data(), current_tx_id);
         if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
            ret = iterator.removeCurrent();
            ensure(ret == OP_RESULT::OK);
            COUNTERS_BLOCK() { CRCounters::myCounters().cc_versions_space_removed++; }
            iterator.markAsDirty();
            if (iterator.mergeIfNeeded()) {
               goto retry;
            }
            if (iterator.cur == iterator.leaf->count) {
               ret = iterator.next();
            }
         } else {
            break;
         }
      }
      jumpmu_return;
   }
   }
   jumpmuCatch() {}
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
