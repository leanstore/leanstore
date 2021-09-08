#include "VersionsSpace.hpp"

#include "Units.hpp"
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
void VersionsSpace::insertVersion(WORKERID, TXID tx_id, u64 command_id, u64 payload_length, std::function<void(u8*)> cb)
{
   const u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, command_id);
   // -------------------------------------------------------------------------------------
   u8 payload[payload_length];
   cb(payload);
   btree->insert(key, key_length, payload, payload_length);
}
// -------------------------------------------------------------------------------------
bool VersionsSpace::retrieveVersion(WORKERID, TXID tx_id, u64 command_id, std::function<void(const u8*, u64)> cb)
{
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
void VersionsSpace::purgeTXIDRange(TXID from_tx_id, TXID to_tx_id)
{
   // [from, to]
   Slice key(reinterpret_cast<u8*>(&from_tx_id), sizeof(TXID));
   // -------------------------------------------------------------------------------------
   jumpmuTry()
   {
   retry : {
      leanstore::storage::btree::BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(btree));
      OP_RESULT ret = iterator.seek(key);
      while (ret == OP_RESULT::OK) {
         iterator.assembleKey();
         auto& current_tx_id = *reinterpret_cast<const TXID*>(iterator.key().data());
         if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
            iterator.removeCurrent();
            iterator.markAsDirty();
            if (iterator.mergeIfNeeded()) {
               goto retry;
            }
            ret = iterator.next();
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
