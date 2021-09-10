#include "VersionsSpaceSTD.hpp"

#include "Units.hpp"
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
// -------------------------------------------------------------------------------------
void VersionsSpaceSTD::insertVersion(WORKERID, TXID tx_id, COMMANDID command_id, u64 payload_length, std::function<void(u8*)> cb)
{
   u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, command_id);
   std::basic_string<u8> key_str(key, key_length);
   std::basic_string<u8> payload_str;
   payload_str.resize(payload_length);
   cb(payload_str.data());
   // -------------------------------------------------------------------------------------
   std::unique_lock guard(mutex);
   ensure(map.count(key_str) == 0);
   map[key_str] = payload_str;
}
// -------------------------------------------------------------------------------------
bool VersionsSpaceSTD::retrieveVersion(WORKERID, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64 payload_length)> cb)
{
   u64 key_length = sizeof(tx_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, command_id);
   std::basic_string<u8> key_str(key, key_length);
   // -------------------------------------------------------------------------------------
   std::unique_lock guard(mutex);
   auto iter = map.find(key_str);
   if (iter == map.end()) {
      return false;
   } else {
      cb(iter->second.data(), iter->second.size());
      return true;
   }
}
// -------------------------------------------------------------------------------------
void VersionsSpaceSTD::purgeTXIDRange(WORKERID, TXID from_tx_id, TXID to_tx_id)
{  // [from, to]
   std::basic_string<u8> begin;
   begin.resize(sizeof(from_tx_id));
   utils::fold(begin.data(), from_tx_id);
   std::unique_lock guard(mutex);
   auto iter = map.lower_bound(begin);
   while (iter != map.end()) {
      auto& current_tx_id = *reinterpret_cast<const TXID*>(iter->first.data());
      if (current_tx_id >= from_tx_id && current_tx_id <= to_tx_id) {
         iter = map.erase(iter);
      } else {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
