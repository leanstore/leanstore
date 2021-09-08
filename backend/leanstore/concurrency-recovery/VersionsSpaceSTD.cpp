#include "Units.hpp"
#include "VersionsSpaceSTD.hpp"
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
std::shared_mutex VersionsSpaceSTD::mutex;
std::map<std::basic_string<u8>, std::basic_string<u8>> VersionsSpaceSTD::map;
// -------------------------------------------------------------------------------------
void VersionsSpaceSTD::insertVersion(WORKERID session_id, TXID tx_id, DTID dt_id, u64 command_id, u64 payload_length, std::function<void(u8*)> cb)
{
   u64 key_length = sizeof(tx_id) + sizeof(dt_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, dt_id);
   offset += utils::fold(key + offset, command_id);
   std::basic_string<u8> key_str(key, key_length);
   std::basic_string<u8> payload_str;
   payload_str.resize(payload_length);
   cb(payload_str.data());
   // -------------------------------------------------------------------------------------
   std::unique_lock guard(mutex);
   map[key_str] = payload_str;
}
// -------------------------------------------------------------------------------------
bool VersionsSpaceSTD::retrieveVersion(WORKERID session_id, TXID tx_id, DTID dt_id, u64 command_id, std::function<void(const u8*, u64 payload_length)> cb)
{
   u64 key_length = sizeof(tx_id) + sizeof(dt_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, dt_id);
   offset += utils::fold(key + offset, command_id);
   std::basic_string<u8> key_str(key, key_length);
   // -------------------------------------------------------------------------------------
   std::shared_lock guard(mutex);
   auto iter = map.find(key_str);
   if (iter == map.end()) {
      return false;
   } else {
      cb(iter->second.data(), iter->second.size());
      return true;
   }
}
// -------------------------------------------------------------------------------------
void VersionsSpaceSTD::purgeTXIDRange(TXID from_tx_id, TXID to_tx_id)
{  // [from, to]
   // std::basic_string<u8> begin, end;
   // begin.resize(sizeof(from_tx_id));
   // utils::fold(begin.data(), from_tx_id);
   // end.resize(sizeof(from_tx_id));
   // utils::fold(end.data(), to_tx_id);
   // std::shared_lock guard(mutex);
   // auto range = map.range(begin)
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
