#include "VersionsSpace.hpp"
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
std::shared_mutex VersionsSpace::mutex;
std::map<std::basic_string<u8>, std::basic_string<u8>> VersionsSpace::map;
// -------------------------------------------------------------------------------------
void VersionsSpace::insertVersion(TXID tx_id, DTID dt_id, u64 command_id, u64 payload_length, std::function<void(u8*)> cb)
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
bool VersionsSpace::retrieveVersion(TXID tx_id, DTID dt_id, u64 command_id, std::function<void(u8*, u64 payload_length)> cb)
{
   u64 key_length = sizeof(tx_id) + sizeof(dt_id) + sizeof(command_id);
   u8 key[key_length];
   u64 offset = 0;
   offset += utils::fold(key + offset, tx_id);
   offset += utils::fold(key + offset, dt_id);
   offset += utils::fold(key + offset, command_id);
   std::basic_string<u8> key_str(key, key_length);
   // -------------------------------------------------------------------------------------
   mutex.lock_shared();
   auto iter = map.find(key_str);
   if (iter == map.end()) {
      mutex.unlock_shared();
      return false;
   } else {
      cb(iter->second.data(), iter->second.size());
      mutex.unlock_shared();
      return true;
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
