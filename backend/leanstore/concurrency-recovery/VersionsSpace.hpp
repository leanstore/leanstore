#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <unordered_map>
#include <thread>
#include <vector>
#include <shared_mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
class VersionsSpace
{
  private:
   KVInterface* kv_store;
   // -------------------------------------------------------------------------------------
   static std::shared_mutex mutex;
   static std::map<std::basic_string<u8>, std::basic_string<u8>>
       map;  // TODO: tmp hack
             // -------------------------------------------------------------------------------------

  public:
   void insertVersion(TXID tx_id, DTID dt_id, u64 command_id, u64 payload_length, std::function<void(u8*)> cb);
   bool retrieveVersion(TXID tx_id, DTID dt_id, u64 command_id, std::function<void(u8*, u64 payload_length)> cb);
   void purgTXIDRange(TXID from_tx_id, TXID to_tx_id);  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
