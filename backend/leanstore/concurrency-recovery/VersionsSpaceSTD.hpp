#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "VersionsSpaceInterface.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/BTreeLL.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
class VersionsSpaceSTD : public VersionsSpaceInterface
{
  private:
   // -------------------------------------------------------------------------------------
   static std::shared_mutex mutex;
   static std::map<std::basic_string<u8>, std::basic_string<u8>>
       map;  // TODO: tmp hack
             // -------------------------------------------------------------------------------------

  public:
   VersionsSpaceSTD() = default;
   ~VersionsSpaceSTD() = default;
   virtual void insertVersion(WORKERID session_id, TXID tx_id, DTID dt_id, u64 command_id, u64 payload_length, std::function<void(u8*)> cb);
   virtual bool retrieveVersion(WORKERID session_id, TXID tx_id, DTID dt_id, u64 command_id, std::function<void(const u8*, u64 payload_length)> cb);
   virtual void purgeTXIDRange(TXID from_tx_id, TXID to_tx_id);  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
