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
   std::shared_mutex mutex;
   std::map<std::basic_string<u8>, std::basic_string<u8>> map;
   // -------------------------------------------------------------------------------------

  public:
   VersionsSpaceSTD() = default;
   ~VersionsSpaceSTD() = default;
   virtual void insertVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, u64 payload_length, std::function<void(u8*)> cb);
   virtual bool retrieveVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64 payload_length)> cb);
   virtual void purgeTXIDRange(WORKERID worker_id, TXID from_tx_id, TXID to_tx_id);  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
