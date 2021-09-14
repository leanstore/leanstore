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
using BTreeLL = leanstore::storage::btree::BTreeLL;
class VersionsSpace : public VersionsSpaceInterface
{
  private:
   struct alignas(64) Session {
      BufferFrame* bf;
      u64 version;
      s64 pos = -1;
      TXID last_tx_id;
      bool init = false;
   };
   Session sessions[leanstore::cr::STATIC_MAX_WORKERS];

  public:
   std::unique_ptr<BTreeLL*[]> btrees;
   virtual void insertVersion(WORKERID worker_id,
                              TXID tx_id,
                              COMMANDID command_id,
                              u64 payload_length,
                              bool should_callback,
                              DTID dt_id,
                              std::function<void(u8*)> cb);
   virtual bool retrieveVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64 payload_length)> cb);
   virtual void purgeTXIDRange(WORKERID worker_id,
                               TXID from_tx_id,
                               TXID to_tx_id,
                               std::function<void(const TXID, const DTID, const u8*, u64 payload_length)> cb);  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
