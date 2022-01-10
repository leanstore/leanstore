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
class SMTCInterface
{
  public:
   virtual bool isVisibleForMe(WORKERID worker_id, TXID tts, bool to_write = true);
   bool isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, TXID tts);

   virtual void insertVersion(WORKERID worker_id,
                              TXID tx_id,
                              COMMANDID command_id,
                              DTID dt_id,
                              bool is_remove,
                              u64 payload_length,
                              std::function<void(u8*)> cb,
                              bool same_thread = true) = 0;
   virtual bool retrieveVersion(WORKERID worker_id,
                                TXID tx_id,
                                COMMANDID command_id,
                                const bool is_remove,
                                std::function<void(const u8*, u64 payload_length)> cb) = 0;
   virtual void purgeVersions(WORKERID worker_id, TXID from_tx_id, TXID to_tx_id, RemoveVersionCallback cb, const u64 limit = 0) = 0;
   virtual void visitRemoveVersions(WORKERID worker_id, TXID from_tx_id, TXID to_tx_id,
                                    RemoveVersionCallback cb) = 0;  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
