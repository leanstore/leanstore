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
struct __attribute__((packed)) VersionMeta {
   bool should_callback = false;
   DTID dt_id;
   u8 payload[];
};
class VersionsSpaceInterface
{
  public:
   virtual void insertVersion(WORKERID worker_id,
                              TXID tx_id,
                              COMMANDID command_id,
                              u64 payload_length,
                              bool should_callback,
                              DTID dt_id,
                              std::function<void(u8*)> cb) = 0;
   virtual bool retrieveVersion(WORKERID worker_id, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64 payload_length)> cb) = 0;
   virtual void purgeTXIDRange(WORKERID worker_id,
                               TXID from_tx_id,
                               TXID to_tx_id,
                               std::function<void(const TXID, const DTID, const u8*, u64 payload_length)> cb) = 0;  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
