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
class VersionsSpaceInterface
{
  public:
   virtual void insertVersion(WORKERID session_id, TXID tx_id, COMMANDID command_id, u64 payload_length, std::function<void(u8*)> cb) = 0;
   virtual bool retrieveVersion(WORKERID session_id, TXID tx_id, COMMANDID command_id, std::function<void(const u8*, u64 payload_length)> cb) = 0;
   virtual void purgeTXIDRange(TXID from_tx_id, TXID to_tx_id) = 0;  // [from, to]
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
