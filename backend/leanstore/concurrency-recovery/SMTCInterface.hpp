// #pragma once
// #include "Exceptions.hpp"
// #include "Units.hpp"
// #include "leanstore/Config.hpp"
// #include "leanstore/KVInterface.hpp"
// #include "leanstore/utils/Misc.hpp"
// // -------------------------------------------------------------------------------------
// // -------------------------------------------------------------------------------------
// #include <atomic>
// #include <condition_variable>
// #include <functional>
// #include <map>
// #include <shared_mutex>
// #include <thread>
// #include <unordered_map>
// #include <vector>
// // -------------------------------------------------------------------------------------
// namespace leanstore
// {
// namespace cr
// {
// // -------------------------------------------------------------------------------------
// class SMTCInterface
// {
//   public:
//    virtual void insertStartCommitMapping(std::function<TXID()> cb) = 0;
//    virtual TXID getCommitTimestamp(TXID start_ts);
//    virtual TXID getLargestTransactionSTComittedBeforeThis(WORKERID worker_id, TXID start_tx) = 0;
//    // virtual bool isVisibleForMe(WORKERID worker_id, TXID tts, bool to_write = true) = 0;
//    // virtual bool isVisibleForAll(TXID start_ts) = 0;
//    // bool isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, TXID tts);
// };
// // -------------------------------------------------------------------------------------
// }  // namespace cr
// }  // namespace leanstore
