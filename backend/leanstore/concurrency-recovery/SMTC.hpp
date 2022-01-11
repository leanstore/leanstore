// #pragma once
// #include "Exceptions.hpp"
// #include "SMTCInterface.hpp"
// #include "Units.hpp"
// #include "Worker.hpp"
// #include "leanstore/Config.hpp"
// #include "leanstore/KVInterface.hpp"
// // -------------------------------------------------------------------------------------
// // -------------------------------------------------------------------------------------
// #include <atomic>
// #include <functional>
// #include <thread>
// #include <vector>
// // -------------------------------------------------------------------------------------
// namespace leanstore
// {
// namespace cr
// {
// // -------------------------------------------------------------------------------------
// using BTreeLL = leanstore::storage::btree::BTreeLL;
// class SMTC : public SMTCInterface
// {
//   private:
//    BTreeLL* start_to_commit_map;
//    BTreeLL* commit_to_start_map;

//   public:
//    virtual bool isVisibleForMe(WORKERID worker_id, TXID tts, bool to_write = true) final;
//    virtual bool isVisibleForAll(TXID start_ts) final;
// };
// // -------------------------------------------------------------------------------------
// }  // namespace cr
// }  // namespace leanstore
