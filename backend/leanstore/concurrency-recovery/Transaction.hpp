#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
enum class TX_MODE : u8 { LONG_READWRITE, LONG_READONLY, SINGLE_READONLY, SINGLE_READWRITE };
enum class TX_ISOLATION_LEVEL : u8 { SERIALIZABLE = 3, SNAPSHOT_ISOLATION = 2, READ_COMMITTED = 1, READ_UNCOMMITTED = 0 };
inline TX_ISOLATION_LEVEL parseIsolationLevel(std::string str)
{
   if (str == "ser") {
      return leanstore::TX_ISOLATION_LEVEL::SERIALIZABLE;
   } else if (str == "si") {
      return leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   } else if (str == "rc") {
      return leanstore::TX_ISOLATION_LEVEL::READ_COMMITTED;
   } else if (str == "ru") {
      return leanstore::TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
   } else {
      UNREACHABLE();
      return leanstore::TX_ISOLATION_LEVEL::READ_UNCOMMITTED;
   }
}
// -------------------------------------------------------------------------------------
namespace cr
{
// -------------------------------------------------------------------------------------
struct Transaction {
   enum class TYPE : u8 { USER, SYSTEM };
   enum class STATE { IDLE, STARTED, READY_TO_COMMIT, COMMITED, ABORTED };
   STATE state = STATE::IDLE;
   u64 commit_mark = 0;
   LID min_observed_gsn_when_started, max_observed_gsn;
   TX_MODE current_tx_mode = TX_MODE::LONG_READWRITE;
   TX_ISOLATION_LEVEL current_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   bool is_durable = false;
   bool can_use_single_version_mode = false;
   // -------------------------------------------------------------------------------------
   bool isReadOnly() { return current_tx_mode == TX_MODE::LONG_READONLY || current_tx_mode == TX_MODE::SINGLE_READONLY; }
   bool isSingleStatement() { return current_tx_mode == TX_MODE::SINGLE_READWRITE || current_tx_mode == TX_MODE::SINGLE_READONLY; }
   bool isDurable() { return is_durable; }
   bool isSerializable() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::SERIALIZABLE; }
   bool atLeastSI() { return current_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION; }
   bool isSI() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION; }
   bool isReadCommitted() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::READ_COMMITTED; }
   bool isReadUncommitted() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::READ_UNCOMMITTED; }
   bool canUseSingleVersion() { return can_use_single_version_mode; }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
