#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
enum class TX_MODE : u8 { OLAP, OLTP, SINGLE_STATEMENT };
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
   enum class STATE { IDLE, STARTED, READY_TO_COMMIT, COMMITTED, ABORTED };
   STATE state = STATE::IDLE;
   TXID start_ts = 0;  // = TXID
   TXID commit_ts = 0;
   LID min_observed_gsn_when_started, max_observed_gsn;
   TX_MODE current_tx_mode = TX_MODE::OLTP;
   TX_ISOLATION_LEVEL current_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION;
   bool is_durable = false;
   bool can_use_single_version_mode = false;
   bool safe_snapshot = false;
   bool is_read_only = false;
   bool has_wrote = false;
   // -------------------------------------------------------------------------------------
   bool isOLAP() { return current_tx_mode == TX_MODE::OLAP; }
   bool isOLTP() { return current_tx_mode == TX_MODE::OLTP; }
   bool isReadOnly() { return is_read_only; }
   bool hasWrote() { return has_wrote; }
   bool isSingleStatement() { return current_tx_mode == TX_MODE::SINGLE_STATEMENT; }
   bool isDurable() { return is_durable; }
   bool atLeastSI() { return current_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION; }
   bool isSI() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION; }
   bool isReadCommitted() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::READ_COMMITTED; }
   bool isReadUncommitted() { return current_tx_isolation_level == TX_ISOLATION_LEVEL::READ_UNCOMMITTED; }
   bool canUseSingleVersion() { return can_use_single_version_mode; }
   // -------------------------------------------------------------------------------------
   inline u64 startTS() { return start_ts; }
   inline u64 commitTS() { return commit_ts; }
   // -------------------------------------------------------------------------------------
   void markAsWrite()
   {
      assert(isReadOnly() == false);
      has_wrote = true;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
