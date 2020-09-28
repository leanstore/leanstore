#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <mutex>
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct WALEntry {
  enum class Type : u8 { TX_START, DT_SPECIFIC, TX_COMMIT, TX_ABORT };
  // -------------------------------------------------------------------------------------
  u16 size;
  Type type;
  DTID dt_id;
  LID lsn;
  PID pid;
  LID gsn;
  u8 payload[];
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
struct WAL {
  static constexpr u64 WAL_SIZE = 1024 * 1024 * 1024;  // 20 * 5 MiB
  u8 buffer[WAL_SIZE];
  // -------------------------------------------------------------------------------------
  atomic<u64> wt_cursor = 0;  // Worker thread cursor
  atomic<u64> ww_cursor = 0;  // WAL Writer cursor
  WALEntry& entry(u16 requested_size)
  {
    u16 size = requested_size + sizeof(WALEntry);
    while ((WAL_SIZE - wt_cursor) < size) {
    }  // Spin until we have enough space
    WALEntry& entry = *reinterpret_cast<WALEntry*>(buffer + wt_cursor);
    entry.size = sizeof(WALEntry) + size;
    return entry;
  }
  // -------------------------------------------------------------------------------------
  LID lsn_counter = 0;
  LID max_gsn = 0;
};
// -------------------------------------------------------------------------------------
struct Transaction {
  enum class STATE { IDLE, STARTED, COMMITED, ABORTED };
  STATE state = STATE::IDLE;
  LID start_gsn = 0, current_gsn = 0;
  DTID current_dt_id = -1;
};
// -------------------------------------------------------------------------------------
struct Partition {
  enum class TYPE : u8 { USER, SYSTEM };
  TYPE type;
  Transaction tx;
  WAL wal;
  Partition(TYPE type) : type(type) {}
  // -------------------------------------------------------------------------------------
  inline LID getCurrentGSN() { return tx.current_gsn; }
  inline void setCurrentGSN(LID gsn)
  {
    tx.current_gsn = gsn;
    if (tx.start_gsn == 0)
      tx.start_gsn = gsn;
  }
  // -------------------------------------------------------------------------------------
  u8* reserveEntry(PID pid, DTID dt_id, LID gsn, u64 requested_size);
  // -------------------------------------------------------------------------------------
  void startTX();
  void commitTX();
  void abortTX();
};
// -------------------------------------------------------------------------------------
// MUST be allocated dynamically
class CRMG
{
 public:
  static std::set<Partition*> all_threads;
  static thread_local Partition* user_partition;
  static thread_local Partition* system_partition;
  static std::mutex mutex;
  // -------------------------------------------------------------------------------------
  CRMG();
  ~CRMG();
  // -------------------------------------------------------------------------------------
  static void registerThread();
  static void removeThread();
  // -------------------------------------------------------------------------------------
  inline static Partition& user()
  {
    assert(user_partition != nullptr);
    return *user_partition;
  }
  inline static Partition& system()
  {
    assert(system_partition != nullptr);
    return *system_partition;
  }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
