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
  enum class TYPE : u8 { USER_TX_START, SYSTEM_TX_START, DT_SPECIFIC, TX_COMMIT, TX_ABORT };
  // -------------------------------------------------------------------------------------
  u16 size;
  TYPE type;
  DTID dt_id;
  LID lsn;
  LID gsn;
  PID pid;
  u8 payload[];
};
// -------------------------------------------------------------------------------------
struct WAL {
  static constexpr u64 WAL_SIZE = 1024 * 1024 * 1024 * 4ull;  // 20 * 5 MiB
  u8 buffer[WAL_SIZE];
  // -------------------------------------------------------------------------------------
  atomic<u64> wt_cursor = 0;  // Worker thread cursor
  atomic<u64> ww_cursor = 0;  // WAL Writer cursor
  // -------------------------------------------------------------------------------------
  LID lsn_counter = 0;
  LID max_gsn = 0;
  // -------------------------------------------------------------------------------------
  inline WALEntry& reserve(u16 requested_size)
  {
    u16 size = requested_size + sizeof(WALEntry);
    ensure((WAL_SIZE - wt_cursor) >= size);
    while ((WAL_SIZE - wt_cursor) < size) {
    }  // Spin until we have enough space
    return *reinterpret_cast<WALEntry*>(buffer + wt_cursor);
  }
  // -------------------------------------------------------------------------------------
  inline void submit(u16 reserved_size) { wt_cursor.store(wt_cursor + reserved_size + sizeof(WALEntry), std::memory_order_release); }
};
// -------------------------------------------------------------------------------------
struct Transaction {
  enum class TYPE : u8 { USER, SYSTEM };
  enum class STATE { IDLE, STARTED, COMMITED, ABORTED };
  STATE state = STATE::IDLE;
  u64 tx_id = 0;
  LID start_gsn = 0, current_gsn = 0;
  DTID current_dt_id = -1;
};
// -------------------------------------------------------------------------------------
struct Partition {
  template <typename T>
  class WALEntryHandler
  {
   private:
    Partition* partition;

   public:
    u8* entry;
    u16 requested_size;
    inline T* operator->() { return reinterpret_cast<T*>(entry); }
    WALEntryHandler() = default;
    WALEntryHandler(Partition& p, u8* entry, u16 size) : partition(&p), entry(entry), requested_size(size) {}
    void submit() { partition->submitDTEntry(requested_size); }
  };
  // -------------------------------------------------------------------------------------
  u16 partition_id;  // thread_id * 2
  Transaction user_tx, system_tx;
  Transaction* active_tx = &user_tx;
  WAL wal;
  Partition(u64 partition_id) : partition_id(partition_id) {}
  // -------------------------------------------------------------------------------------
  inline Transaction& tx() { return *active_tx; }
  // -------------------------------------------------------------------------------------
  inline LID getCurrentGSN() { return tx().current_gsn; }
  inline void setCurrentGSN(LID gsn)
  {
    tx().current_gsn = gsn;
    if (tx().start_gsn == 0)
      tx().start_gsn = gsn;
  }
  // -------------------------------------------------------------------------------------
  template <typename T>
  WALEntryHandler<T> reserveDTEntry(PID pid, DTID dt_id, LID gsn, u16 requested_size)
  {
    WALEntry& entry = wal.reserve(requested_size);
    entry.size = sizeof(WALEntry) + requested_size;
    entry.lsn = wal.lsn_counter++;
    entry.dt_id = dt_id;
    entry.pid = pid;
    entry.gsn = gsn;
    entry.type = WALEntry::TYPE::DT_SPECIFIC;
    return {*this, entry.payload, requested_size};
  }
  inline void submitDTEntry(u16 requested_size) { wal.submit(requested_size); }
  // -------------------------------------------------------------------------------------
  void startTX(Transaction::TYPE tx_type = Transaction::TYPE::USER);
  void commitTX();
  void abortTX();
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// MUST be allocated dynamically
class CRMG
{
  struct TLSHandler {
    Partition* p;
    TLSHandler() { p = CRMG::registerThread(); }
    ~TLSHandler() { CRMG::removeThread(p); }
  };

 public:
  static thread_local TLSHandler tls_handler;
  static std::mutex mutex;
  static std::set<Partition*> all_threads;
  static u64 partitions_counter;
  // -------------------------------------------------------------------------------------
  CRMG();
  ~CRMG();
  // -------------------------------------------------------------------------------------
  static Partition* registerThread();
  static void removeThread(Partition*);
  // -------------------------------------------------------------------------------------
  inline static Partition& my()
  {
    assert(tls_handler.p != nullptr);
    return *tls_handler.p;
  }
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
