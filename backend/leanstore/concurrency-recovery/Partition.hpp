#pragma once
#include "Transaction.hpp"
#include "WAL.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct Partition {
  template <typename T>
  class WALEntryHandler
  {
   private:
    Partition* partition;

   public:
    u8* entry;
    u64 requested_size;
    inline T* operator->() { return reinterpret_cast<T*>(entry); }
    WALEntryHandler() = default;
    WALEntryHandler(Partition& p, u8* entry, u64 size) : partition(&p), entry(entry), requested_size(size) {}
    void submit() { partition->submitDTEntry(requested_size); }
  };
  // -------------------------------------------------------------------------------------
  u16 partition_id;  // thread_id * 2
  LID current_gsn;   // Clock
  Transaction user_tx, system_tx;
  Transaction* active_tx = &user_tx;
  WAL wal;
  // -------------------------------------------------------------------------------------
  std::mutex commit_mutex;
  atomic<bool> waiting_for_commit_signal = false;
  atomic<bool> _signal = false;
  // -------------------------------------------------------------------------------------
  Partition(u64 partition_id);
  ~Partition();
  // -------------------------------------------------------------------------------------
  inline Transaction& tx() { return *active_tx; }
  // -------------------------------------------------------------------------------------
  inline LID getCurrentGSN() { return current_gsn; }
  inline void setCurrentGSN(LID gsn) { current_gsn = gsn; }
  // -------------------------------------------------------------------------------------
  template <typename T>
  WALEntryHandler<T> reserveDTEntry(PID pid, DTID dt_id, LID gsn, u64 requested_size)
  {
    WALEntry& entry = *reinterpret_cast<WALEntry*>(wal.reserve(sizeof(WALEntry) + requested_size));
    entry.size = sizeof(WALEntry) + requested_size;
    entry.lsn = wal.lsn_counter++;
    entry.dt_id = dt_id;
    entry.pid = pid;
    entry.gsn = gsn;
    entry.type = WALEntry::TYPE::DT_SPECIFIC;
    return {*this, entry.payload, requested_size};
  }
  inline void submitDTEntry(u64 requested_size) { wal.submit(requested_size); }
  // -------------------------------------------------------------------------------------
  void startTX(Transaction::TYPE tx_type = Transaction::TYPE::USER);
  void commitTX();
  void abortTX();
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
