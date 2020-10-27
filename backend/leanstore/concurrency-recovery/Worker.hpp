#pragma once
#include "Transaction.hpp"
#include "WALEntry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <mutex>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct Worker {
   // Static
   static atomic<u64> central_tts;
   static thread_local Worker* tls_ptr;
   // -------------------------------------------------------------------------------------
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count);
   ~Worker();
   // -------------------------------------------------------------------------------------
   // Shared with all workers
   atomic<u64> active_tts = 0;  // High water mark:  < active_tts are either committed or aborted
   // -------------------------------------------------------------------------------------
   // Protect W+GCT shared data (worker <-> group commit thread)
   std::mutex worker_group_commiter_mutex;
   // -------------------------------------------------------------------------------------
   std::vector<Transaction> ready_to_commit_queue;
   // -------------------------------------------------------------------------------------
   // Accessible only by the group commit thread
   struct GroupCommitData {
      u64 ready_to_commit_cut = 0;  // Exclusive ) == size
      u64 max_safe_gsn_to_commit = std::numeric_limits<u64>::max();
      LID gsn_to_flush;
      u64 wt_cursor_to_flush;
      u64 bytes_to_ignore_in_the_next_round = 0;
   };
   GroupCommitData group_commit_data;
   // -------------------------------------------------------------------------------------
   LID clock_gsn;
   // -------------------------------------------------------------------------------------
   // START WAL
   // -------------------------------------------------------------------------------------
   static constexpr s64 WORKER_WAL_SIZE = 1024 * 1024 * 10;
   static constexpr s64 CR_ENTRY_SIZE = sizeof(WALEntry);
   // -------------------------------------------------------------------------------------
   // Published using mutex
   atomic<u64> wal_wt_cursor;  // W->GCT
   atomic<LID> wal_max_gsn;    // W->GCT, under mutex
   // -------------------------------------------------------------------------------------
   atomic<u64> wal_ww_cursor;                    // GCT->W
   alignas(512) u8 wal_buffer[WORKER_WAL_SIZE];  // W->GCT
   LID wal_lsn_counter = 0;
   // -------------------------------------------------------------------------------------
   u32 walFreeSpace();
   u32 walContiguousFreeSpace();
   void walEnsureEnoughSpace(u32 requested_size);
   u8* walReserve(u32 requested_size);
   // -------------------------------------------------------------------------------------
   // END WAL
   // -------------------------------------------------------------------------------------
   static inline Worker& my() { return *Worker::tls_ptr; }
   u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   // -------------------------------------------------------------------------------------
   Transaction tx;
   WALEntry* active_entry;
   // -------------------------------------------------------------------------------------
   inline LID getCurrentGSN() { return clock_gsn; }
   inline void setCurrentGSN(LID gsn) { clock_gsn = gsn; }
   // -------------------------------------------------------------------------------------
  private:
   // Without Payload, by submit no need to update clock (gsn)
   WALEntry& reserveWALEntry();
   void submitWALEntry();

  public:
   // -------------------------------------------------------------------------------------
   void startTX();
   void commitTX();
   void abortTX();
   // -------------------------------------------------------------------------------------
   template <typename T>
   class WALEntryHandler
   {
     public:
      u8* entry;
      u64 total_size;
      inline T* operator->() { return reinterpret_cast<T*>(entry); }
      inline T& operator*() { return *reinterpret_cast<T*>(entry); }
      WALEntryHandler() = default;
      WALEntryHandler(u8* entry, u64 size) : entry(entry), total_size(size) {}
      void submit() { cr::Worker::my().submitDTEntry(total_size); }
   };
   // -------------------------------------------------------------------------------------
   template <typename T>
   WALEntryHandler<T> reserveDTEntry(PID pid, DTID dt_id, LID gsn, u64 requested_size)
   {
      const u64 total_size = sizeof(WALEntry) + requested_size;
      ensure(walContiguousFreeSpace() >= total_size);
      active_entry = reinterpret_cast<WALEntry*>(wal_buffer + wal_wt_cursor);
      active_entry->size = sizeof(WALEntry) + requested_size;
      active_entry->lsn = wal_lsn_counter++;
      active_entry->dt_id = dt_id;
      active_entry->pid = pid;
      active_entry->gsn = gsn;
      active_entry->type = WALEntry::TYPE::DT_SPECIFIC;
      return {active_entry->payload, total_size};
   }
   void submitDTEntry(u64 requested_size);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
