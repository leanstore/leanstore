#pragma once
#include "Transaction.hpp"
#include "WALEntry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct WTTS {
   u8 worker_id : 8;
   u64 tts : 56;
};
struct WLSN {
   u8 worker_id : 8;
   u64 lsn : 56;
   WLSN(u8 worker_id, u64 lsn) : worker_id(worker_id), lsn(lsn) {}
};
static_assert(sizeof(WTTS) == sizeof(u64), "");
static_assert(sizeof(WLSN) == sizeof(u64), "");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
struct alignas(512) WALChunk {
   static constexpr u16 STATIC_MAX_WORKERS = 256;
   struct Slot {
      u64 offset;
      u64 length;
   };
   u8 workers_count;
   u32 total_size;
   Slot slot[STATIC_MAX_WORKERS];
   u8 data[];
};
// -------------------------------------------------------------------------------------
struct Worker {
   // Static
   static thread_local Worker* tls_ptr;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   const s32 ssd_fd;
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, s32 fd);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   // -------------------------------------------------------------------------------------
   u64 next_tts = 0;
   // Shared with all workers
   u8 padding_1[64];
   atomic<u64> high_water_mark = 0;  // High water mark, exclusive: TS < mark are visible
   u8 padding_2[64];
   // -------------------------------------------------------------------------------------
   // Garbage Collect (is_removed) when the is_removed version visible for all
   u64 lower_water_mark = 0;  // Safe to garbage collect
   atomic<u64>* lower_water_marks;
   struct TODO {
      u64 tts;
      LID lsn;
      u64 in_memory_offset;
   };
   std::queue<TODO> todo_list;
   void addTODO(u64 tts, LID lsn, u32 in_memory_offset);
   // -------------------------------------------------------------------------------------
   unique_ptr<u64[]> my_snapshot;
   // -------------------------------------------------------------------------------------
   // Protect W+GCT shared data (worker <-> group commit thread)
   // -------------------------------------------------------------------------------------
   // Accessible only by the group commit thread
   struct GroupCommitData {
      u64 ready_to_commit_cut = 0;  // Exclusive ) == size
      u64 max_safe_gsn_to_commit = std::numeric_limits<u64>::max();
      LID gsn_to_flush;
      u64 wt_cursor_to_flush;
      LID first_lsn_in_chunk;
      bool skip = false;
   };
   GroupCommitData group_commit_data;
   // -------------------------------------------------------------------------------------
   // Shared between Group Committer and Worker
   std::mutex worker_group_commiter_mutex;
   std::vector<Transaction> ready_to_commit_queue;
   struct WALFinder {
      std::mutex m;
      std::map<LID, WALChunk::Slot> ht;  // LSN->SSD Offset
      void insertJumpPoint(LID lsn, WALChunk::Slot slot);
      WALChunk::Slot getJumpPoint(LID lsn);
      ~WALFinder();
   };
   WALFinder wal_finder;
   // -------------------------------------------------------------------------------------
   static constexpr s64 WORKER_WAL_SIZE = 1024 * 1024 * 10;
   static constexpr s64 CR_ENTRY_SIZE = sizeof(WALMetaEntry);
   // -------------------------------------------------------------------------------------
   // Published using mutex
   atomic<u64> wal_wt_cursor = 0;  // W->GCT
   atomic<LID> wal_max_gsn = 0;    // W->GCT, under mutex
   u64 wal_buffer_round = 0, wal_next_to_clean = 0;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   atomic<u64> wal_ww_cursor = 0;                // GCT->W
   alignas(512) u8 wal_buffer[WORKER_WAL_SIZE];  // W->GCT
   LID wal_lsn_counter = 0;
   LID clock_gsn;
   // -------------------------------------------------------------------------------------
   u32 walFreeSpace();
   u32 walContiguousFreeSpace();
   void walEnsureEnoughSpace(u32 requested_size);
   u8* walReserve(u32 requested_size);
   void invalidateEntriesUntil(u64 until);
   // -------------------------------------------------------------------------------------
   // Iterate over current TX entries
   u64 current_tx_wal_start;
   void iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback);
   // -------------------------------------------------------------------------------------
   Transaction active_tx;
   WALMetaEntry* active_mt_entry;
   WALDTEntry* active_dt_entry;
   // -------------------------------------------------------------------------------------
  private:
   // Without Payload, by submit no need to update clock (gsn)
   WALMetaEntry& reserveWALMetaEntry();
   void submitWALMetaEntry();

  public:
   // -------------------------------------------------------------------------------------
   template <typename T>
   class WALEntryHandler
   {
     public:
      u8* entry;
      u64 total_size;
      u64 lsn;
      u32 in_memory_offset;
      inline T* operator->() { return reinterpret_cast<T*>(entry); }
      inline T& operator*() { return *reinterpret_cast<T*>(entry); }
      WALEntryHandler() = default;
      WALEntryHandler(u8* entry, u64 size, u64 lsn, u64 in_memory_offset)
          : entry(entry), total_size(size), lsn(lsn), in_memory_offset(in_memory_offset)
      {
      }
      void submit() { cr::Worker::my().submitDTEntry(total_size); }
   };
   // -------------------------------------------------------------------------------------
   template <typename T>
   WALEntryHandler<T> reserveDTEntry(u64 requested_size, PID pid, LID gsn, DTID dt_id)
   {
      const auto lsn = wal_lsn_counter++;
      const u64 total_size = sizeof(WALDTEntry) + requested_size;
      ensure(walContiguousFreeSpace() >= total_size);
      // Sync
      invalidateEntriesUntil(wal_wt_cursor + total_size);
      active_dt_entry = new (wal_buffer + wal_wt_cursor) WALDTEntry();
      active_dt_entry->lsn.store(lsn, std::memory_order_release);
      active_dt_entry->magic_debugging_number = 99;
      active_dt_entry->type = WALEntry::TYPE::DT_SPECIFIC;
      active_dt_entry->size = total_size;
      // -------------------------------------------------------------------------------------
      active_dt_entry->pid = pid;
      active_dt_entry->gsn = gsn;
      active_dt_entry->dt_id = dt_id;
      return {active_dt_entry->payload, total_size, active_dt_entry->lsn, wal_wt_cursor};
   }
   void submitDTEntry(u64 total_size);
   // -------------------------------------------------------------------------------------
  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX();
   void commitTX();
   void abortTX();
   bool isVisibleForMe(u8 worker_id, u64 tts);
   bool isVisibleForMe(u64 tts);
   inline LID getCurrentGSN() { return clock_gsn; }
   inline void setCurrentGSN(LID gsn) { clock_gsn = gsn; }
   // -------------------------------------------------------------------------------------
   void refreshSnapshot();
   // -------------------------------------------------------------------------------------
   void getWALEntry(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALEntry(LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALDTEntryPayload(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(u8*)> callback);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
