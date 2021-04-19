#pragma once
#include "Transaction.hpp"
#include "WALEntry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <mutex>
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
   static atomic<u64> global_snapshot_clock;
   static std::mutex global_mutex;
   // -------------------------------------------------------------------------------------
   static unique_ptr<atomic<u64>[]> global_so_starts;
   static unique_ptr<atomic<u64>[]> global_tts;
   // -------------------------------------------------------------------------------------
   const u64 SO_LATCHED = std::numeric_limits<u64>::max();
   bool force_si_refresh = false;
   bool workers_sorted = false;
   u64 so_start;
   u64 oldest_so_start, oldest_so_start_worker_id;
   unique_ptr<atomic<u64>[]> snapshot;
   unique_ptr<u64[]> all_sorted_so_starts;
   unique_ptr<u64[]> all_so_starts;
   // -------------------------------------------------------------------------------------
   static constexpr u64 WORKERS_BITS = 8;
   static constexpr u64 WORKERS_INCREMENT = 1ull << WORKERS_BITS;
   static constexpr u64 WORKERS_MASK = (1ull << WORKERS_BITS) - 1;
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
   // -------------------------------------------------------------------------------------
   struct TODO {  // In-memory
      u8 version_worker_id;
      u64 version_tts;
      u64 commited_before_so;
      DTID dt_id;
      // -------------------------------------------------------------------------------------
      u8 entry[64];  // TODO: dyanmically allocating buffer is costly
   };
   std::list<TODO> todo_commited_queue, todo_staging_queue;  // TODO: optimize (no need for sync)
   void stageTODO(u8 worker_id, u64 tts, DTID dt_id, u64 size, std::function<void(u8* dst)> callback);
   void commitTODO(u8 worker_id, u64 tts, u64 commited_before_so, DTID dt_id, u64 size, std::function<void(u8* dst)> callback);
   void stageTODOs(u64 so);
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
   u8 pad1[64];
   std::atomic<u64> ready_to_commit_queue_size = 0;
   u8 pad2[64];
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
   u8 pad3[64];
   atomic<u64> wal_gct_max_gsn_0 = 0;
   atomic<u64> wal_gct_max_gsn_1 = 0;
   atomic<u64> wal_gct = 0;  // W->GCT
   u8 pad4[64];
   void publishOffset()
   {
      const u64 msb = wal_gct & (u64(1) << 63);
      wal_gct.store(wal_wt_cursor | msb, std::memory_order_release);
   }
   void publishMaxGSNOffset()
   {
      const bool was_second_slot = wal_gct & (u64(1) << 63);
      u64 msb;
      if (was_second_slot) {
         wal_gct_max_gsn_0.store(wal_max_gsn, std::memory_order_release);
         msb = 0;
      } else {
         wal_gct_max_gsn_1.store(wal_max_gsn, std::memory_order_release);
         msb = 1ull << 63;
      }
      wal_gct.store(wal_wt_cursor | msb, std::memory_order_release);
   }
   u64 wal_wt_cursor = 0;
   LID wal_max_gsn = 0;
   u64 wal_buffer_round = 0, wal_next_to_clean = 0;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   atomic<u64> wal_gct_cursor = 0;               // GCT->W
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
   void checkup();
   void shutdown();
   // -------------------------------------------------------------------------------------
   inline LID getCurrentGSN() { return clock_gsn; }
   inline void setCurrentGSN(LID gsn) { clock_gsn = gsn; }
   // -------------------------------------------------------------------------------------
   void sortWorkers();
   void refreshSnapshot();
   bool isVisibleForAll(u8 worker_id, u64 tts);
   bool isVisibleForIt(u8 whom_worker_id, u8 what_worker_id, u64 tts);
   bool isVisibleForMe(u8 worker_id, u64 tts);
   bool isVisibleForMe(u64 tts);
   // -------------------------------------------------------------------------------------
   // Experimentell
   bool isVisibleForItCommitedBeforeSO(u8 whom_worker_id, u64 cb_so) { return all_so_starts[whom_worker_id] > cb_so; }
   u64 getCB(u8 from_worker_id, u64 ca_so) { return (all_so_starts[from_worker_id] > ca_so) ? all_so_starts[from_worker_id] : 0; }
   // -------------------------------------------------------------------------------------
   void getWALEntry(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALEntry(LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALDTEntryPayload(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(u8*)> callback);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
