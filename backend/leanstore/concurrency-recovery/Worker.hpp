#pragma once
#include "Transaction.hpp"
#include "VersionsSpaceInterface.hpp"
#include "WALEntry.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/RingBufferST.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <list>
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
static constexpr u16 STATIC_MAX_WORKERS = 256;
struct alignas(512) WALChunk {
   struct Slot {
      u64 offset;
      u64 length;
   };
   u8 workers_count;
   u32 total_size;
   Slot slot[STATIC_MAX_WORKERS];
};
// -------------------------------------------------------------------------------------
struct Worker {
   // Static
   static thread_local Worker* tls_ptr;
   // -------------------------------------------------------------------------------------
   static atomic<u64> global_logical_clock;
   static atomic<u64> global_gsn_flushed;       // The minimum of all workers maximum flushed GSN
   static atomic<u64> global_sync_to_this_gsn;  // Artifically increment the workers GSN to this point at the next round to prevent GSN from skewing
                                                // and undermining RFA
   static std::mutex global_mutex;
   // -------------------------------------------------------------------------------------
   static unique_ptr<atomic<u64>[]> global_workers_in_progress_txid;
   static unique_ptr<atomic<u64>[]> global_workers_oltp_lwm;
   static unique_ptr<atomic<u64>[]> global_workers_olap_lwm;
   static atomic<u64> global_oltp_lwm;  // TODO:
   static atomic<u64> global_olap_lwm;  // TODO:
                                        // -------------------------------------------------------------------------------------

   // -------------------------------------------------------------------------------------
   // All the local tracking data
   u64 local_oltp_lwm;
   u64 local_olap_lwm;
   u64 cleaned_untill_oltp_lwm = 0;
   u64 cleaned_untill_olap_lwm = 0;
   u64 command_id = 0;
   Transaction active_tx;
   WALMetaEntry* active_mt_entry;
   WALDTEntry* active_dt_entry;
   // Snapshot Acquisition Time (SAT) = TXID = CommitMark - 1
   bool force_si_refresh = false;
   bool workers_sorted = false;
   bool transactions_order_refreshed = false;
   u64 local_oldest_olap_tx_id;
   u64 local_oldest_olap_tx_worker_id;
   u64 local_oldest_oltp_tx_id;  // OLAP <= OLTP
   unique_ptr<atomic<u64>[]> local_workers_in_progress_txids;
   // local_workers_sta can lag and it only tells us whether "it" definitely sees a version, but not if it does not
   unique_ptr<u64[]> local_workers_sorted_txids;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   VersionsSpaceInterface& versions_space;
   const s32 ssd_fd;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   static constexpr u64 WORKERS_BITS = 8;
   static constexpr u64 WORKERS_INCREMENT = 1ull << WORKERS_BITS;
   static constexpr u64 WORKERS_MASK = (1ull << WORKERS_BITS) - 1;
   static constexpr u64 OLTP_OLAP_SAME_BIT = (1ull << 63);
   static constexpr u64 RC_BIT = (1ull << 63);
   static constexpr u64 OLAP_BIT = (1ull << 62);
   static constexpr u64 CLEAN_BITS_MASK = ~(OLAP_BIT | RC_BIT);
   // -------------------------------------------------------------------------------------
   // Temporary helpers
   static std::tuple<u8, u64> decomposeWIDCM(u64 widcm)
   {
      u8 worker_id = widcm & WORKERS_MASK;
      u64 worker_commit_mark = widcm >> WORKERS_BITS;
      return {worker_id, worker_commit_mark};
   }
   static u64 composeWIDCM(u8 worker_id, u64 worker_commit_mark)
   {
      u64 widcm = worker_commit_mark << WORKERS_BITS;
      widcm |= worker_id;
      return widcm;
   }
   // -------------------------------------------------------------------------------------
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, VersionsSpaceInterface& versions_space, s32 fd);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   // -------------------------------------------------------------------------------------
   // Shared with all workers
   // -------------------------------------------------------------------------------------
   struct TODOEntry {  // In-memory
      WORKERID worker_id;
      TXID tx_id;
      TXID commit_tts;
      TXID or_before_tx_id;
      DTID dt_id;  // max value -> purge the whole tx
      u64 payload_length;
      // -------------------------------------------------------------------------------------
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   // 2PL unlock datastructures
   struct UnlockTask {
      DTID dt_id;
      u64 payload_length;
      u8 payload[];
      UnlockTask(DTID dt_id, u64 payload_length) : dt_id(dt_id), payload_length(payload_length) {}
   };
   std::vector<std::unique_ptr<u8[]>> unlock_tasks_after_commit;
   void addUnlockTask(DTID dt_id, u64 payload_length, std::function<void(u8* dst)> callback);
   void executeUnlockTasks();
   // -------------------------------------------------------------------------------------
   // Optimization: remove relations from snapshot as soon as we are finished with them (esp. in long read-only tx)
   static constexpr u64 MAX_RELATIONS_COUNT = 128;
   struct RelationsList {
      std::atomic<u64> count = 0;
      std::atomic<DTID> dt_ids[MAX_RELATIONS_COUNT];
      void add(DTID dt_id)
      {
         const u64 current_index = count.load();
         assert((current_index + 1) < MAX_RELATIONS_COUNT);
         dt_ids[current_index].store(dt_id, std::memory_order_release);
         count.store(current_index + 1, std::memory_order_release);
      }
      void reset() { count.store(0, std::memory_order_release); }
   };
   RelationsList relations_cut_from_snapshot;
   // -------------------------------------------------------------------------------------
   // Protect W+GCT shared data (worker <-> group commit thread)
   // -------------------------------------------------------------------------------------
   // Accessible only by the group commit thread
   struct GroupCommitData {
      u64 ready_to_commit_cut = 0;  // Exclusive ) == size
      u64 max_safe_gsn_to_commit = std::numeric_limits<u64>::max();
      LID gsn_to_flush;        // Will flush up to this GSN when the current round is over
      u64 wt_cursor_to_flush;  // Will flush up to this GSN when the current round is over
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
   u8 pad3[64];
   // The following three atomics are used to publish state changes from worker to GCT
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
         wal_gct_max_gsn_0.store(clock_gsn, std::memory_order_release);
         msb = 0;
      } else {
         wal_gct_max_gsn_1.store(clock_gsn, std::memory_order_release);
         msb = 1ull << 63;
      }
      wal_gct.store(wal_wt_cursor | msb, std::memory_order_release);
   }
   std::tuple<LID, u64> fetchMaxGSNOffset()
   {
      const u64 worker_atomic = wal_gct.load();
      LID gsn;
      if (worker_atomic & (1ull << 63)) {
         gsn = wal_gct_max_gsn_1.load();
      } else {
         gsn = wal_gct_max_gsn_0.load();
      }
      const u64 max_gsn = worker_atomic & (~(1ull << 63));
      return {gsn, max_gsn};
   }
   // -------------------------------------------------------------------------------------
   u64 wal_wt_cursor = 0;
   u64 wal_buffer_round = 0, wal_next_to_clean = 0;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   atomic<u64> wal_gct_cursor = 0;               // GCT->W
   alignas(512) u8 wal_buffer[WORKER_WAL_SIZE];  // W->GCT
   LID wal_lsn_counter = 0;
   LID clock_gsn;
   LID rfa_gsn_flushed;
   bool needs_remote_flush = false;
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
   inline u8 workerID() { return worker_id; }
   inline u64 snapshotAcquistionTime() { return active_tx.TTS(); }  // SAT

  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX(TX_MODE next_tx_type = TX_MODE::OLTP,
                TX_ISOLATION_LEVEL next_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
                bool read_only = false);
   void commitTX();
   void abortTX();
   void checkup();
   void shutdown();
   // -------------------------------------------------------------------------------------
   inline LID getCurrentGSN() { return clock_gsn; }
   inline void setCurrentGSN(LID gsn) { clock_gsn = gsn; }
   // -------------------------------------------------------------------------------------
   void sortWorkers();
   void refreshSnapshotHWMs();
   void switchToAlwaysUpToDateMode();
   bool isVisibleForAll(u64 commited_before_so);
   bool isVisibleForIt(u8 whom_worker_id, u8 what_worker_id, u64 tts);
   bool isVisibleForMe(u8 worker_id, u64 tts, bool to_write = true);
   bool isVisibleForMe(u64 tts);
   // -------------------------------------------------------------------------------------
   void getWALEntry(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALEntry(LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback);
   void getWALDTEntryPayload(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(u8*)> callback);
};
// -------------------------------------------------------------------------------------
// Shortcuts
inline Transaction& activeTX()
{
   return cr::Worker::my().active_tx;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
