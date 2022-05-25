#pragma once
#include "HistoryTreeInterface.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
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
static constexpr u16 STATIC_MAX_WORKERS = std::numeric_limits<WORKERID>::max();
struct alignas(512) WALChunk {
   struct Slot {
      u64 offset;
      u64 length;
   };
   WORKERID workers_count;
   u32 total_size;
   Slot slot[STATIC_MAX_WORKERS];
};
// -------------------------------------------------------------------------------------
// Stages: pre-committed (SI passed) -> hardened (its own WALs are written and fsync) -> committed/signaled (all dependencies are flushed too and the
// user got the OK)
struct Worker {
   // Static members
   static thread_local Worker* tls_ptr;
   // Logging
   static atomic<u64> global_gsn_flushed;       // The minimum of all workers maximum flushed GSN
   static atomic<u64> global_sync_to_this_gsn;  // Artifically increment the workers GSN to this point at the next round to prevent GSN from
                                                // skewing and undermining RFA
   static atomic<u64> global_logical_clock;
   // -------------------------------------------------------------------------------------
   // Concurrency Control
   static unique_ptr<atomic<u64>[]> global_workers_current_snapshot;
   static atomic<TXID> global_oldest_oltp_start_ts, global_oltp_lwm;
   static atomic<TXID> global_oldest_all_start_ts, global_all_lwm;
   static atomic<TXID> global_newest_olap_start_ts;
   static std::shared_mutex global_mutex;
   // -------------------------------------------------------------------------------------
   static constexpr u64 WORKERS_BITS = 8;
   static constexpr u64 WORKERS_INCREMENT = 1ull << WORKERS_BITS;
   static constexpr u64 WORKERS_MASK = (1ull << WORKERS_BITS) - 1;
   static constexpr u64 LATCH_BIT = (1ull << 63);
   static constexpr u64 RC_BIT = (1ull << 62);
   static constexpr u64 OLAP_BIT = (1ull << 61);
   static constexpr u64 OLTP_OLAP_SAME_BIT = OLAP_BIT;
   static constexpr u64 CLEAN_BITS_MASK = ~(LATCH_BIT | OLAP_BIT | RC_BIT);
   // TXID : [LATCH_BIT | RC_BIT | OLAP_BIT | id];
   // LWM : [LATCH_BIT | RC_BIT | OLTP_OLAP_SAME_BIT | id];
   static constexpr s64 WORKER_WAL_SIZE = 1024 * 1024 * 10;
   static constexpr s64 CR_ENTRY_SIZE = sizeof(WALMetaEntry);
   // -------------------------------------------------------------------------------------
   // Worker Local
   struct {
      WALMetaEntry* active_mt_entry;
      WALDTEntry* active_dt_entry;
      // Shared between Group Committer and Worker
      std::mutex precommitted_queue_mutex;
      std::vector<Transaction> precommitted_queue;
      u8 pad1[64];
      std::atomic<u64> ready_to_commit_queue_size = 0;
      u8 pad2[64];
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
      u8 pad3[64];
      // The following three atomics are used to publish state changes from worker to GCT
      atomic<u64> wal_gct_max_gsn_0 = 0;
      atomic<u64> wal_gct_max_gsn_1 = 0;
      atomic<u64> wal_gct = 0;  // W->GCT
      u8 pad4[64];
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
   } logging;
   // -------------------------------------------------------------------------------------
   // Concurrency Control
   // LWM: start timestamp of the transaction that has its effect visible by all in its class
   struct {
      atomic<TXID> local_lwm_latch = 0;
      atomic<TXID> oltp_lwm_receiver;
      atomic<TXID> all_lwm_receiver;
      atomic<TXID> local_latest_write_tx = 0, local_latest_lwm_for_tx = 0;
      TXID local_all_lwm, local_oltp_lwm;
      leanstore::KVInterface* commit_tree;
      std::unique_ptr<u8[]> commit_tree_handler;
      TXID local_global_all_lwm_cache = 0;
      unique_ptr<TXID[]> local_snapshot_cache;  // = Readview
      unique_ptr<TXID[]> local_snapshot_cache_ts;
      unique_ptr<TXID[]> local_workers_start_ts;
      // -------------------------------------------------------------------------------------
      // Clean up state
      u64 cleaned_untill_oltp_lwm = 0;
   } cc;
   // -------------------------------------------------------------------------------------
   u64 command_id = 0;
   Transaction active_tx;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   HistoryTreeInterface& history_tree;
   const s32 ssd_fd;
   const bool is_page_provider = false;
   // -------------------------------------------------------------------------------------
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, HistoryTreeInterface& versions_space, s32 fd, const bool is_page_provider = false);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   // -------------------------------------------------------------------------------------
   // Shared with all workers
   // -------------------------------------------------------------------------------------
   inline u64 insertVersion(DTID dt_id, bool is_remove, u64 payload_length, std::function<void(u8*)> cb)
   {
      utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree);
      const u64 new_command_id = (command_id++) | ((is_remove) ? TYPE_MSB(COMMANDID) : 0);
      history_tree.insertVersion(worker_id, active_tx.TTS(), new_command_id, dt_id, is_remove, payload_length, cb);
      return new_command_id;
   }
   inline bool retrieveVersion(WORKERID its_worker_id,
                               TXID its_tx_id,
                               COMMANDID its_command_id,
                               std::function<void(const u8*, u64 payload_length)> cb)
   {
      utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree);
      const bool is_remove = its_command_id & TYPE_MSB(COMMANDID);
      const bool found = history_tree.retrieveVersion(its_worker_id, its_tx_id, its_command_id, is_remove, cb);
      return found;
   }
   // -------------------------------------------------------------------------------------
   void publishOffset()
   {
      const u64 msb = logging.wal_gct & MSB;
      logging.wal_gct.store(logging.wal_wt_cursor | msb, std::memory_order_release);
   }
   void publishMaxGSNOffset()
   {
      const bool was_second_slot = logging.wal_gct & MSB;
      u64 msb;
      if (was_second_slot) {
         logging.wal_gct_max_gsn_0.store(logging.clock_gsn, std::memory_order_release);
         msb = 0;
      } else {
         logging.wal_gct_max_gsn_1.store(logging.clock_gsn, std::memory_order_release);
         msb = MSB;
      }
      logging.wal_gct.store(logging.wal_wt_cursor | msb, std::memory_order_release);
   }
   std::tuple<LID, u64> fetchMaxGSNOffset()
   {
      const u64 worker_atomic = logging.wal_gct.load();
      LID gsn;
      if (worker_atomic & (MSB)) {
         gsn = logging.wal_gct_max_gsn_1.load();
      } else {
         gsn = logging.wal_gct_max_gsn_0.load();
      }
      const u64 max_gsn = worker_atomic & (~(MSB));
      return {gsn, max_gsn};
   }
   // -------------------------------------------------------------------------------------
   u32 walFreeSpace();
   u32 walContiguousFreeSpace();
   void walEnsureEnoughSpace(u32 requested_size);
   u8* walReserve(u32 requested_size);
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
      const auto lsn = logging.wal_lsn_counter++;
      const u64 total_size = sizeof(WALDTEntry) + requested_size;
      ensure(walContiguousFreeSpace() >= total_size);
      logging.active_dt_entry = new (logging.wal_buffer + logging.wal_wt_cursor) WALDTEntry();
      logging.active_dt_entry->lsn.store(lsn, std::memory_order_release);
      logging.active_dt_entry->magic_debugging_number = 99;
      logging.active_dt_entry->type = WALEntry::TYPE::DT_SPECIFIC;
      logging.active_dt_entry->size = total_size;
      // -------------------------------------------------------------------------------------
      logging.active_dt_entry->pid = pid;
      logging.active_dt_entry->gsn = gsn;
      logging.active_dt_entry->dt_id = dt_id;
      return {logging.active_dt_entry->payload, total_size, logging.active_dt_entry->lsn, logging.wal_wt_cursor};
   }
   void submitDTEntry(u64 total_size);
   // -------------------------------------------------------------------------------------
   inline WORKERID workerID() { return worker_id; }
   inline u64 snapshotAcquistionTime() { return active_tx.TTS(); }  // SAT

  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX(TX_MODE next_tx_type = TX_MODE::OLTP,
                TX_ISOLATION_LEVEL next_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
                bool read_only = false);
   void commitTX();
   void abortTX();
   void garbageCollection();
   void shutdown();
   // -------------------------------------------------------------------------------------
   inline LID getCurrentGSN() { return logging.clock_gsn; }
   inline void setCurrentGSN(LID gsn) { logging.clock_gsn = gsn; }
   // -------------------------------------------------------------------------------------
   void refreshGlobalState();
   void switchToReadCommittedMode();
   void switchToSnapshotIsolationMode();
   // -------------------------------------------------------------------------------------
   enum class VISIBILITY : u8 { VISIBLE_ALREADY, VISIBLE_NEXT_ROUND, UNDETERMINED };
   bool isVisibleForAll(WORKERID worker_id, TXID start_ts);
   bool isVisibleForMe(WORKERID worker_id, u64 tts, bool to_write = true);
   VISIBILITY isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, u64 tts);
   VISIBILITY isVisibleForIt(WORKERID whom_worker_id, TXID commit_ts);
   TXID getCommitTimestamp(WORKERID worker_id, TXID start_ts);
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
