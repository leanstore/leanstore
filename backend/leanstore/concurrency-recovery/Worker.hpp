#pragma once
#include "HistoryTreeInterface.hpp"
#include "Transaction.hpp"
#include "WALEntry.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/OptimisticSpinStruct.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
static constexpr u16 STATIC_MAX_WORKERS = std::numeric_limits<WORKERID>::max();
// -------------------------------------------------------------------------------------
// Abbreviations: WT (Worker Thread), GCT (Group Commit Thread or whoever writes the WAL)
// Stages: pre-committed (SI passed) -> hardened (its own WALs are written and fsync) -> committed/signaled (all dependencies are flushed too and the
// user got the OK)
struct Worker {
   // Static members
   static thread_local Worker* tls_ptr;
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
   struct Logging {
      static atomic<u64> global_min_gsn_flushed;   // The minimum of all workers maximum flushed GSN
      static atomic<u64> global_sync_to_this_gsn;  // Artifically increment the workers GSN to this point at the next round to prevent GSN from
                                                   // skewing and undermining RFA
      static atomic<u64> global_min_commit_ts_flushed;
      // -------------------------------------------------------------------------------------
      WALMetaEntry* active_mt_entry;
      WALDTEntry* active_dt_entry;
      // Shared between Group Committer and Worker
      std::mutex precommitted_queue_mutex;
      std::vector<Transaction> precommitted_queue;
      std::vector<Transaction> precommitted_queue_rfa;
      // -------------------------------------------------------------------------------------
      std::atomic<TXID> hardened_commit_ts = 0, signaled_commit_ts = 0;  // W: LW, R: WT
      std::atomic<TXID> hardened_gsn = 0;                                // W: LW, R: LC
      // -------------------------------------------------------------------------------------
      // Protect W+GCT shared data (worker <-> group commit thread)
      struct WorkerToLW {
         u64 version = 0;
         LID last_gsn = 0;
         u64 wal_written_offset = 0;
         TXID precommitted_tx_start_ts = 0;
         TXID precommitted_tx_commit_ts = 0;
      };
      utils::OptimisticSpinStruct<WorkerToLW> wt_to_lw;
      // New: RFA: check for user tx dependency on tuple insert, update, lookup. Deletes are treated as system transaction
      std::vector<std::tuple<WORKERID, TXID>> rfa_checks_at_precommit;
      void checkLogDepdency(WORKERID other_worker_id, TXID other_user_tx_id)
      {
         if (!remote_flush_dependency && my().worker_id != other_worker_id) {
            if (other(other_worker_id).signaled_commit_ts < other_user_tx_id) {
               rfa_checks_at_precommit.push_back({other_worker_id, other_user_tx_id});
            }
         }
      }
      // -------------------------------------------------------------------------------------
      // Accessible only by the group commit thread
      u64 wal_wt_cursor = 0;
      u64 wal_buffer_round = 0, wal_next_to_clean = 0;
      // -------------------------------------------------------------------------------------
      atomic<u64> wal_gct_cursor = 0;               // GCT->W
      alignas(512) u8 wal_buffer[WORKER_WAL_SIZE];  // W->GCT
      LID wal_lsn_counter = 0;
      LID wt_gsn_clock;
      LID rfa_gsn_flushed;
      bool remote_flush_dependency = false;
      // -------------------------------------------------------------------------------------
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
         void submit() { cr::Worker::my().logging.submitDTEntry(total_size); }
      };
      // -------------------------------------------------------------------------------------
      template <typename T>
      WALEntryHandler<T> reserveDTEntry(u64 requested_size, PID pid, LID gsn, DTID dt_id)
      {
         const auto lsn = wal_lsn_counter++;
         const u64 total_size = sizeof(WALDTEntry) + requested_size;
         ensure(walContiguousFreeSpace() >= total_size);
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
      void publishOffset() { wt_to_lw.updateAttribute(&WorkerToLW::wal_written_offset, wal_wt_cursor); }
      void publishMaxGSNOffset()
      {
         auto current = wt_to_lw.getNoSync();
         current.wal_written_offset = wal_wt_cursor;
         current.last_gsn = wt_gsn_clock;
         wt_to_lw.pushSync(current);
      }
      std::tuple<LID, u64> fetchMaxGSNOffset()
      {
         const auto current = wt_to_lw.getSync();
         return {current.last_gsn, current.wal_written_offset};
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
      // Without Payload, by submit no need to update clock (gsn)
      WALMetaEntry& reserveWALMetaEntry();
      void submitWALMetaEntry();
      inline LID getCurrentGSN() { return wt_gsn_clock; }
      inline void setCurrentGSN(LID gsn) { wt_gsn_clock = gsn; }
      // -------------------------------------------------------------------------------------
      Logging& other(WORKERID other_worker_id) { return my().all_workers[other_worker_id]->logging; }
   } logging;
   // -------------------------------------------------------------------------------------
   // Concurrency Control
   // LWM: start timestamp of the transaction that has its effect visible by all in its class
   struct ConcurrencyControl {
      static atomic<u64> global_clock;
      // -------------------------------------------------------------------------------------
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
      // -------------------------------------------------------------------------------------
      // WiredTiger/PG/MySQL variant
      struct {
         std::vector<TXID> local_workers_tx_id;  // ReadView Vector
         TXID current_snapshot_min_tx_id;
         TXID current_snapshot_max_tx_id;
         atomic<TXID> snapshot_min_tx_id = 0;
      } wt_pg;
      // -------------------------------------------------------------------------------------
      // LeanStore NoSteal
      // Nothing for now
      // -------------------------------------------------------------------------------------
      // Commmit Tree (single-writer multiple-reader)
      struct CommitTree {
         u64 capacity;
         std::unique_ptr<std::pair<TXID, TXID>[]> array;
         std::shared_mutex mutex;
         u64 cursor = 0;
         void cleanIfNecessary();
         TXID commit(TXID start_ts);
         std::optional<std::pair<TXID, TXID>> LCBUnsafe(TXID start_ts);
         TXID LCB(TXID start_ts);
         CommitTree()
         {
            capacity = cr::Worker::my().workers_count;
            assert(capacity);
         }
      };
      CommitTree commit_tree_a;
      // -------------------------------------------------------------------------------------
      // Clean up state
      u64 cleaned_untill_oltp_lwm = 0;
      // -------------------------------------------------------------------------------------
      HistoryTreeInterface& history_tree;
      // -------------------------------------------------------------------------------------
      void garbageCollection();
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
      // -------------------------------------------------------------------------------------
      ConcurrencyControl& other(WORKERID other_worker_id) { return my().all_workers[other_worker_id]->cc; }
      // -------------------------------------------------------------------------------------
      inline u64 insertVersion(DTID dt_id, bool is_remove, u64 payload_length, std::function<void(u8*)> cb)
      {
         utils::Timer timer(CRCounters::myCounters().cc_ms_history_tree);
         const u64 new_command_id = (my().command_id++) | ((is_remove) ? TYPE_MSB(COMMANDID) : 0);
         history_tree.insertVersion(my().worker_id, my().active_tx.startTS(), new_command_id, dt_id, is_remove, payload_length, cb);
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
      }  // -------------------------------------------------------------------------------------
      ConcurrencyControl(HistoryTreeInterface& ht) : history_tree(ht) {}
   } cc;
   // -------------------------------------------------------------------------------------
   u64 command_id = 0;
   Transaction active_tx;
   // -------------------------------------------------------------------------------------
   const u64 worker_id;
   Worker** all_workers;
   const u64 workers_count;
   const s32 ssd_fd;
   const bool is_page_provider = false;
   // -------------------------------------------------------------------------------------
   Worker(u64 worker_id, Worker** all_workers, u64 workers_count, HistoryTreeInterface& versions_space, s32 fd, const bool is_page_provider = false);
   static inline Worker& my() { return *Worker::tls_ptr; }
   ~Worker();
   // -------------------------------------------------------------------------------------
   // Shared with all workers

  public:
   // -------------------------------------------------------------------------------------
   inline WORKERID workerID() { return worker_id; }

  public:
   // -------------------------------------------------------------------------------------
   // TX Control
   void startTX(TX_MODE next_tx_type = TX_MODE::OLTP,
                TX_ISOLATION_LEVEL next_tx_isolation_level = TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
                bool read_only = false);
   void commitTX();
   void abortTX();
   void shutdown();
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
