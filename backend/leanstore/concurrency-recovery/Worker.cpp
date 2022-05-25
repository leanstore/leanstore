#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <stdio.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <sstream>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tls_ptr = nullptr;
atomic<u64> Worker::global_logical_clock = WORKERS_INCREMENT;
atomic<u64> Worker::global_gsn_flushed = 0;
atomic<u64> Worker::global_sync_to_this_gsn = 0;
std::shared_mutex Worker::global_mutex;  // Unused
// -------------------------------------------------------------------------------------
std::unique_ptr<atomic<u64>[]> Worker::global_workers_current_snapshot;  // All transactions < are committed
atomic<u64> Worker::global_oldest_all_start_ts = 0;
atomic<u64> Worker::global_oldest_oltp_start_ts = 0;
atomic<u64> Worker::global_all_lwm = 0;
atomic<u64> Worker::global_oltp_lwm = 0;
atomic<u64> Worker::global_newest_olap_start_ts = 0;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, HistoryTreeInterface& history_tree, s32 fd, const bool is_page_provider)
    : worker_id(worker_id),
      all_workers(all_workers),
      workers_count(workers_count),
      history_tree(history_tree),
      ssd_fd(fd),
      is_page_provider(is_page_provider)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   std::memset(logging.wal_buffer, 0, WORKER_WAL_SIZE);
   if (!is_page_provider) {
      cc.local_snapshot_cache = make_unique<u64[]>(workers_count);
      cc.local_snapshot_cache_ts = make_unique<u64[]>(workers_count);
      cc.local_workers_start_ts = make_unique<u64[]>(workers_count + 1);
      global_workers_current_snapshot[worker_id] = 0;
   }
}
Worker::~Worker() = default;
// -------------------------------------------------------------------------------------
u32 Worker::walFreeSpace()
{
   // A , B , C : a - b + c % c
   const auto gct_cursor = logging.wal_gct_cursor.load();
   if (gct_cursor == logging.wal_wt_cursor) {
      return WORKER_WAL_SIZE;
   } else if (gct_cursor < logging.wal_wt_cursor) {
      return gct_cursor + (WORKER_WAL_SIZE - logging.wal_wt_cursor);
   } else {
      return gct_cursor - logging.wal_wt_cursor;
   }
}
// -------------------------------------------------------------------------------------
u32 Worker::walContiguousFreeSpace()
{
   const auto gct_cursor = logging.wal_gct_cursor.load();
   return (gct_cursor > logging.wal_wt_cursor) ? gct_cursor - logging.wal_wt_cursor : WORKER_WAL_SIZE - logging.wal_wt_cursor;
}
// -------------------------------------------------------------------------------------
void Worker::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
      u32 wait_untill_free_bytes = requested_size + CR_ENTRY_SIZE;
      if ((WORKER_WAL_SIZE - logging.wal_wt_cursor) < static_cast<u32>(requested_size + CR_ENTRY_SIZE)) {
         wait_untill_free_bytes += WORKER_WAL_SIZE - logging.wal_wt_cursor;  // we have to skip this round
      }
      // Spin until we have enough space
      while (walFreeSpace() < wait_untill_free_bytes) {
      }
      if (walContiguousFreeSpace() < requested_size + CR_ENTRY_SIZE) {  // always keep place for CR entry
         WALMetaEntry& entry = *reinterpret_cast<WALMetaEntry*>(logging.wal_buffer + logging.wal_wt_cursor);
         entry.size = sizeof(WALMetaEntry);
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = WORKER_WAL_SIZE - logging.wal_wt_cursor;
         DEBUG_BLOCK() { entry.computeCRC(); }
         // -------------------------------------------------------------------------------------
         logging.wal_wt_cursor = 0;
         publishOffset();
         logging.wal_next_to_clean = 0;
         logging.wal_buffer_round++;  // Carriage Return
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      ensure(logging.wal_wt_cursor + requested_size + CR_ENTRY_SIZE <= WORKER_WAL_SIZE);
   }
}
// -------------------------------------------------------------------------------------
WALMetaEntry& Worker::reserveWALMetaEntry()
{
   walEnsureEnoughSpace(sizeof(WALMetaEntry));
   logging.active_mt_entry = reinterpret_cast<WALMetaEntry*>(logging.wal_buffer + logging.wal_wt_cursor);
   logging.active_mt_entry->lsn.store(logging.wal_lsn_counter++, std::memory_order_release);
   logging.active_mt_entry->size = sizeof(WALMetaEntry);
   return *logging.active_mt_entry;
}
// -------------------------------------------------------------------------------------
void Worker::submitWALMetaEntry()
{
   DEBUG_BLOCK() { logging.active_mt_entry->computeCRC(); }
   logging.wal_wt_cursor += sizeof(WALMetaEntry);
   publishOffset();
}
// -------------------------------------------------------------------------------------
void Worker::submitDTEntry(u64 total_size)
{
   DEBUG_BLOCK() { logging.active_dt_entry->computeCRC(); }
   COUNTERS_BLOCK() { WorkerCounters::myCounters().wal_write_bytes += total_size; }
   logging.wal_wt_cursor += total_size;
   publishMaxGSNOffset();
}
// -------------------------------------------------------------------------------------
// Also for interval garbage collection
void Worker::refreshGlobalState()
{
   if (!FLAGS_todo) {
      // Why bother
      return;
   }
   if (utils::RandomGenerator::getRandU64(0, workers_count) == 0 && global_mutex.try_lock()) {
      utils::Timer timer(CRCounters::myCounters().cc_ms_refresh_global_state);
      TXID local_newest_olap = std::numeric_limits<u64>::min();
      TXID local_oldest_oltp = std::numeric_limits<u64>::max();
      TXID local_oldest_tx = std::numeric_limits<u64>::max();

      for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
         u64 its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
         // -------------------------------------------------------------------------------------
         while ((its_in_flight_tx_id & LATCH_BIT) && ((its_in_flight_tx_id & CLEAN_BITS_MASK) < active_tx.TTS())) {
            its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
         }
         // -------------------------------------------------------------------------------------
         const bool is_rc = its_in_flight_tx_id & RC_BIT;
         const bool is_olap = its_in_flight_tx_id & OLAP_BIT;
         its_in_flight_tx_id &= CLEAN_BITS_MASK;
         if (!is_rc) {
            local_oldest_tx = std::min<TXID>(its_in_flight_tx_id, local_oldest_tx);
            if (is_olap) {
               local_newest_olap = std::max<TXID>(its_in_flight_tx_id, local_newest_olap);
            } else {
               local_oldest_oltp = std::min<TXID>(its_in_flight_tx_id, local_oldest_oltp);
            }
         }
      }
      // -------------------------------------------------------------------------------------
      global_oldest_all_start_ts.store(local_oldest_tx, std::memory_order_release);
      global_oldest_oltp_start_ts.store(local_oldest_oltp, std::memory_order_release);
      global_newest_olap_start_ts.store(local_newest_olap, std::memory_order_release);
      // -------------------------------------------------------------------------------------
      TXID global_all_lwm_buffer = std::numeric_limits<TXID>::max();
      TXID global_oltp_lwm_buffer = std::numeric_limits<TXID>::max();
      bool skipped_a_worker = false;
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
         if (all_workers[w_i]->cc.local_latest_lwm_for_tx == all_workers[w_i]->cc.local_latest_write_tx) {
            skipped_a_worker = true;
            continue;
         } else {
            all_workers[w_i]->cc.local_latest_lwm_for_tx.store(all_workers[w_i]->cc.local_latest_write_tx, std::memory_order_release);
         }
         // -------------------------------------------------------------------------------------
         TXID its_all_lwm_buffer = 0, its_oltp_lwm_buffer = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         all_workers[w_i]->cc.commit_tree->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_all_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
         // -------------------------------------------------------------------------------------
         if (FLAGS_olap_mode && global_oldest_all_start_ts != global_oldest_oltp_start_ts) {
            utils::fold(key, global_oldest_oltp_start_ts);
            all_workers[w_i]->cc.commit_tree->prefixLookupForPrev(
                key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_oltp_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
            ensure(its_all_lwm_buffer <= its_oltp_lwm_buffer);
            global_oltp_lwm_buffer = std::min<TXID>(its_oltp_lwm_buffer, global_oltp_lwm_buffer);
         } else {
            its_oltp_lwm_buffer = its_all_lwm_buffer;
         }
         // -------------------------------------------------------------------------------------
         global_all_lwm_buffer = std::min<TXID>(its_all_lwm_buffer, global_all_lwm_buffer);
         // -------------------------------------------------------------------------------------
         all_workers[w_i]->cc.local_lwm_latch.store(all_workers[w_i]->cc.local_lwm_latch.load() + 1, std::memory_order_release);  // Latch
         all_workers[w_i]->cc.all_lwm_receiver.store(its_all_lwm_buffer, std::memory_order_release);
         all_workers[w_i]->cc.oltp_lwm_receiver.store(its_oltp_lwm_buffer, std::memory_order_release);
         all_workers[w_i]->cc.local_lwm_latch.store(all_workers[w_i]->cc.local_lwm_latch.load() + 1, std::memory_order_release);  // Release
      }
      if (!skipped_a_worker) {
         global_all_lwm.store(global_all_lwm_buffer, std::memory_order_release);
         global_oltp_lwm.store(global_oltp_lwm_buffer, std::memory_order_release);
      }
      // -------------------------------------------------------------------------------------
      global_mutex.unlock();
   }
}
// -------------------------------------------------------------------------------------
void Worker::startTX(TX_MODE next_tx_type, TX_ISOLATION_LEVEL next_tx_isolation_level, bool read_only)
{
   Transaction prev_tx = active_tx;
   // For single-statement transactions, snapshot isolation and serialization are the same as read committed
   if (next_tx_type == TX_MODE::SINGLE_STATEMENT && 0) {  // TODO: check consequences on refreshGlobalState & GC
      if (next_tx_isolation_level > TX_ISOLATION_LEVEL::READ_COMMITTED) {
         next_tx_isolation_level = TX_ISOLATION_LEVEL::READ_COMMITTED;
      }
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      current_tx_wal_start = logging.wal_wt_cursor;
      if (!read_only) {
         WALMetaEntry& entry = reserveWALMetaEntry();
         entry.type = WALEntry::TYPE::TX_START;
         submitWALMetaEntry();
      }
      assert(prev_tx.state != Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      // Initialize RFA
      if (FLAGS_wal_rfa) {
         const LID sync_point = Worker::global_sync_to_this_gsn.load();
         if (sync_point > getCurrentGSN()) {
            setCurrentGSN(sync_point);
            publishMaxGSNOffset();
         }
         logging.rfa_gsn_flushed = Worker::global_gsn_flushed.load();
         logging.needs_remote_flush = false;
      } else {
         logging.needs_remote_flush = true;
      }
      // -------------------------------------------------------------------------------------
      active_tx.state = Transaction::STATE::STARTED;
      active_tx.has_wrote = false;
      active_tx.min_observed_gsn_when_started = logging.clock_gsn;
      active_tx.current_tx_mode = next_tx_type;
      active_tx.current_tx_isolation_level = next_tx_isolation_level;
      active_tx.is_read_only = read_only;
      active_tx.is_durable = FLAGS_wal;  // TODO:
      // -------------------------------------------------------------------------------------
      // Draw TXID from global counter and publish it with the TX type (i.e., OLAP or OLTP)
      // We have to acquire a transaction id and use it for locking in ANY isolation level
      if (next_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION) {
         // Also means multi statement
         global_workers_current_snapshot[worker_id].store(active_tx.tx_id | LATCH_BIT, std::memory_order_release);
         active_tx.tx_id = global_logical_clock.fetch_add(1);
         if (FLAGS_olap_mode) {
            global_workers_current_snapshot[worker_id].store(active_tx.tx_id | ((active_tx.isOLAP()) ? OLAP_BIT : 0), std::memory_order_release);
         } else {
            global_workers_current_snapshot[worker_id].store(active_tx.tx_id, std::memory_order_release);
         }
         cc.local_global_all_lwm_cache = global_all_lwm.load();
         // -------------------------------------------------------------------------------------
         if (prev_tx.isReadCommitted() || prev_tx.isReadUncommitted()) {
            switchToSnapshotIsolationMode();
         }
         // -------------------------------------------------------------------------------------
      } else {
         if (prev_tx.atLeastSI()) {
            switchToReadCommittedMode();
         }
         if (next_tx_type != TX_MODE::SINGLE_STATEMENT) {
            active_tx.tx_id = global_logical_clock.fetch_add(1);
         }
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::switchToSnapshotIsolationMode()
{
   {
      std::unique_lock guard(global_mutex);
      global_workers_current_snapshot[worker_id].store(global_logical_clock.load(), std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::switchToReadCommittedMode()
{
   {
      // Latch-free work only when all counters increase monotone, we can not simply go back
      std::unique_lock guard(global_mutex);
      const u64 last_commit_mark_flagged = global_workers_current_snapshot[worker_id].load() | RC_BIT;
      global_workers_current_snapshot[worker_id].store(last_commit_mark_flagged, std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::shutdown()
{
   garbageCollection();
   switchToReadCommittedMode();
}
// -------------------------------------------------------------------------------------
void Worker::garbageCollection()
{
   if (!FLAGS_todo) {
      return;
   }
   // -------------------------------------------------------------------------------------
   // TODO: smooth purge, we should not let the system hang on this, as a quick fix, it should be enough if we purge in small batches
   utils::Timer timer(CRCounters::myCounters().cc_ms_gc);
synclwm : {
   u64 lwm_version = cc.local_lwm_latch.load();
   while ((lwm_version = cc.local_lwm_latch.load()) & 1)
      ;
   cc.local_all_lwm = cc.all_lwm_receiver.load();
   cc.local_oltp_lwm = cc.oltp_lwm_receiver.load();
   if (lwm_version != cc.local_lwm_latch.load()) {
      goto synclwm;
   }
   ensure(!FLAGS_olap_mode || cc.local_all_lwm <= cc.local_oltp_lwm);
}
   // ATTENTION: atm, with out extra sync, the two lwm can not
   if (cc.local_all_lwm > cc.cleaned_untill_oltp_lwm) {
      // PURGE!
      history_tree.purgeVersions(
          worker_id, 0, cc.local_all_lwm - 1,
          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length, const bool called_before) {
             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id, called_before);
             COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++; }
          },
          0);
      cc.cleaned_untill_oltp_lwm = std::max(cc.local_all_lwm, cc.cleaned_untill_oltp_lwm);
      // -------------------------------------------------------------------------------------
      {
         TXID erase_till = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         cc.commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
         if (erase_till) {
            u8 start_key[sizeof(TXID)];
            utils::fold(start_key, u64(0));
            u8 end_key[sizeof(TXID)];
            utils::fold(end_key, erase_till - 1);
            cc.commit_tree->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID));
         }
      }
   }
   if (FLAGS_olap_mode && cc.local_all_lwm != cc.local_oltp_lwm) {
      //  TXID between OLAP and OLTP
      TXID erase_till = 0, erase_from = 0;
      u8 key[sizeof(TXID)];
      utils::fold(key, global_newest_olap_start_ts);
      cc.commit_tree->prefixLookup(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_from); });
      utils::fold(key, global_oldest_oltp_start_ts);
      cc.commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
      if (erase_till) {
         u8 start_key[sizeof(TXID)];
         utils::fold(start_key, erase_from + 1);
         u8 end_key[sizeof(TXID)];
         utils::fold(end_key, erase_till - 1);
         cc.commit_tree->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID), false);
      }
      // -------------------------------------------------------------------------------------
      if (cc.local_oltp_lwm > 0 && cc.local_oltp_lwm > cc.cleaned_untill_oltp_lwm) {
         // MOVE deletes to the graveyard
         const u64 from_tx_id = cc.cleaned_untill_oltp_lwm > 0 ? cc.cleaned_untill_oltp_lwm : 0;
         history_tree.visitRemoveVersions(worker_id, from_tx_id, cc.local_oltp_lwm - 1,
                                          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload,
                                              [[maybe_unused]] u64 version_payload_length, const bool called_before) {
                                             cc.cleaned_untill_oltp_lwm = std::max(cc.cleaned_untill_oltp_lwm, tx_id + 1);
                                             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id,
                                                                                                     called_before);
                                             COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_oltp_executed[dt_id]++; }
                                          });
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (activeTX().isDurable()) {
      command_id = 0;  // Reset command_id only on commit and never on abort
      // -------------------------------------------------------------------------------------
      if (activeTX().isSingleStatement() && active_tx.state != Transaction::STATE::STARTED) {
         return;  // Skip double commit in case of single statement upsert [hack]
      }
      assert(active_tx.state == Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      if (!activeTX().isReadOnly()) {
         WALMetaEntry& entry = reserveWALMetaEntry();
         entry.type = WALEntry::TYPE::TX_COMMIT;
         submitWALMetaEntry();
      }
      // -------------------------------------------------------------------------------------
      active_tx.max_observed_gsn = logging.clock_gsn;
      active_tx.state = Transaction::STATE::READY_TO_COMMIT;
      // We don't have pmem, so we can only avoid remote flushes for real if the tx is a single lookup
      if (!FLAGS_wal_rfa_pmem_simulate) {
         logging.needs_remote_flush |= !activeTX().isReadOnly();
      }
      if (logging.needs_remote_flush) {  // RFA
         std::unique_lock<std::mutex> g(logging.precommitted_queue_mutex);
         logging.precommitted_queue.push_back(active_tx);
         logging.ready_to_commit_queue_size += 1;
      } else {
         active_tx.state = Transaction::STATE::COMMITTED;
         CRCounters::myCounters().rfa_committed_tx += 1;
      }
      // -------------------------------------------------------------------------------------
      if (activeTX().hasWrote()) {
         TXID commit_ts;
         cc.commit_tree->append(
             [&](u8* key) {
                commit_ts = global_logical_clock.fetch_add(1);
                utils::fold(key, commit_ts);
             },
             sizeof(TXID), [&](u8* value) { *reinterpret_cast<TXID*>(value) = active_tx.TTS(); }, sizeof(TXID), cc.commit_tree_handler);
         cc.local_latest_write_tx.store(commit_ts, std::memory_order_release);
      }
      // Only committing snapshot/ changing between SI and lower modes
      if (activeTX().atLeastSI()) {
         refreshGlobalState();
      }
      // All isolation level generate garbage
      garbageCollection();
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   if (FLAGS_wal) {
      ensure(active_tx.state == Transaction::STATE::STARTED);
      const u64 tx_id = active_tx.TTS();
      std::vector<const WALEntry*> entries;
      iterateOverCurrentTXEntries([&](const WALEntry& entry) {
         if (entry.type == WALEntry::TYPE::DT_SPECIFIC) {
            entries.push_back(&entry);
         }
      });
      std::for_each(entries.rbegin(), entries.rend(), [&](const WALEntry* entry) {
         const auto& dt_entry = *reinterpret_cast<const WALDTEntry*>(entry);
         leanstore::storage::DTRegistry::global_dt_registry.undo(dt_entry.dt_id, dt_entry.payload, tx_id);
      });
      // -------------------------------------------------------------------------------------
      history_tree.purgeVersions(worker_id, active_tx.TTS(), active_tx.TTS(), [&](const TXID, const DTID, const u8*, u64, const bool) {});
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = reserveWALMetaEntry();
      entry.type = WALEntry::TYPE::TX_ABORT;
      submitWALMetaEntry();
      active_tx.state = Transaction::STATE::ABORTED;
   }
   jumpmu::jump();
}
// -------------------------------------------------------------------------------------
Worker::VISIBILITY Worker::isVisibleForIt(WORKERID whom_worker_id, TXID commit_ts)
{
   return cc.local_workers_start_ts[whom_worker_id] > commit_ts ? VISIBILITY::VISIBLE_ALREADY : VISIBILITY::VISIBLE_NEXT_ROUND;
}
// -------------------------------------------------------------------------------------
// UNDETERMINED is not possible atm because we spin on start_ts
Worker::VISIBILITY Worker::isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, TXID tx_ts)
{
   const bool is_commit_ts = tx_ts & MSB;
   const TXID commit_ts = is_commit_ts ? (tx_ts & MSB_MASK) : getCommitTimestamp(what_worker_id, tx_ts);
   return isVisibleForIt(whom_worker_id, commit_ts);
}
// -------------------------------------------------------------------------------------
TXID Worker::getCommitTimestamp(WORKERID worker_id, TXID tx_ts)
{
   if (tx_ts & MSB) {
      return tx_ts & MSB_MASK;
   }
   assert((tx_ts & MSB) || isVisibleForMe(worker_id, tx_ts));
   // -------------------------------------------------------------------------------------
   const TXID& start_ts = tx_ts;
   TXID commit_ts = std::numeric_limits<TXID>::max();  // TODO: align with GC
   u8 key[sizeof(TXID)];
   utils::fold(key, start_ts);
   all_workers[worker_id]->cc.commit_tree->prefixLookup(key, sizeof(TXID),
                                                        [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, commit_ts); });
   ensure(commit_ts > start_ts);
   return commit_ts;
}
// -------------------------------------------------------------------------------------
// It is also used to check whether the tuple is write-locked, hence we need the to_write intention flag
bool Worker::isVisibleForMe(WORKERID other_worker_id, u64 tx_ts, bool to_write)
{
   const bool is_commit_ts = tx_ts & MSB;
   const TXID committed_ts = (tx_ts & MSB) ? (tx_ts & MSB_MASK) : 0;
   const TXID start_ts = tx_ts & MSB_MASK;
   if (is_commit_ts && activeTX().isSingleStatement()) {
      return true;
   }
   if (!to_write && activeTX().isReadUncommitted()) {
      return true;
   }
   if (worker_id == other_worker_id) {
      return true;
   } else {
      if (activeTX().isReadCommitted() || activeTX().isReadUncommitted()) {
         if (is_commit_ts) {
            return true;
         }
         u8 key[sizeof(TXID)];
         utils::fold(key, std::numeric_limits<TXID>::max());
         TXID committed_till = 0;
         all_workers[other_worker_id]->cc.commit_tree->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { committed_till = *reinterpret_cast<const TXID*>(s_value); });
         return committed_till >= tx_ts;
      } else if (activeTX().atLeastSI()) {
         if (is_commit_ts) {
            return active_tx.TTS() > committed_ts;
         }
         if (start_ts < cc.local_global_all_lwm_cache) {
            return true;
         }
         // -------------------------------------------------------------------------------------
         if (cc.local_snapshot_cache_ts[other_worker_id] == active_tx.TTS()) {  // Use the cache
            return cc.local_snapshot_cache[other_worker_id] >= start_ts;
         } else if (cc.local_snapshot_cache[other_worker_id] >= start_ts) {
            return true;
         }
         TXID largest_visible_tx_id = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, active_tx.TTS());
         OP_RESULT ret = all_workers[other_worker_id]->cc.commit_tree->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { largest_visible_tx_id = *reinterpret_cast<const TXID*>(s_value); });
         if (ret == OP_RESULT::OK) {
            cc.local_snapshot_cache[other_worker_id] = largest_visible_tx_id;
            cc.local_snapshot_cache_ts[other_worker_id] = active_tx.TTS();
            return largest_visible_tx_id >= start_ts;
         }
         return false;
      } else {
         UNREACHABLE();
      }
   }
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForAll(WORKERID, TXID ts)
{
   if (ts & MSB) {
      // Commit Timestamp
      return (ts & MSB_MASK) < global_oldest_all_start_ts.load();
   } else {
      // Start Timestamp
      return ts < global_all_lwm.load();
   }
}
// -------------------------------------------------------------------------------------
// Called by worker, so concurrent writes on the buffer
void Worker::iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback)
{
   u64 cursor = current_tx_wal_start;
   while (cursor != logging.wal_wt_cursor) {
      const WALEntry& entry = *reinterpret_cast<WALEntry*>(logging.wal_buffer + cursor);
      DEBUG_BLOCK()
      {
         if (entry.type != WALEntry::TYPE::CARRIAGE_RETURN)
            entry.checkCRC();
      }
      if (entry.type == WALEntry::TYPE::CARRIAGE_RETURN) {
         cursor = 0;
      } else {
         callback(entry);
         cursor += entry.size;
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
