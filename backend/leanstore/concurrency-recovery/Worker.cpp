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
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, VersionsSpaceInterface& versions_space, s32 fd, const bool is_page_provider)
    : worker_id(worker_id),
      all_workers(all_workers),
      workers_count(workers_count),
      versions_space(versions_space),
      ssd_fd(fd),
      is_page_provider(is_page_provider)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   std::memset(wal_buffer, 0, WORKER_WAL_SIZE);
   if (!is_page_provider) {
      local_snapshot_cache = make_unique<u64[]>(workers_count);
      local_snapshot_cache_ts = make_unique<u64[]>(workers_count);
      local_workers_start_ts = make_unique<u64[]>(workers_count + 1);
      global_workers_current_snapshot[worker_id] = 0;
   }
}
Worker::~Worker() = default;
// -------------------------------------------------------------------------------------
u32 Worker::walFreeSpace()
{
   // A , B , C : a - b + c % c
   const auto gct_cursor = wal_gct_cursor.load();
   if (gct_cursor == wal_wt_cursor) {
      return WORKER_WAL_SIZE;
   } else if (gct_cursor < wal_wt_cursor) {
      return gct_cursor + (WORKER_WAL_SIZE - wal_wt_cursor);
   } else {
      return gct_cursor - wal_wt_cursor;
   }
}
// -------------------------------------------------------------------------------------
u32 Worker::walContiguousFreeSpace()
{
   const auto gct_cursor = wal_gct_cursor.load();
   return (gct_cursor > wal_wt_cursor) ? gct_cursor - wal_wt_cursor : WORKER_WAL_SIZE - wal_wt_cursor;
}
// -------------------------------------------------------------------------------------
void Worker::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
      u32 wait_untill_free_bytes = requested_size + CR_ENTRY_SIZE;
      if ((WORKER_WAL_SIZE - wal_wt_cursor) < static_cast<u32>(requested_size + CR_ENTRY_SIZE)) {
         wait_untill_free_bytes += WORKER_WAL_SIZE - wal_wt_cursor;  // we have to skip this round
      }
      // Spin until we have enough space
      while (walFreeSpace() < wait_untill_free_bytes) {
      }
      if (walContiguousFreeSpace() < requested_size + CR_ENTRY_SIZE) {  // always keep place for CR entry
         WALMetaEntry& entry = *reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_wt_cursor);
         invalidateEntriesUntil(WORKER_WAL_SIZE);
         entry.size = sizeof(WALMetaEntry);
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = WORKER_WAL_SIZE - wal_wt_cursor;
         DEBUG_BLOCK() { entry.computeCRC(); }
         // -------------------------------------------------------------------------------------
         wal_wt_cursor = 0;
         publishOffset();
         wal_next_to_clean = 0;
         wal_buffer_round++;  // Carriage Return
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      ensure(wal_wt_cursor + requested_size + CR_ENTRY_SIZE <= WORKER_WAL_SIZE);
   }
}
// -------------------------------------------------------------------------------------
// Not need if workers will never read from other workers WAL
void Worker::invalidateEntriesUntil(u64 until)
{
   if (FLAGS_vw && wal_buffer_round > 0) {  // ATM, needed only for VW
      constexpr u64 INVALIDATE_LSN = std::numeric_limits<u64>::max();
      assert(wal_next_to_clean >= wal_wt_cursor);
      assert(wal_next_to_clean <= WORKER_WAL_SIZE);
      if (wal_next_to_clean < until) {
         u64 offset = wal_next_to_clean;
         while (offset < until) {
            auto entry = reinterpret_cast<WALEntry*>(wal_buffer + offset);
            DEBUG_BLOCK()
            {
               assert(offset + entry->size <= WORKER_WAL_SIZE);
               if (entry->type != WALEntry::TYPE::CARRIAGE_RETURN) {
                  entry->checkCRC();
               }
               assert(entry->lsn < INVALIDATE_LSN);
            }
            entry->lsn.store(INVALIDATE_LSN, std::memory_order_release);
            offset += entry->size;
         }
         wal_next_to_clean = offset;
      }
   }
}
// -------------------------------------------------------------------------------------
WALMetaEntry& Worker::reserveWALMetaEntry()
{
   walEnsureEnoughSpace(sizeof(WALMetaEntry));
   active_mt_entry = reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_wt_cursor);
   invalidateEntriesUntil(wal_wt_cursor + sizeof(WALMetaEntry));
   active_mt_entry->lsn.store(wal_lsn_counter++, std::memory_order_release);
   active_mt_entry->size = sizeof(WALMetaEntry);
   return *active_mt_entry;
}
// -------------------------------------------------------------------------------------
void Worker::submitWALMetaEntry()
{
   DEBUG_BLOCK() { active_mt_entry->computeCRC(); }
   wal_wt_cursor += sizeof(WALMetaEntry);
   publishOffset();
}
// -------------------------------------------------------------------------------------
void Worker::submitDTEntry(u64 total_size)
{
   DEBUG_BLOCK() { active_dt_entry->computeCRC(); }
   COUNTERS_BLOCK() { WorkerCounters::myCounters().wal_write_bytes += total_size; }
   wal_wt_cursor += total_size;
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
   if (FLAGS_imitate_wt) {
      local_all_lwm = std::numeric_limits<u64>::max();
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
         u64 its_in_flight_tx_id = all_workers[w_i]->local_min_in_progress.load();
         local_all_lwm = std::min(local_all_lwm, its_in_flight_tx_id & CLEAN_BITS_MASK);
      }
      if (local_all_lwm)
         local_all_lwm--;
      return;
   }
   // -------------------------------------------------------------------------------------
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
         if (all_workers[w_i]->local_latest_lwm_for_tx == all_workers[w_i]->local_latest_write_tx) {
            skipped_a_worker = true;
            continue;
         } else {
            all_workers[w_i]->local_latest_lwm_for_tx.store(all_workers[w_i]->local_latest_write_tx, std::memory_order_release);
         }
         // -------------------------------------------------------------------------------------
         TXID its_all_lwm_buffer = 0, its_oltp_lwm_buffer = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         all_workers[w_i]->commit_to_start_map->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_all_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
         // -------------------------------------------------------------------------------------
         if (FLAGS_olap_mode && global_oldest_all_start_ts != global_oldest_oltp_start_ts) {
            utils::fold(key, global_oldest_oltp_start_ts);
            all_workers[w_i]->commit_to_start_map->prefixLookupForPrev(
                key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_oltp_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
            ensure(its_all_lwm_buffer <= its_oltp_lwm_buffer);
            global_oltp_lwm_buffer = std::min<TXID>(its_oltp_lwm_buffer, global_oltp_lwm_buffer);
         } else {
            its_oltp_lwm_buffer = its_all_lwm_buffer;
         }
         // -------------------------------------------------------------------------------------
         global_all_lwm_buffer = std::min<TXID>(its_all_lwm_buffer, global_all_lwm_buffer);
         // -------------------------------------------------------------------------------------
         all_workers[w_i]->local_lwm_latch.store(all_workers[w_i]->local_lwm_latch.load() + 1, std::memory_order_release);  // Latch
         all_workers[w_i]->all_lwm_receiver.store(its_all_lwm_buffer, std::memory_order_release);
         all_workers[w_i]->oltp_lwm_receiver.store(its_oltp_lwm_buffer, std::memory_order_release);
         all_workers[w_i]->local_lwm_latch.store(all_workers[w_i]->local_lwm_latch.load() + 1, std::memory_order_release);  // Release
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
   if (next_tx_type == TX_MODE::SINGLE_STATEMENT) {
      if (next_tx_isolation_level > TX_ISOLATION_LEVEL::READ_COMMITTED) {
         next_tx_isolation_level = TX_ISOLATION_LEVEL::READ_COMMITTED;
      }
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      current_tx_wal_start = wal_wt_cursor;
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
         rfa_gsn_flushed = Worker::global_gsn_flushed.load();
         needs_remote_flush = false;
      } else {
         needs_remote_flush = true;
      }
      // -------------------------------------------------------------------------------------
      active_tx.state = Transaction::STATE::STARTED;
      active_tx.has_wrote = false;
      active_tx.min_observed_gsn_when_started = clock_gsn;
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
         global_workers_current_snapshot[worker_id].store(active_tx.tx_id | ((active_tx.isOLAP()) ? OLAP_BIT : 0), std::memory_order_release);
         local_global_all_lwm_cache = global_all_lwm.load();
         // -------------------------------------------------------------------------------------
         if (FLAGS_imitate_wt) {
            TXID tmp = std::numeric_limits<TXID>::max();
            for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
               u64 its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
               // -------------------------------------------------------------------------------------
               while (its_in_flight_tx_id & LATCH_BIT) {
                  its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
               }
               local_workers_start_ts[w_i] = its_in_flight_tx_id;
               tmp = std::min<TXID>(tmp, its_in_flight_tx_id);
            }
            local_min_in_progress.store(tmp, std::memory_order_release);
            return;
         }
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
   if (FLAGS_imitate_wt && local_all_lwm) {
      versions_space.purgeVersions(
          worker_id, 0, local_all_lwm - 1,
          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length, const bool called_before) {
             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id, called_before);
             COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++; }
          },
          0);
      return;
   }
   // -------------------------------------------------------------------------------------
   // TODO: smooth purge, we should not let the system hang on this, as a quick fix, it should be enough if we purge in small batches
   utils::Timer timer(CRCounters::myCounters().cc_ms_gc);
synclwm : {
   u64 lwm_version = local_lwm_latch.load();
   while ((lwm_version = local_lwm_latch.load()) & 1)
      ;
   local_all_lwm = all_lwm_receiver.load();
   local_oltp_lwm = oltp_lwm_receiver.load();
   if (lwm_version != local_lwm_latch.load()) {
      goto synclwm;
   }
   ensure(!FLAGS_olap_mode || local_all_lwm <= local_oltp_lwm);
}
   // ATTENTION: atm, with out extra sync, the two lwm can not
   if (local_all_lwm > cleaned_untill_oltp_lwm) {
      // PURGE!
      versions_space.purgeVersions(
          worker_id, 0, local_all_lwm - 1,
          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length, const bool called_before) {
             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id, called_before);
             COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++; }
          },
          0);
      cleaned_untill_oltp_lwm = std::max(local_all_lwm, cleaned_untill_oltp_lwm);
      // -------------------------------------------------------------------------------------
      {
         TXID erase_till = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         commit_to_start_map->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
         if (erase_till) {
            u8 start_key[sizeof(TXID)];
            utils::fold(start_key, u64(0));
            u8 end_key[sizeof(TXID)];
            utils::fold(end_key, erase_till - 1);
            commit_to_start_map->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID));
         }
      }
   }
   if (FLAGS_olap_mode && local_all_lwm != local_oltp_lwm) {
      //  TXID between OLAP and OLTP
      TXID erase_till = 0, erase_from = 0;
      u8 key[sizeof(TXID)];
      utils::fold(key, global_newest_olap_start_ts);
      commit_to_start_map->prefixLookup(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_from); });
      utils::fold(key, global_oldest_oltp_start_ts);
      commit_to_start_map->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
      if (erase_till) {
         u8 start_key[sizeof(TXID)];
         utils::fold(start_key, erase_from + 1);
         u8 end_key[sizeof(TXID)];
         utils::fold(end_key, erase_till - 1);
         commit_to_start_map->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID), false);
      }
      // -------------------------------------------------------------------------------------
      if (local_oltp_lwm > 0 && local_oltp_lwm > cleaned_untill_oltp_lwm) {
         // MOVE deletes to the graveyard
         const u64 from_tx_id = cleaned_untill_oltp_lwm > 0 ? cleaned_untill_oltp_lwm : 0;
         versions_space.visitRemoveVersions(worker_id, from_tx_id, local_oltp_lwm - 1,
                                            [&](const TXID tx_id, const DTID dt_id, const u8* version_payload,
                                                [[maybe_unused]] u64 version_payload_length, const bool called_before) {
                                               cleaned_untill_oltp_lwm = std::max(cleaned_untill_oltp_lwm, tx_id + 1);
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
      active_tx.max_observed_gsn = clock_gsn;
      active_tx.state = Transaction::STATE::READY_TO_COMMIT;
      // We don't have pmem, so we can only avoid remote flushes for real if the tx is a single lookup
      if (!FLAGS_wal_rfa_pmem_simulate) {
         needs_remote_flush |= !activeTX().isReadOnly();
      }
      if (needs_remote_flush) {  // RFA
         std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
         ready_to_commit_queue.push_back(active_tx);
         ready_to_commit_queue_size += 1;
      } else {
         active_tx.state = Transaction::STATE::COMMITTED;
         CRCounters::myCounters().rfa_committed_tx += 1;
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_imitate_wt) {
         global_workers_current_snapshot[worker_id].store(global_logical_clock.fetch_add(1), std::memory_order_release);
      } else {
         if (activeTX().hasWrote()) {
            TXID commit_ts;
            commit_to_start_map->append(
                [&](u8* key) {
                   commit_ts = global_logical_clock.fetch_add(1);
                   utils::fold(key, commit_ts);
                },
                sizeof(TXID), [&](u8* value) { *reinterpret_cast<TXID*>(value) = active_tx.TTS(); }, sizeof(TXID), map_leaf_handler);
            local_latest_write_tx.store(commit_ts, std::memory_order_release);
         }
         if (activeTX().isSerializable()) {
            executeUnlockTasks();
         }
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
      if (activeTX().isSerializable()) {
         executeUnlockTasks();
      }
      versions_space.purgeVersions(worker_id, active_tx.TTS(), active_tx.TTS(), [&](const TXID, const DTID, const u8*, u64, const bool) {});
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
   return local_workers_start_ts[whom_worker_id] > commit_ts ? VISIBILITY::VISIBLE_ALREADY : VISIBILITY::VISIBLE_NEXT_ROUND;
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
   all_workers[worker_id]->commit_to_start_map->prefixLookup(key, sizeof(TXID),
                                                             [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, commit_ts); });
   ensure(commit_ts > start_ts);
   return commit_ts;
}
// -------------------------------------------------------------------------------------
// It is also used to check whether the tuple is write-locked, hence we need the to_write intention flag
bool Worker::isVisibleForMe(WORKERID other_worker_id, u64 tx_ts, bool to_write)
{
   if (FLAGS_imitate_wt) {
     if (tx_ts == active_tx.TTS() || tx_ts < local_min_in_progress)
         return true;
      if (tx_ts > active_tx.TTS()) {
         return false;
      }
      const bool is_not_in_progress = (std::find(local_workers_start_ts.get(), local_workers_start_ts.get() + workers_count, tx_ts) ==
                                       local_workers_start_ts.get() + workers_count);
      return is_not_in_progress;
   }
   // -------------------------------------------------------------------------------------
   const bool is_commit_ts = tx_ts & MSB;
   const TXID committed_ts = (tx_ts & MSB) ? (tx_ts & MSB_MASK) : 0;
   const TXID start_ts = tx_ts & MSB_MASK;
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
         all_workers[other_worker_id]->commit_to_start_map->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { committed_till = *reinterpret_cast<const TXID*>(s_value); });
         return committed_till >= tx_ts;
      } else if (activeTX().atLeastSI()) {
         if (is_commit_ts) {
            return active_tx.TTS() > committed_ts;
         }
         if (start_ts < local_global_all_lwm_cache) {
            return true;
         }
         // -------------------------------------------------------------------------------------
         if (local_snapshot_cache_ts[other_worker_id] == active_tx.TTS()) {  // Use the cache
            return local_snapshot_cache[other_worker_id] >= start_ts;
         } else if (local_snapshot_cache[other_worker_id] >= start_ts) {
            return true;
         }
         TXID largest_visible_tx_id = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, active_tx.TTS());
         OP_RESULT ret = all_workers[other_worker_id]->commit_to_start_map->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { largest_visible_tx_id = *reinterpret_cast<const TXID*>(s_value); });
         if (ret == OP_RESULT::OK) {
            local_snapshot_cache[other_worker_id] = largest_visible_tx_id;
            local_snapshot_cache_ts[other_worker_id] = active_tx.TTS();
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
   while (cursor != wal_wt_cursor) {
      const WALEntry& entry = *reinterpret_cast<WALEntry*>(wal_buffer + cursor);
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
WALChunk::Slot Worker::WALFinder::getJumpPoint(LID lsn)
{
   std::unique_lock guard(m);
   // -------------------------------------------------------------------------------------
   if (ht.size() == 0) {
      return {0, 0};
   } else {
      auto iter = ht.lower_bound(lsn);
      if (iter != ht.end() && iter->first == lsn) {
         return iter->second;
      } else {
         iter = std::prev(iter);
         return iter->second;
      }
   }
}
// -------------------------------------------------------------------------------------
// TODO:
void Worker::WALFinder::insertJumpPoint(LID LSN, WALChunk::Slot slot)
{
   std::unique_lock guard(m);
   ht[LSN] = slot;
}
// -------------------------------------------------------------------------------------
Worker::WALFinder::~WALFinder() {}
// -------------------------------------------------------------------------------------
void Worker::getWALDTEntryPayload(WORKERID worker_id, LID lsn, u32 in_memory_offset, std::function<void(u8*)> callback)
{
   all_workers[worker_id]->getWALEntry(lsn, in_memory_offset, [&](WALEntry* entry) { callback(reinterpret_cast<WALDTEntry*>(entry)->payload); });
}
// -------------------------------------------------------------------------------------
void Worker::getWALEntry(WORKERID worker_id, LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback)
{
   all_workers[worker_id]->getWALEntry(lsn, in_memory_offset, callback);
}
// -------------------------------------------------------------------------------------
void Worker::getWALEntry(LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback)
{
   {
      // 1- Optimistically locate the entry
      auto dt_entry = reinterpret_cast<WALEntry*>(wal_buffer + in_memory_offset);
      const u16 dt_size = dt_entry->size;
      if (dt_entry->lsn != lsn) {
         goto outofmemory;
      }
      u8 log[dt_size];
      std::memcpy(log, wal_buffer + in_memory_offset, dt_size);
      if (dt_entry->lsn != lsn) {
         goto outofmemory;
      }
      auto entry = reinterpret_cast<WALEntry*>(log);
      assert(entry->lsn == lsn);
      DEBUG_BLOCK() { entry->checkCRC(); }
      callback(entry);
      COUNTERS_BLOCK() { WorkerCounters::myCounters().wal_buffer_hit++; }
      return;
   }
outofmemory : {
   COUNTERS_BLOCK() { WorkerCounters::myCounters().wal_buffer_miss++; }
   // 2- Read from SSD, accelerate using getLowerBound
   const auto slot = wal_finder.getJumpPoint(lsn);
   if (slot.offset == 0) {
      goto outofmemory;
   }
   const u64 lower_bound = slot.offset;
   const u64 lower_bound_aligned = utils::downAlign(lower_bound);
   const u64 read_size_aligned = utils::upAlign(slot.length + lower_bound - lower_bound_aligned);
   auto log_chunk = static_cast<u8*>(std::aligned_alloc(512, read_size_aligned));
   const u64 ret = pread(ssd_fd, log_chunk, read_size_aligned, lower_bound_aligned);
   posix_check(ret >= read_size_aligned);
   WorkerCounters::myCounters().wal_read_bytes += read_size_aligned;
   // -------------------------------------------------------------------------------------
   u64 offset = 0;
   u8* ptr = log_chunk + lower_bound - lower_bound_aligned;
   auto entry = reinterpret_cast<WALEntry*>(ptr + offset);
   while (true) {
      DEBUG_BLOCK() { entry->checkCRC(); }
      assert(entry->size > 0 && entry->lsn <= lsn);
      if (entry->lsn == lsn) {
         callback(entry);
         std::free(log_chunk);
         return;
      }
      if ((offset + entry->size) < slot.length) {
         offset += entry->size;
         entry = reinterpret_cast<WALEntry*>(ptr + offset);
      } else {
         break;
      }
   }
   std::free(log_chunk);
   goto outofmemory;
   ensure(false);
   return;
}
}
// -------------------------------------------------------------------------------------
void Worker::addUnlockTask(DTID dt_id, u64 payload_length, std::function<void(u8*)> callback)
{
   unlock_tasks_after_commit.push_back(std::make_unique<u8[]>(payload_length + sizeof(UnlockTask)));
   callback((new (unlock_tasks_after_commit.back().get()) UnlockTask(dt_id, payload_length))->payload);
}
// -------------------------------------------------------------------------------------
void Worker::executeUnlockTasks()
{
   for (auto& unlock_ptr : unlock_tasks_after_commit) {
      auto& unlock_task = *reinterpret_cast<UnlockTask*>(unlock_ptr.get());
      leanstore::storage::DTRegistry::global_dt_registry.unlock(unlock_task.dt_id, unlock_task.payload);
   }
   unlock_tasks_after_commit.clear();
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
