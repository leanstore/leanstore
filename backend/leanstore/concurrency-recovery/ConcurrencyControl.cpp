#include "Worker.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Worker::ConcurrencyControl::global_clock = WORKERS_INCREMENT;
// -------------------------------------------------------------------------------------
// Also for interval garbage collection
void Worker::ConcurrencyControl::refreshGlobalState()
{
   if (FLAGS_si_commit_protocol == 2) {
      local_all_lwm = std::numeric_limits<TXID>::max();
      for (WORKERID w_i = 0; w_i < my().workers_count; w_i++) {
         TXID its_in_flight_tx_id = other(w_i).wt_pg.snapshot_min_tx_id.load();
         local_all_lwm = std::min(local_all_lwm, its_in_flight_tx_id);
      }
      if (local_all_lwm)
         local_all_lwm--;
      return;
   }
   // -------------------------------------------------------------------------------------
   if (!FLAGS_todo) {
      // Why bother
      return;
   }
   if (utils::RandomGenerator::getRandU64(0, my().workers_count) == 0 && global_mutex.try_lock()) {
      utils::Timer timer(CRCounters::myCounters().cc_ms_refresh_global_state);
      TXID local_newest_olap = std::numeric_limits<u64>::min();
      TXID local_oldest_oltp = std::numeric_limits<u64>::max();
      TXID local_oldest_tx = std::numeric_limits<u64>::max();

      for (WORKERID w_i = 0; w_i < my().workers_count; w_i++) {
         u64 its_in_flight_tx_id = global_workers_current_snapshot[w_i].load();
         // -------------------------------------------------------------------------------------
         while ((its_in_flight_tx_id & LATCH_BIT) && ((its_in_flight_tx_id & CLEAN_BITS_MASK) < activeTX().startTS())) {
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
      for (WORKERID w_i = 0; w_i < my().workers_count; w_i++) {
         if (other(w_i).local_latest_lwm_for_tx == other(w_i).local_latest_write_tx) {
            skipped_a_worker = true;
            continue;
         } else {
            other(w_i).local_latest_lwm_for_tx.store(other(w_i).local_latest_write_tx, std::memory_order_release);
         }
         // -------------------------------------------------------------------------------------
         TXID its_all_lwm_buffer = 0, its_oltp_lwm_buffer = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         other(w_i).commit_tree->prefixLookupForPrev(
             key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_all_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
         // -------------------------------------------------------------------------------------
         if (FLAGS_olap_mode && global_oldest_all_start_ts != global_oldest_oltp_start_ts) {
            utils::fold(key, global_oldest_oltp_start_ts);
            other(w_i).commit_tree->prefixLookupForPrev(
                key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) { its_oltp_lwm_buffer = *reinterpret_cast<const TXID*>(s_value); });
            ensure(its_all_lwm_buffer <= its_oltp_lwm_buffer);
            global_oltp_lwm_buffer = std::min<TXID>(its_oltp_lwm_buffer, global_oltp_lwm_buffer);
         } else {
            its_oltp_lwm_buffer = its_all_lwm_buffer;
         }
         // -------------------------------------------------------------------------------------
         global_all_lwm_buffer = std::min<TXID>(its_all_lwm_buffer, global_all_lwm_buffer);
         // -------------------------------------------------------------------------------------
         other(w_i).local_lwm_latch.store(other(w_i).local_lwm_latch.load() + 1, std::memory_order_release);  // Latch
         other(w_i).all_lwm_receiver.store(its_all_lwm_buffer, std::memory_order_release);
         other(w_i).oltp_lwm_receiver.store(its_oltp_lwm_buffer, std::memory_order_release);
         other(w_i).local_lwm_latch.store(other(w_i).local_lwm_latch.load() + 1, std::memory_order_release);  // Release
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
void Worker::ConcurrencyControl::switchToSnapshotIsolationMode()
{
   {
      std::unique_lock guard(global_mutex);
      global_workers_current_snapshot[my().worker_id].store(global_clock.load(), std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::switchToReadCommittedMode()
{
   {
      // Latch-free work only when all counters increase monotone, we can not simply go back
      std::unique_lock guard(global_mutex);
      const u64 last_commit_mark_flagged = global_workers_current_snapshot[my().worker_id].load() | RC_BIT;
      global_workers_current_snapshot[my().worker_id].store(last_commit_mark_flagged, std::memory_order_release);
   }
   refreshGlobalState();
}
// -------------------------------------------------------------------------------------
void Worker::ConcurrencyControl::garbageCollection()
{
   if (!FLAGS_todo) {
      return;
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_si_commit_protocol == 2) {
      if (local_all_lwm)
         history_tree.purgeVersions(
             my().worker_id, 0, local_all_lwm - 1,
             [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length,
                 const bool called_before) {
                leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, my().worker_id, tx_id, called_before);
                COUNTERS_BLOCK()
                {
                   WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++;
                }
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
      history_tree.purgeVersions(
          my().worker_id, 0, local_all_lwm - 1,
          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length, const bool called_before) {
             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, my().worker_id, tx_id, called_before);
             COUNTERS_BLOCK()
             {
                WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++;
             }
          },
          0);
      cleaned_untill_oltp_lwm = std::max(local_all_lwm, cleaned_untill_oltp_lwm);
      // -------------------------------------------------------------------------------------
      {
         TXID erase_till = 0;
         u8 key[sizeof(TXID)];
         utils::fold(key, global_oldest_all_start_ts);
         commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
         if (erase_till) {
            u8 start_key[sizeof(TXID)];
            utils::fold(start_key, u64(0));
            u8 end_key[sizeof(TXID)];
            utils::fold(end_key, erase_till - 1);
            commit_tree->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID));
         }
      }
   }
   if (FLAGS_olap_mode && local_all_lwm != local_oltp_lwm) {
      //  TXID between OLAP and OLTP
      TXID erase_till = 0, erase_from = 0;
      u8 key[sizeof(TXID)];
      utils::fold(key, global_newest_olap_start_ts);
      commit_tree->prefixLookup(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_from); });
      utils::fold(key, global_oldest_oltp_start_ts);
      commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, erase_till); });
      if (erase_till) {
         u8 start_key[sizeof(TXID)];
         utils::fold(start_key, erase_from + 1);
         u8 end_key[sizeof(TXID)];
         utils::fold(end_key, erase_till - 1);
         commit_tree->rangeRemove(start_key, sizeof(TXID), end_key, sizeof(TXID), false);
      }
      // -------------------------------------------------------------------------------------
      if (local_oltp_lwm > 0 && local_oltp_lwm > cleaned_untill_oltp_lwm) {
         // MOVE deletes to the graveyard
         const u64 from_tx_id = cleaned_untill_oltp_lwm > 0 ? cleaned_untill_oltp_lwm : 0;
         history_tree.visitRemoveVersions(my().worker_id, from_tx_id, local_oltp_lwm - 1,
                                          [&](const TXID tx_id, const DTID dt_id, const u8* version_payload,
                                              [[maybe_unused]] u64 version_payload_length, const bool called_before) {
                                             cleaned_untill_oltp_lwm = std::max(cleaned_untill_oltp_lwm, tx_id + 1);
                                             leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, my().worker_id, tx_id,
                                                                                                     called_before);
                                             COUNTERS_BLOCK()
                                             {
                                                WorkerCounters::myCounters().cc_todo_oltp_executed[dt_id]++;
                                             }
                                          });
      }
   }
}
Worker::ConcurrencyControl::VISIBILITY Worker::ConcurrencyControl::isVisibleForIt(WORKERID whom_worker_id, TXID commit_ts)
{
   ensure(FLAGS_si_commit_protocol == 0);
   return local_workers_start_ts[whom_worker_id] > commit_ts ? VISIBILITY::VISIBLE_ALREADY : VISIBILITY::VISIBLE_NEXT_ROUND;
}
// -------------------------------------------------------------------------------------
// UNDETERMINED is not possible atm because we spin on start_ts
Worker::ConcurrencyControl::VISIBILITY Worker::ConcurrencyControl::isVisibleForIt(WORKERID whom_worker_id, WORKERID what_worker_id, TXID tx_ts)
{
   ensure(FLAGS_si_commit_protocol == 0);
   const bool is_commit_ts = tx_ts & MSB;
   const TXID commit_ts = is_commit_ts ? (tx_ts & MSB_MASK) : getCommitTimestamp(what_worker_id, tx_ts);
   return isVisibleForIt(whom_worker_id, commit_ts);
}
// -------------------------------------------------------------------------------------
TXID Worker::ConcurrencyControl::getCommitTimestamp(WORKERID worker_id, TXID tx_ts)
{
   ensure(FLAGS_si_commit_protocol == 0);
   if (tx_ts & MSB) {
      return tx_ts & MSB_MASK;
   }
   assert((tx_ts & MSB) || isVisibleForMe(worker_id, tx_ts));
   // -------------------------------------------------------------------------------------
   const TXID& start_ts = tx_ts;
   TXID commit_ts = std::numeric_limits<TXID>::max();  // TODO: align with GC
   u8 key[sizeof(TXID)];
   utils::fold(key, start_ts);
   other(worker_id).commit_tree->prefixLookup(key, sizeof(TXID), [&](const u8* s_key, u16, const u8*, u16) { utils::unfold(s_key, commit_ts); });
   ensure(commit_ts > start_ts);
   return commit_ts;
}
// -------------------------------------------------------------------------------------
// It is also used to check whether the tuple is write-locked, hence we need the to_write intention flag
bool Worker::ConcurrencyControl::isVisibleForMe(WORKERID other_worker_id, u64 tx_ts, bool to_write)
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
   if (my().worker_id == other_worker_id) {
      return true;
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_si_commit_protocol == 1) {
      // Same as variant 0
   } else if (FLAGS_si_commit_protocol == 2) {
      if (tx_ts < wt_pg.current_snapshot_min_tx_id) {
         return true;
      }
      if (tx_ts > wt_pg.current_snapshot_max_tx_id) {
         return false;
      }
      for (auto& o_tx_id : wt_pg.local_workers_tx_id) {
         if (o_tx_id == tx_ts) {
            return false;
         }
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   if (activeTX().isReadCommitted() || activeTX().isReadUncommitted()) {
      if (is_commit_ts) {
         return true;
      }
      u8 key[sizeof(TXID)];
      utils::fold(key, std::numeric_limits<TXID>::max());
      TXID committed_till = 0;
      other(other_worker_id).commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) {
         committed_till = *reinterpret_cast<const TXID*>(s_value);
      });
      return committed_till >= tx_ts;
   } else if (activeTX().atLeastSI()) {
      if (is_commit_ts) {
         return my().active_tx.startTS() > committed_ts;
      }
      if (start_ts < local_global_all_lwm_cache) {
         return true;
      }
      // -------------------------------------------------------------------------------------
      if (local_snapshot_cache_ts[other_worker_id] == activeTX().startTS()) {  // Use the cache
         return local_snapshot_cache[other_worker_id] >= start_ts;
      } else if (local_snapshot_cache[other_worker_id] >= start_ts) {
         return true;
      }
      TXID largest_visible_tx_id = 0;
      u8 key[sizeof(TXID)];
      utils::fold(key, my().active_tx.startTS());
      OP_RESULT ret = other(other_worker_id).commit_tree->prefixLookupForPrev(key, sizeof(TXID), [&](const u8*, u16, const u8* s_value, u16) {
         largest_visible_tx_id = *reinterpret_cast<const TXID*>(s_value);
      });
      if (ret == OP_RESULT::OK) {
         local_snapshot_cache[other_worker_id] = largest_visible_tx_id;
         local_snapshot_cache_ts[other_worker_id] = my().active_tx.startTS();
         return largest_visible_tx_id >= start_ts;
      }
      return false;
   } else {
      UNREACHABLE();
   }
}
// -------------------------------------------------------------------------------------
bool Worker::ConcurrencyControl::isVisibleForAll(WORKERID, TXID ts)
{
   ensure(FLAGS_si_commit_protocol == 0);
   if (ts & MSB) {
      // Commit Timestamp
      return (ts & MSB_MASK) < global_oldest_all_start_ts.load();
   } else {
      // Start Timestamp
      return ts < global_all_lwm.load();
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
