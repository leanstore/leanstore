#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <stdio.h>

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
std::mutex Worker::global_mutex;                                         // Unused
std::unique_ptr<atomic<u64>[]> Worker::global_workers_in_progress_txid;  // All transactions < are committed
std::unique_ptr<atomic<u64>[]> Worker::global_workers_rv_start;          //
// -------------------------------------------------------------------------------------
std::unique_ptr<atomic<u64>[]> Worker::global_workers_oltp_lwm;
std::unique_ptr<atomic<u64>[]> Worker::global_workers_olap_lwm;
atomic<u64> Worker::global_oltp_lwm = 0;  // No worker should start with TTS == 0
atomic<u64> Worker::global_olap_lwm = 0;  // No worker should start with TTS == 0
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
      local_workers_in_progress_txids = make_unique<atomic<u64>[]>(workers_count);
      local_workers_sorted_txids = make_unique<u64[]>(workers_count + 1);
      local_workers_rv_start = make_unique<u64[]>(workers_count + 1);
      local_workers_olap_lwm = make_unique<u64[]>(workers_count);
      global_workers_in_progress_txid[worker_id] = 0;
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
   wal_wt_cursor += total_size;
   publishMaxGSNOffset();
}
// -------------------------------------------------------------------------------------
void Worker::refreshSnapshotHWMs()
{
   local_oldest_olap_tx_worker_id = std::numeric_limits<u64>::max();
   local_oldest_oltp_tx_id = std::numeric_limits<u64>::max();
   local_oldest_olap_tx_id = std::numeric_limits<u64>::max();
   bool is_oltp_same_as_olap = true;
   local_workers_cut_timestamp = global_logical_clock.load();
   olap_in_progress_tx_count = 0;
   global_workers_rv_start[worker_id].store(global_logical_clock.fetch_add(1), std::memory_order_release);
   for (u64 w_i = 0; w_i < workers_count; w_i++) {
      u64 its_in_flight_tx_id = global_workers_in_progress_txid[w_i];
      const bool is_rc = its_in_flight_tx_id & RC_BIT;
      const bool is_olap = its_in_flight_tx_id & OLAP_BIT;
      olap_in_progress_tx_count += is_olap;
      its_in_flight_tx_id &= CLEAN_BITS_MASK;
      is_oltp_same_as_olap &= !is_olap;
      if (!is_rc) {
         local_oldest_olap_tx_id = std::min<u64>(its_in_flight_tx_id, local_oldest_olap_tx_id);
         local_oldest_olap_tx_worker_id = w_i;
         if (!is_olap) {
            local_oldest_oltp_tx_id = std::min<u64>(its_in_flight_tx_id, local_oldest_oltp_tx_id);
         }
      }
      its_in_flight_tx_id &= ~(1ull << 63);
      local_workers_in_progress_txids[w_i].store(its_in_flight_tx_id, std::memory_order_release);
   }
   workers_sorted = false;
   if (is_oltp_same_as_olap) {
      local_oldest_oltp_tx_id = local_oldest_olap_tx_id;
   }
   global_workers_olap_lwm[worker_id].store(local_oldest_olap_tx_id | ((is_oltp_same_as_olap) ? OLTP_OLAP_SAME_BIT : 0), std::memory_order_release);
   if (!is_oltp_same_as_olap) {
      global_workers_oltp_lwm[worker_id].store(local_oldest_oltp_tx_id, std::memory_order_release);
   }
   // -------------------------------------------------------------------------------------
   const TXID tx_id = global_logical_clock.fetch_add(1);
   global_workers_in_progress_txid[worker_id].store(tx_id | ((active_tx.isOLAP()) ? OLAP_BIT : 0), std::memory_order_release);
   active_tx.tx_id = tx_id;
   // -------------------------------------------------------------------------------------
   // Special handling for OLAP tx
   if (active_tx.isOLAP()) {
      global_workers_olap_lwm[worker_id].store(local_oldest_olap_tx_id, std::memory_order_release);
      global_workers_oltp_lwm[worker_id].store(std::numeric_limits<u64>::max(), std::memory_order_release);
   }
   // -------------------------------------------------------------------------------------
   // Receiving
   local_olap_lwm = std::numeric_limits<u64>::max();
   local_oltp_lwm = std::numeric_limits<u64>::max();
   for (u64 w_i = 0; w_i < workers_count; w_i++) {
      u64 its_olap_lwm = global_workers_olap_lwm[w_i].load();
      bool is_olap_same_as_oltp_lwm = its_olap_lwm & OLTP_OLAP_SAME_BIT;
      its_olap_lwm &= CLEAN_BITS_MASK;
      local_workers_olap_lwm[w_i] = its_olap_lwm;
      local_olap_lwm = std::min<u64>(local_olap_lwm, its_olap_lwm);
      if (is_olap_same_as_oltp_lwm) {
         local_oltp_lwm = std::min<u64>(local_oltp_lwm, its_olap_lwm);
      } else {
         local_oltp_lwm = std::min<u64>(local_oltp_lwm, global_workers_oltp_lwm[w_i].load());
      }
   }
   // -------------------------------------------------------------------------------------
   u64 current_global_olap_lwm = global_olap_lwm.load();
   while (local_olap_lwm > current_global_olap_lwm) {
      if (global_olap_lwm.compare_exchange_strong(current_global_olap_lwm, local_olap_lwm))
         break;
   }
   // -------------------------------------------------------------------------------------
   relations_cut_from_snapshot.reset();
}
// -------------------------------------------------------------------------------------
void Worker::prepareForIntervalGC()
{
   if (!workers_sorted) {
      COUNTERS_BLOCK() { CRCounters::myCounters().cc_prepare_igc++; }
      local_workers_sorted_txids[0] = 0;
      for (u64 w_i = 0; w_i < workers_count; w_i++) {
         local_workers_rv_start[w_i] = global_workers_rv_start[w_i].load();
         local_workers_sorted_txids[w_i + 1] = (local_workers_in_progress_txids[w_i] << WORKERS_BITS) | w_i;
      }
      // Avoid extra work if the last round also was full of single statement workers
      if (1 || local_oldest_olap_tx_id < std::numeric_limits<u64>::max()) {  // TODO: Disable
         std::sort(local_workers_sorted_txids.get(), local_workers_sorted_txids.get() + workers_count + 1, std::less<u64>());
         assert(local_workers_sorted_txids[0] <= local_workers_sorted_txids[1]);
      }
   }
   workers_sorted = true;
}
// -------------------------------------------------------------------------------------
void Worker::startTX(TX_MODE next_tx_type, TX_ISOLATION_LEVEL next_tx_isolation_level, bool read_only)
{
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
      assert(active_tx.state != Transaction::STATE::STARTED);
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
      active_tx.min_observed_gsn_when_started = clock_gsn;
      // -------------------------------------------------------------------------------------
      transactions_order_refreshed = false;
      if (next_tx_isolation_level >= TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION) {
         if (force_si_refresh || FLAGS_si_refresh_rate == 0) {
            refreshSnapshotHWMs();
         }
         force_si_refresh = false;
      } else {
         if (activeTX().atLeastSI()) {
            switchToAlwaysUpToDateMode();
         }
      }
      // -------------------------------------------------------------------------------------
      checkup();
   }
   active_tx.current_tx_mode = next_tx_type;
   active_tx.current_tx_isolation_level = next_tx_isolation_level;
   active_tx.is_read_only = read_only;
   active_tx.is_durable = FLAGS_wal;  // TODO:
}
// -------------------------------------------------------------------------------------
void Worker::switchToAlwaysUpToDateMode()
{
   {
      const u64 last_commit_mark_flagged = global_workers_in_progress_txid[worker_id].load() | (1ull << 63);
      global_workers_in_progress_txid[worker_id].store(last_commit_mark_flagged, std::memory_order_release);
   }
   global_workers_olap_lwm[worker_id].store(std::numeric_limits<u64>::max(), std::memory_order_release);
   global_workers_oltp_lwm[worker_id].store(std::numeric_limits<u64>::max(), std::memory_order_release);
   for (u64 w_i = 0; w_i < workers_count; w_i++) {
      local_workers_in_progress_txids[w_i].store(1ull << 63, std::memory_order_release);
   }
}
// -------------------------------------------------------------------------------------
void Worker::shutdown()
{
   checkup();
   switchToAlwaysUpToDateMode();
}
// -------------------------------------------------------------------------------------
// Pre: Refresh Snapshot Ordering
void Worker::checkup()
{
   if (FLAGS_commit_hwm) {
      if (!FLAGS_todo) {
         return;
      }
      // TODO: smooth purge, we should not let the system hang on this, as a quick fix, it should be enough if we purge in small batches
      if (local_olap_lwm > 0) {
         // PURGE!
         versions_space.purgeVersions(
             worker_id, 0, local_olap_lwm - 1,
             [&](const TXID tx_id, const DTID dt_id, const u8* version_payload, [[maybe_unused]] u64 version_payload_length,
                 const bool called_before) {
                leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id, called_before);
                COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_olap_executed[dt_id]++; }
             },
             FLAGS_todo_batch_size);
         cleaned_untill_oltp_lwm = std::max(local_olap_lwm, cleaned_untill_oltp_lwm);
      }
      if (FLAGS_olap_mode && local_oltp_lwm > 0 && local_oltp_lwm > cleaned_untill_oltp_lwm) {
         // MOVE deletes to the graveyard
         const u64 from_tx_id = cleaned_untill_oltp_lwm > 0 ? cleaned_untill_oltp_lwm : 0;
         versions_space.visitRemoveVersions(worker_id, from_tx_id, local_oltp_lwm - 1,
                                            [&](const TXID tx_id, const DTID dt_id, const u8* version_payload,
                                                [[maybe_unused]] u64 version_payload_length, const bool called_before) {
                                               leanstore::storage::DTRegistry::global_dt_registry.todo(dt_id, version_payload, worker_id, tx_id,
                                                                                                       called_before);
                                               COUNTERS_BLOCK() { WorkerCounters::myCounters().cc_todo_oltp_executed[dt_id]++; }
                                            });
         cleaned_untill_oltp_lwm = local_oltp_lwm;
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (activeTX().isDurable()) {
      command_id = 0;
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
      if (FLAGS_commit_hwm) {
         if (!activeTX().isReadOnly()) {
            // ATTENTION: make sure that only TX with tx_id larger than ours can see our commit
            global_workers_in_progress_txid[worker_id].store(active_tx.TTS() + 1, std::memory_order_release);
         }
      }
      if (activeTX().isSerializable()) {
         executeUnlockTasks();
      }
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
      force_si_refresh = true;
   }
   jumpmu::jump();
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForIt(u8 whom_worker_id, u8 what_worker_id, u64 tts)
{
   COUNTERS_BLOCK() { CRCounters::myCounters().cc_cross_workers_visibility_check++; }
   return what_worker_id == whom_worker_id || (all_workers[whom_worker_id]->local_workers_in_progress_txids[what_worker_id] > tts);
}
// -------------------------------------------------------------------------------------
// It is also used to check whether the tuple is write-locked, hence we need the to_write intention flag
// There are/will be two types of write locks: ones that are released with commit hwm and ones that are manually released after commit.
bool Worker::isVisibleForMe(u8 other_worker_id, u64 tts, bool to_write)
{
   if (!to_write && activeTX().isReadUncommitted()) {
      return true;
   }
   if (worker_id == other_worker_id) {
      return true;
   } else {
      if (activeTX().isReadCommitted() || activeTX().isReadUncommitted()) {
         const u64 masked_commit_mark = local_workers_in_progress_txids[other_worker_id].load() & ~(1ull << 63);
         if (masked_commit_mark > tts) {
            return true;
         } else {
            // Refresh
            const u64 its_commit_mark = global_workers_in_progress_txid[other_worker_id].load() & ~(1ull << 63);
            const u64 flagged_commit_mark = its_commit_mark | (1ull << 63);
            local_workers_in_progress_txids[other_worker_id].store(flagged_commit_mark, std::memory_order_release);
            return (its_commit_mark > tts);
         }
      } else if (activeTX().atLeastSI()) {
         return local_workers_in_progress_txids[other_worker_id].load() > tts;
      } else {
         UNREACHABLE();
      }
   }
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForMe(u64 wtts)
{
   const u64 other_worker_id = wtts % workers_count;
   const u64 tts = wtts & ~(255ull << 56);
   return isVisibleForMe(other_worker_id, tts);
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForAll(u64 tx_id)
{
   return tx_id < global_olap_lwm.load();
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
void Worker::getWALDTEntryPayload(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(u8*)> callback)
{
   all_workers[worker_id]->getWALEntry(lsn, in_memory_offset, [&](WALEntry* entry) { callback(reinterpret_cast<WALDTEntry*>(entry)->payload); });
}
// -------------------------------------------------------------------------------------
void Worker::getWALEntry(u8 worker_id, LID lsn, u32 in_memory_offset, std::function<void(WALEntry*)> callback)
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
