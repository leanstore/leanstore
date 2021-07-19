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
atomic<u64> Worker::global_logical_clock = 0;
atomic<u64> Worker::global_gsn_flushed = 0;
atomic<u64> Worker::global_sync_to_this_gsn = 0;
std::mutex Worker::global_mutex;
std::unique_ptr<atomic<u64>[]> Worker::global_tx_start_timestamps;
std::unique_ptr<atomic<u64>[]> Worker::global_workers_commit_marks;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, s32 fd)
    : worker_id(worker_id),
      all_workers(all_workers),
      workers_count(workers_count),
      ssd_fd(fd),
      todo_hwm_rb(1024ull * 1024 * 50),
      todo_lwm_rb(1024ull * 1024 * 50)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   std::memset(wal_buffer, 0, WORKER_WAL_SIZE);
   local_workers_commit_marks = make_unique<atomic<u64>[]>(workers_count);
   local_tx_start_timestamps = make_unique<u64[]>(workers_count);
   local_sorted_tx_start_timestamps = make_unique<u64[]>(workers_count);
   global_tx_start_timestamps[worker_id] = global_logical_clock.fetch_add(WORKERS_INCREMENT) | worker_id;
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
   tx_start = global_logical_clock.fetch_add(WORKERS_INCREMENT) | worker_id;
   global_tx_start_timestamps[worker_id].store(tx_start, std::memory_order_release);
   // -------------------------------------------------------------------------------------
   for (u64 w = 0; w < workers_count; w++) {
      local_workers_commit_marks[w].store(global_workers_commit_marks[w], std::memory_order_release);
   }
}
// -------------------------------------------------------------------------------------
void Worker::refreshSnapshotOrderingIfNeeded()
{
   if (!snapshot_order_refreshed && (FLAGS_si_refresh_rate == 0 || active_tx.commit_mark % FLAGS_si_refresh_rate == 0)) {
      // cout << "refresh" << endl;
      oldest_tx_start = std::numeric_limits<u64>::max();
      oldest_tx_start_worker_id = worker_id;
      for (u64 w = 0; w < workers_count; w++) {
         const u64 its_tx_start = global_tx_start_timestamps[w].load();
         if (its_tx_start < oldest_tx_start) {
            oldest_tx_start = its_tx_start;
            oldest_tx_start_worker_id = w;
         }
         local_tx_start_timestamps[w] = its_tx_start;
      }
      workers_sorted = false;
      snapshot_order_refreshed = true;
   }
}
// -------------------------------------------------------------------------------------
void Worker::refreshSnapshot()
{
   refreshSnapshotOrderingIfNeeded();
   refreshSnapshotHWMs();
}
// -------------------------------------------------------------------------------------
void Worker::sortWorkers()
{
   if (!workers_sorted) {
      refreshSnapshotOrderingIfNeeded();
      // Avoid extra work if the last round also was full of single statement workers
      if (oldest_tx_start < std::numeric_limits<u64>::max()) {
         std::memcpy(local_sorted_tx_start_timestamps.get(), local_tx_start_timestamps.get(), workers_count * sizeof(u64));
         std::sort(local_sorted_tx_start_timestamps.get(), local_sorted_tx_start_timestamps.get() + workers_count, std::greater<u64>());
      } else if (local_sorted_tx_start_timestamps[workers_count - 1] < std::numeric_limits<u64>::max()) {
         std::memcpy(local_sorted_tx_start_timestamps.get(), local_tx_start_timestamps.get(), workers_count * sizeof(u64));
      }
   }
   workers_sorted = true;
}
// -------------------------------------------------------------------------------------
void Worker::startTX(TX_MODE next_tx_type, TX_ISOLATION_LEVEL next_tx_isolation_level)
{
   // For single-statement transactions, snapshot isolation and serialization are the same as read committed
   if (next_tx_type == TX_MODE::SINGLE_READONLY || next_tx_type == TX_MODE::SINGLE_READWRITE) {
      if (next_tx_isolation_level > TX_ISOLATION_LEVEL::READ_COMMITTED) {
         next_tx_isolation_level = TX_ISOLATION_LEVEL::READ_COMMITTED;
      }
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      current_tx_wal_start = wal_wt_cursor;
      if (next_tx_type != TX_MODE::SINGLE_READONLY && next_tx_type != TX_MODE::LONG_READONLY) {
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
      active_tx.commit_mark = global_workers_commit_marks[worker_id];
      active_tx.min_observed_gsn_when_started = clock_gsn;
      // -------------------------------------------------------------------------------------
      if (FLAGS_commit_hwm) {
         snapshot_order_refreshed = false;
         if (next_tx_isolation_level > TX_ISOLATION_LEVEL::READ_COMMITTED) {
            if (force_si_refresh || FLAGS_si_refresh_rate == 0 || active_tx.commit_mark % FLAGS_si_refresh_rate == 0) {
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
   }
   active_tx.current_tx_mode = next_tx_type;
   active_tx.current_tx_isolation_level = next_tx_isolation_level;
   active_tx.is_durable = FLAGS_wal;  // TODO:
}
// -------------------------------------------------------------------------------------
void Worker::switchToAlwaysUpToDateMode()
{
   global_tx_start_timestamps[worker_id].store((std::numeric_limits<u64>::max() - WORKERS_INCREMENT + worker_id) & ~(1ull << 63),
                                               std::memory_order_release);
   for (u64 w = 0; w < workers_count; w++) {
      local_workers_commit_marks[w].store(std::numeric_limits<u64>::max(), std::memory_order_release);
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
      if (todo_hwm_rb.empty() && todo_lwm_rb.empty()) {
         return;
      }
      refreshSnapshotOrderingIfNeeded();
      // -------------------------------------------------------------------------------------
      {
         auto& rb = todo_hwm_rb;
         while (FLAGS_todo && !rb.empty()) {
            auto& todo = *reinterpret_cast<TODOEntry*>(rb.front());
            if (oldest_tx_start > todo.after_so) {
               WorkerCounters::myCounters().cc_rtodo_lng_executed[todo.dt_id]++;
               leanstore::storage::DTRegistry::global_dt_registry.todo(todo.dt_id, todo.payload, todo.version_worker_id,
                                                                       todo.version_worker_commit_mark);
               rb.popFront();
            } else {
               WorkerCounters::myCounters().cc_todo_1_break[todo.dt_id]++;
               break;
            }
         }
      }
      {
         auto& rb = todo_lwm_rb;
         while (FLAGS_todo && !rb.empty()) {
            auto& todo = *reinterpret_cast<TODOEntry*>(rb.front());
            ensure(todo.or_before_so > 0);
            if (oldest_tx_start > todo.after_so) {
               WorkerCounters::myCounters().cc_rtodo_shrt_executed[todo.dt_id]++;
               leanstore::storage::DTRegistry::global_dt_registry.todo(todo.dt_id, todo.payload, todo.version_worker_id,
                                                                       todo.version_worker_commit_mark);
               rb.popFront();
            } else {
               bool safe_to_gc = true;
               WorkerCounters::myCounters().cc_rtodo_opt_considered[todo.dt_id]++;
               for (u64 w_i = 0; w_i < workers_count && (safe_to_gc); w_i++) {
                  if (local_tx_start_timestamps[w_i] > todo.after_so) {
                     safe_to_gc &= true;
                  } else {
                     auto decomposed = decomposeWIDCM(todo.or_before_so);
                     safe_to_gc &= !isVisibleForIt(w_i, std::get<0>(decomposed), std::get<1>(decomposed));
                  }
               }
               if (safe_to_gc) {
                  WorkerCounters::myCounters().cc_rtodo_opt_executed[todo.dt_id]++;
                  leanstore::storage::DTRegistry::global_dt_registry.todo(todo.dt_id, todo.payload, todo.version_worker_id,
                                                                          todo.version_worker_commit_mark);
                  rb.popFront();
               } else {
                  break;
               }
            }
         }
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (activeTX().isDurable()) {
      if (activeTX().isSingleStatement() && active_tx.state != Transaction::STATE::STARTED) {
         return;  // Skip double commit in case of single statement upsert [hacky]
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
         active_tx.state = Transaction::STATE::COMMITED;
         CRCounters::myCounters().rfa_committed_tx += 1;
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_commit_hwm) {
         if (!activeTX().isReadOnly()) {
            global_workers_commit_marks[worker_id].store(active_tx.commit_mark + 1, std::memory_order_release);
            commitTODOs(global_logical_clock.fetch_add(WORKERS_INCREMENT));
         }
      }
      executeUnlockTasks();
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   if (FLAGS_wal) {
      ensure(active_tx.state == Transaction::STATE::STARTED);
      iterateOverCurrentTXEntries([&](const WALEntry& entry) {
         const u64 tts = active_tx.commit_mark;
         if (entry.type == WALEntry::TYPE::DT_SPECIFIC) {
            const auto& dt_entry = *reinterpret_cast<const WALDTEntry*>(&entry);
            leanstore::storage::DTRegistry::global_dt_registry.undo(dt_entry.dt_id, dt_entry.payload, tts);
         }
      });
      // -------------------------------------------------------------------------------------
      executeUnlockTasks();
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
   return what_worker_id == whom_worker_id || (all_workers[whom_worker_id]->local_workers_commit_marks[what_worker_id] > tts);
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
      if (local_workers_commit_marks[other_worker_id].load() > tts) {
         return true;
      } else if (activeTX().isSingleStatement() || activeTX().isReadCommitted()) {
         // Single statement transaction can refresh their vector on-demand
         local_workers_commit_marks[other_worker_id].store(global_workers_commit_marks[other_worker_id].load(), std::memory_order_release);
         return local_workers_commit_marks[other_worker_id].load() > tts;
      } else {
         return false;
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
bool Worker::isVisibleForAll(u64 commited_before_so)
{
   if (activeTX().isSingleStatement()) {
      refreshSnapshotOrderingIfNeeded();
   }
   return commited_before_so < oldest_tx_start;
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
// TODO: rename or_before_so
void Worker::stageTODO(u8 worker_id, u64 tts, DTID dt_id, u64 payload_length, std::function<void(u8*)> cb, u64 head_widcm)
{
   u8* todo_ptr;
   const u64 total_todo_length = payload_length + sizeof(TODOEntry);
   auto decompose = decomposeWIDCM(head_widcm);
   if (FLAGS_vi_twoq_todo && head_widcm && !isVisibleForIt(oldest_tx_start_worker_id, std::get<0>(decompose), std::get<1>(decompose))) {
      // TODO: Alternatively, we can check if oldest_tx_start > start_tx of the chain head
      todo_ptr = todo_lwm_rb.pushBack(total_todo_length);
      if (todo_lwm_tx_start == nullptr) {
         todo_lwm_tx_start = todo_ptr;
      }
      WorkerCounters::myCounters().cc_rtodo_opt_staged[dt_id]++;
   } else {
      head_widcm = 0;
      todo_ptr = todo_hwm_rb.pushBack(total_todo_length);
      if (todo_hwm_tx_start == nullptr) {
         todo_hwm_tx_start = todo_ptr;
      }
   }
   auto& todo_entry = *new (todo_ptr) TODOEntry();
   todo_entry.version_worker_id = worker_id;
   todo_entry.version_worker_commit_mark = tts;
   todo_entry.dt_id = dt_id;
   todo_entry.payload_length = payload_length;
   todo_entry.or_before_so = head_widcm;
   // -------------------------------------------------------------------------------------
   cb(todo_entry.payload);
}
// -------------------------------------------------------------------------------------
void Worker::commitTODOs(u64 so)
{
   if (todo_hwm_tx_start) {
      todo_hwm_rb.iterateUntilTail(todo_hwm_tx_start, [&](u8* rb_payload) { reinterpret_cast<TODOEntry*>(rb_payload)->after_so = so; });
      todo_hwm_tx_start = nullptr;
   }
   if (todo_lwm_tx_start) {
      todo_lwm_rb.iterateUntilTail(todo_lwm_tx_start, [&](u8* rb_payload) { reinterpret_cast<TODOEntry*>(rb_payload)->after_so = so; });
      todo_lwm_tx_start = nullptr;
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTODO(u8 worker_id, u64 worker_cm, u64 after_so, DTID dt_id, u64 payload_length, std::function<void(u8*)> cb)
{
   const u64 total_todo_length = sizeof(TODOEntry) + payload_length;
   u8* todo_ptr = todo_hwm_rb.pushBack(total_todo_length);
   auto& todo_entry = *new (todo_ptr) TODOEntry();
   todo_entry.version_worker_id = worker_id;
   todo_entry.version_worker_commit_mark = worker_cm;
   todo_entry.dt_id = dt_id;
   todo_entry.payload_length = payload_length;
   todo_entry.after_so = after_so;
   todo_entry.or_before_so = 0;
   cb(todo_entry.payload);
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
