#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tls_ptr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count, s32 fd)
    : worker_id(worker_id), all_workers(all_workers), workers_count(workers_count), fd(fd)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   active_tts.store(worker_id, std::memory_order_release);
   my_snapshot = make_unique<u64[]>(workers_count);
   my_concurrent_transcations = make_unique<u64[]>(workers_count);
}
Worker::~Worker() {}
// -------------------------------------------------------------------------------------
u32 Worker::walFreeSpace()
{
   // A , B , C : a - b + c % c
   auto ww_cursor = wal_ww_cursor.load();
   if (ww_cursor == wal_wt_cursor) {
      return WORKER_WAL_SIZE;
   } else if (ww_cursor < wal_wt_cursor) {
      return ww_cursor + (WORKER_WAL_SIZE - wal_wt_cursor);
   } else {
      return ww_cursor - wal_wt_cursor;
   }
}
// -------------------------------------------------------------------------------------
u32 Worker::walContiguousFreeSpace()
{
   return WORKER_WAL_SIZE - wal_wt_cursor;
}
// -------------------------------------------------------------------------------------
void Worker::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
      // Spin until we have enough space
      while (walFreeSpace() < (requested_size + CR_ENTRY_SIZE)) {
      }
      if (walContiguousFreeSpace() < (requested_size + CR_ENTRY_SIZE)) {  // always keep place for CR entry
         auto& entry = *reinterpret_cast<WALEntry*>(wal_buffer + wal_wt_cursor);
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = WORKER_WAL_SIZE - wal_wt_cursor;
         wal_wt_cursor = 0;
      }
   }
}
// -------------------------------------------------------------------------------------
WALMetaEntry& Worker::reserveWALMetaEntry()
{
   walEnsureEnoughSpace(sizeof(WALMetaEntry));
   return *reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_wt_cursor);
}
// -------------------------------------------------------------------------------------
void Worker::submitWALMetaEntry()
{
   const u64 next_wal_wt_cursor = wal_wt_cursor + sizeof(WALMetaEntry);
   wal_wt_cursor.store(next_wal_wt_cursor, std::memory_order_release);
}
// -------------------------------------------------------------------------------------
void Worker::submitDTEntry(u64 total_size)
{
   std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
   const u64 next_wt_cursor = wal_wt_cursor + total_size;
   wal_wt_cursor.store(next_wt_cursor, std::memory_order_relaxed);
   wal_max_gsn.store(clock_gsn, std::memory_order_relaxed);
}
// -------------------------------------------------------------------------------------
void Worker::startTX()
{
   if (FLAGS_wal) {
      current_tx_wal_start = wal_wt_cursor;
      WALMetaEntry& entry = reserveWALMetaEntry();
      entry.size = sizeof(WALMetaEntry) + 0;
      entry.type = WALEntry::TYPE::TX_START;
      entry.lsn = wal_lsn_counter++;
      submitWALMetaEntry();
      assert(tx.state != Transaction::STATE::STARTED);
      tx.state = Transaction::STATE::STARTED;
      tx.min_gsn = clock_gsn;
      if (FLAGS_si) {
         for (u64 w = 0; w < workers_count; w++) {
            my_snapshot[w] = all_workers[w]->high_water_mark;
            my_concurrent_transcations[w] = all_workers[w]->active_tts;
         }
         std::sort(my_concurrent_transcations.get(), my_concurrent_transcations.get() + workers_count, std::greater<int>());
         tx.tx_id = active_tts.fetch_add(1);
         active_tts.store(tx.tx_id, std::memory_order_release);
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (FLAGS_wal) {
      assert(tx.state == Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      // TODO: MVCC, actually nothing when it comes to our SI plan
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = reserveWALMetaEntry();
      entry.size = sizeof(WALMetaEntry) + 0;
      entry.type = WALEntry::TYPE::TX_COMMIT;
      entry.lsn = wal_lsn_counter++;
      submitWALMetaEntry();
      // -------------------------------------------------------------------------------------
      tx.max_gsn = clock_gsn;
      tx.state = Transaction::STATE::READY_TO_COMMIT;
      {
         std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
         ready_to_commit_queue.push_back(tx);
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   if (FLAGS_wal) {
      iterateOverCurrentTXEntries([&](const WALEntry& entry) {
         const u64 tts = active_tts;
         if (entry.type == WALEntry::TYPE::DT_SPECIFIC) {
            const auto& dt_entry = *reinterpret_cast<const WALDTEntry*>(&entry);
            leanstore::storage::DTRegistry::global_dt_registry.undo(dt_entry.dt_id, dt_entry.payload, tts);
         }
      });
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = reserveWALMetaEntry();
      entry.size = sizeof(WALMetaEntry) + 0;
      entry.type = WALEntry::TYPE::TX_ABORT;
      entry.lsn = wal_lsn_counter++;
      submitWALMetaEntry();
      tx.state = Transaction::STATE::ABORTED;
   }
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForMe(u8 other_worker_id, u64 tts)
{
   return worker_id == other_worker_id || my_snapshot[other_worker_id] > tts;
}
// -------------------------------------------------------------------------------------
bool Worker::isVisibleForMe(u64 wtts)
{
   const u64 worker_id = wtts % workers_count;
   const u64 tts = wtts & ~(255ull < 63);
   return wtts == active_tts || my_snapshot[worker_id] > tts;
}
// -------------------------------------------------------------------------------------
// Called by worker, so concurrent writes on the buffer
void Worker::iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback)
{
   u64 cursor = current_tx_wal_start;
   while (cursor != wal_wt_cursor) {
      const WALEntry& entry = *reinterpret_cast<WALEntry*>(wal_buffer + cursor);
      if (entry.type == WALEntry::TYPE::CARRIAGE_RETURN) {
         cursor = 0;
      } else {
         callback(entry);
         cursor += entry.size;
      }
   }
}
// -------------------------------------------------------------------------------------
u64 Worker::WALFinder::getLowerBound(LID lsn)
{
   std::unique_lock guard(m);
   return ht.lower_bound(lsn)->second;
}
// -------------------------------------------------------------------------------------
void Worker::WALFinder::insertLowerBound(LID LSN, u64 ssd_offset)
{
   std::unique_lock guard(m);
   ht[LSN] = ssd_offset;
}
// -------------------------------------------------------------------------------------
void Worker::getWALDTEntry(u8 worker_id, LID lsn, std::function<void(u8*)> callback)
{
   auto wal_entry = getWALEntry(worker_id, lsn);
   auto dt_entry = reinterpret_cast<WALDTEntry*>(wal_entry.get());
   callback(dt_entry->payload);
}
// -------------------------------------------------------------------------------------
std::unique_ptr<u8[]> Worker::getWALEntry(u8 worker_id, LID lsn)
{
   return all_workers[worker_id]->getWALEntry(lsn);
}
// -------------------------------------------------------------------------------------
std::unique_ptr<u8[]> Worker::getWALEntry(LID lsn)
{
   // 1- Scan the local buffer
   const u64 initial_lower_bound = wal_finder.getLowerBound(lsn);
   WALEntry* entry = reinterpret_cast<WALEntry*>(wal_buffer);
   while ((reinterpret_cast<u8*>(entry) - wal_buffer) < WORKER_WAL_SIZE) {
      if (entry->lsn == lsn) {
         std::unique_ptr<u8[]> entry_buffer = std::make_unique<u8[]>(entry->size);
         std::memcpy(entry_buffer.get(), entry, entry->size);
         if (wal_finder.getLowerBound(lsn) == initial_lower_bound) {
            return entry_buffer;
         } else {
            break;
         }
      }
      entry += entry->size;
   }
   // 2- Read from SSD, accelerate using getLowerBound
   // TODO: optimize, do not read 10 MiB!
   const u64 lower_bound = wal_finder.getLowerBound(lsn);
   std::unique_ptr<u8[]> log_chunk = std::make_unique<u8[]>(WORKER_WAL_SIZE);
   assert(lower_bound % 512 == 0);
   pread(fd, log_chunk.get(), lower_bound, WORKER_WAL_SIZE);
   entry = reinterpret_cast<WALEntry*>(log_chunk.get());
   while ((reinterpret_cast<u8*>(entry) - log_chunk.get()) < WORKER_WAL_SIZE) {
      if (entry->lsn == lsn) {
         std::unique_ptr<u8[]> entry_buffer = std::make_unique<u8[]>(entry->size);
         std::memcpy(entry_buffer.get(), entry, entry->size);
         return entry_buffer;
      }
      entry += entry->size;
   }
   ensure(false);
   return nullptr;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
