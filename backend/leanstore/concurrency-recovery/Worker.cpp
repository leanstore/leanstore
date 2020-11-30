#include "Worker.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cstdlib>
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
    : worker_id(worker_id), all_workers(all_workers), workers_count(workers_count), ssd_fd(fd)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
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
         entry.lsn = wal_lsn_counter++;
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = WORKER_WAL_SIZE - wal_wt_cursor;
         wal_buffer_round++;  // Carriage Return
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
      assert(active_tx.state != Transaction::STATE::STARTED);
      active_tx.state = Transaction::STATE::STARTED;
      active_tx.min_gsn = clock_gsn;
      if (FLAGS_si) {
         for (u64 w = 0; w < workers_count; w++) {
            my_snapshot[w] = all_workers[w]->high_water_mark;
            my_concurrent_transcations[w] = all_workers[w]->next_tts;
         }
         std::sort(my_concurrent_transcations.get(), my_concurrent_transcations.get() + workers_count, std::greater<int>());
         active_tx.tts = next_tts.fetch_add(1);
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   if (FLAGS_wal) {
      assert(active_tx.state == Transaction::STATE::STARTED);
      // -------------------------------------------------------------------------------------
      // TODO: MVCC, actually nothing when it comes to our SI plan
      // -------------------------------------------------------------------------------------
      WALMetaEntry& entry = reserveWALMetaEntry();
      entry.size = sizeof(WALMetaEntry) + 0;
      entry.type = WALEntry::TYPE::TX_COMMIT;
      entry.lsn = wal_lsn_counter++;
      submitWALMetaEntry();
      // -------------------------------------------------------------------------------------
      active_tx.max_gsn = clock_gsn;
      active_tx.state = Transaction::STATE::READY_TO_COMMIT;
      {
         std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
         ready_to_commit_queue.push_back(active_tx);
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   if (FLAGS_wal) {
      iterateOverCurrentTXEntries([&](const WALEntry& entry) {
         const u64 tts = active_tx.tts;
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
      active_tx.state = Transaction::STATE::ABORTED;
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
   const u64 other_worker_id = wtts % workers_count;
   const u64 tts = wtts & ~(255ull << 56);
   return isVisibleForMe(other_worker_id, tts);
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
WALChunk::Slot Worker::WALFinder::getJumpPoint(LID lsn)
{
   std::unique_lock guard(m);
   if (ht.size() == 0) {
      return {0, 0};
   } else {
      auto iter = ht.lower_bound(lsn);
      if (iter->first == lsn) {
         return iter->second;
      } else {
         iter = std::prev(iter);
         return iter->second;
      }
   }
}
// -------------------------------------------------------------------------------------
void Worker::WALFinder::insertJumpPoint(LID LSN, WALChunk::Slot slot)
{
   std::unique_lock guard(m);
   ht[LSN] = slot;
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
restart : {
   // 1- Scan the local buffer
   bool failed = false;
   {
      WALEntry* entry = reinterpret_cast<WALEntry*>(wal_buffer);
      u64 offset = 0;
      while (offset < WORKER_WAL_SIZE) {
         ensure(entry->size);
         if (entry->lsn > lsn) {
            break;
         }
         if (entry->lsn == lsn) {
            u64 version = wal_wt_cursor.load();
            u64 round = wal_buffer_round.load();
            std::unique_ptr<u8[]> entry_buffer = std::make_unique<u8[]>(entry->size);
            std::memcpy(entry_buffer.get(), entry, entry->size);
            if ((version > offset && round == wal_buffer_round.load()) ||
                (version < offset && wal_wt_cursor.load() < offset && round == wal_buffer_round.load())) {
               return entry_buffer;
            } else {
               failed = true;
               break;
            }
         }
         offset += entry->size;
         entry = reinterpret_cast<WALEntry*>(wal_buffer + offset);
      }
   }
outofmemory : {
   // 2- Read from SSD, accelerate using getLowerBound
   // TODO: optimize, do not read 10 Mob!
   const auto slot = wal_finder.getJumpPoint(lsn);
   std::unique_ptr<u8> log_chunk(reinterpret_cast<u8*>(std::aligned_alloc(512, WORKER_WAL_SIZE)));
   const u64 lower_bound = slot.offset;
   const u64 lower_bound_aligned = utils::downAlign(lower_bound);
   const s32 ret = pread(ssd_fd, log_chunk.get(), WORKER_WAL_SIZE, lower_bound_aligned);
   posix_check(ret > 0);
   // -------------------------------------------------------------------------------------
   u64 offset = 0;
   u8* ptr = log_chunk.get() + lower_bound - lower_bound_aligned;
   WALEntry* entry = reinterpret_cast<WALEntry*>(ptr + offset);
   WALEntry* prev_entry = entry;
   while (true) {
      ensure(entry->size > 0 && entry->lsn <= lsn);
      if (entry->lsn == lsn) {
         std::unique_ptr<u8[]> entry_buffer = std::make_unique<u8[]>(entry->size);
         std::memcpy(entry_buffer.get(), entry, entry->size);
         return entry_buffer;
      }
      if (offset + entry->size < slot.length) {
         offset += entry->size;
         prev_entry = entry;
         entry = reinterpret_cast<WALEntry*>(ptr + offset);
      } else {
         break;
      }
   }
   {
      DEBUG_BLOCK()
      {
         const auto slot2 = wal_finder.getJumpPoint(lsn);
         if (slot.offset == slot2.offset) {
            auto tmp = std::prev(wal_finder.ht.end())->first;
         }
         ensure(slot.offset != slot2.offset);
      }
      goto outofmemory;
   }
   return nullptr;
}
}
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
