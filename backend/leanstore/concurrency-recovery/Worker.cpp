#include "Worker.hpp"

#include "leanstore/profiling/counters/CRCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Worker::central_tts = 0;
thread_local Worker* Worker::tls_ptr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers, u64 workers_count) : worker_id(worker_id), all_workers(all_workers), workers_count(workers_count)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
   active_tts = worker_id;
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
// -------------------------------------------------------------------------------------
WALEntry& Worker::reserveWALEntry()
{
   walEnsureEnoughSpace(sizeof(WALEntry));
   return *reinterpret_cast<WALEntry*>(wal_buffer + wal_wt_cursor);
}
// -------------------------------------------------------------------------------------
void Worker::submitWALEntry()
{
   wal_wt_cursor += sizeof(WALEntry);
}
// -------------------------------------------------------------------------------------
Worker::WALEntryHandler<void> Worker::reserveDTEntry(PID pid, DTID dt_id, LID gsn, u64 requested_size)
{
   const u64 total_size = sizeof(WALEntry) + requested_size;
   ensure(walContiguousFreeSpace() >= total_size);
   active_entry = reinterpret_cast<WALEntry*>(wal_buffer + wal_wt_cursor);
   active_entry->size = sizeof(WALEntry) + requested_size;
   active_entry->lsn = wal_lsn_counter++;
   active_entry->dt_id = dt_id;
   active_entry->pid = pid;
   active_entry->gsn = gsn;
   active_entry->type = WALEntry::TYPE::DT_SPECIFIC;
   return {active_entry->payload, total_size};
}
// -------------------------------------------------------------------------------------
void Worker::submitDTEntry(u64 requested_size)
{
   ensure(clock_gsn == active_entry->gsn);
   std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
   wal_wt_cursor += requested_size + sizeof(WALEntry);
   wal_max_gsn = clock_gsn;
}
// -------------------------------------------------------------------------------------
void Worker::startTX()
{
   WALEntry& entry = reserveWALEntry();
   entry.size = sizeof(WALEntry) + 0;
   entry.lsn = wal_lsn_counter++;
   submitWALEntry();
   assert(tx.state != Transaction::STATE::STARTED);
   tx.state = Transaction::STATE::STARTED;
   tx.min_gsn = clock_gsn;
   tx.tx_id = active_tts;
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   assert(tx.state == Transaction::STATE::STARTED);
   // -------------------------------------------------------------------------------------
   // TODO: MVCC
   // -------------------------------------------------------------------------------------
   WALEntry& entry = reserveWALEntry();
   entry.size = sizeof(WALEntry) + 0;
   entry.type = WALEntry::TYPE::TX_COMMIT;
   entry.lsn = wal_lsn_counter++;
   entry.gsn = clock_gsn;
   submitWALEntry();
   // -------------------------------------------------------------------------------------
   tx.max_gsn = clock_gsn;
   tx.state = Transaction::STATE::READY_TO_COMMIT;
   {
      std::unique_lock<std::mutex> g(worker_group_commiter_mutex);
      ready_to_commit_queue.push_back(tx);
   }
   active_tts = central_tts.fetch_add(workers_count) + worker_id + workers_count;  // Transaction TimeStamp
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   assert(false);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
