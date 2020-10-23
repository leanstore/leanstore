#include "Worker.hpp"

#include "leanstore/profiling/counters/CRCounters.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Worker::transaction_timestamps = 0;
thread_local Worker* Worker::tls_ptr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(u64 worker_id, Worker** all_workers) : worker_id(worker_id), all_workers(all_workers), wal(worker_id)
{
   Worker::tls_ptr = this;
   CRCounters::myCounters().worker_id = worker_id;
}
Worker::~Worker() {}
// -------------------------------------------------------------------------------------
void Worker::startTX(Transaction::TYPE tx_type)
{
   const u64 TTS = transaction_timestamps.fetch_add(256) + worker_id;  // Transaction TimeStamp
   // -------------------------------------------------------------------------------------
   WALEntry& entry = *reinterpret_cast<WALEntry*>(wal.reserve(sizeof(WALEntry)));
   entry.size = sizeof(WALEntry);
   if (tx_type == Transaction::TYPE::USER) {
      entry.type = WALEntry::TYPE::USER_TX_START;
      active_tx = &user_tx;
   } else {
      entry.type = WALEntry::TYPE::SYSTEM_TX_START;
      active_tx = &system_tx;
   }
   entry.lsn = wal.lsn_counter++;
   wal.submit(0);
   assert(tx().state != Transaction::STATE::STARTED);
   tx().state = Transaction::STATE::STARTED;
   tx().start_gsn = current_gsn;
}
// -------------------------------------------------------------------------------------
void Worker::commitTX()
{
   assert(tx().state == Transaction::STATE::STARTED);
   WALEntry& entry = *reinterpret_cast<WALEntry*>(wal.reserve(sizeof(WALEntry)));
   entry.size = sizeof(WALEntry) + 0;
   entry.type = WALEntry::TYPE::TX_COMMIT;
   entry.lsn = wal.lsn_counter++;
   entry.gsn = current_gsn;
   wal.submit(0);
   // -------------------------------------------------------------------------------------
   // TODO:
   // -------------------------------------------------------------------------------------
   tx().state = Transaction::STATE::COMMITED;
   if (active_tx == &system_tx) {
      active_tx = &user_tx;
   }
}
// -------------------------------------------------------------------------------------
void Worker::abortTX()
{
   assert(false);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
