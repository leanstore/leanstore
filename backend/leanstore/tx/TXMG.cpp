#include "TXMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace txmg
{
// -------------------------------------------------------------------------------------
thread_local ThreadData* TXMG::thread_data = nullptr;
std::mutex TXMG::mutex;
std::set<ThreadData*> TXMG::all_threads;
// -------------------------------------------------------------------------------------
TXMG::TXMG() {}
TXMG::~TXMG()
{
  std::unique_lock guard(mutex);
  for (auto& t : all_threads) {
    delete t;
  }
  all_threads.clear();
}
// -------------------------------------------------------------------------------------
void TXMG::registerThread()
{
  std::unique_lock guard(mutex);
  assert(thread_data == nullptr);
  thread_data = new ThreadData();
  all_threads.insert(thread_data);
}
// -------------------------------------------------------------------------------------
void TXMG::removeThread()
{
  std::unique_lock guard(mutex);
  all_threads.erase(thread_data);
  delete thread_data;
}
// -------------------------------------------------------------------------------------
void TXMG::startTX()
{
  WALPartition& wal = my().wal;
  Transaction& tx = my().tx;
  // -------------------------------------------------------------------------------------
  tx.state = Transaction::STATE::STARTED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_START;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
u8* TXMG::reserveEntry(DTID dt_id, u64 requested_size)
{
  WALPartition& wal = my().wal;
  Transaction& tx = my().tx;
  // -------------------------------------------------------------------------------------
  if (tx.current_dt_id != dt_id) {
    tx.current_dt_id = dt_id;
    WALEntry& entry = wal.current_chunk->entry(sizeof(u64));
    entry.type = WALEntry::Type::DT_CHANGE;
    entry.lsn = wal.lsn_counter++;
    *reinterpret_cast<u64*>(entry.payload) = dt_id;
  }
  // -------------------------------------------------------------------------------------
  WALEntry& entry = wal.current_chunk->entry(requested_size);
  return entry.payload;
}
// -------------------------------------------------------------------------------------
void TXMG::commitTX()
{
  WALPartition& wal = my().wal;
  Transaction& tx = my().tx;
  // -------------------------------------------------------------------------------------
  tx.state = Transaction::STATE::COMMITED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_COMMIT;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
void TXMG::abortTX()
{
  WALPartition& wal = my().wal;
  Transaction& tx = my().tx;
  // -------------------------------------------------------------------------------------
  tx.state = Transaction::STATE::ABORTED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_ABORT;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
}  // namespace txmg
}  // namespace leanstore
