#include "CRMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local Partition* CRMG::user_partition = nullptr;
thread_local Partition* CRMG::system_partition = nullptr;
std::mutex CRMG::mutex;
std::set<Partition*> CRMG::all_threads;
// -------------------------------------------------------------------------------------
CRMG::CRMG() {}
CRMG::~CRMG()
{
  std::unique_lock guard(mutex);
  for (auto& t : all_threads) {
    delete t;
  }
  all_threads.clear();
}
// -------------------------------------------------------------------------------------
void CRMG::registerThread()
{
  std::unique_lock guard(mutex);
  assert(thread_data == nullptr);
  system_partition = new Partition(Partition::TYPE::SYSTEM);
  user_partition = new Partition(Partition::TYPE::USER);
  all_threads.insert(user_partition);
  all_threads.insert(system_partition);
}
// -------------------------------------------------------------------------------------
void CRMG::removeThread()
{
  std::unique_lock guard(mutex);
  all_threads.erase(user_partition);
  all_threads.erase(system_partition);
  delete user_partition;
  delete system_partition;
}
// -------------------------------------------------------------------------------------
void Partition::startTX()
{
  tx.state = Transaction::STATE::STARTED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_START;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
u8* Partition::reserveEntry(PID pid, DTID dt_id, LID gsn, u64 requested_size)
{

  WALEntry& entry = wal.current_chunk->entry(requested_size);
  return entry.payload;
}
// -------------------------------------------------------------------------------------
void Partition::commitTX()
{
  tx.state = Transaction::STATE::COMMITED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_COMMIT;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
void Partition::abortTX()
{
  tx.state = Transaction::STATE::ABORTED;
  WALEntry& entry = wal.current_chunk->entry(0);
  entry.type = WALEntry::Type::TX_ABORT;
  entry.lsn = wal.lsn_counter++;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
