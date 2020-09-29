#include "CRMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local CRMG::TLSHandler CRMG::tls_handler;
std::mutex CRMG::mutex;
std::set<Partition*> CRMG::all_threads;
u64 CRMG::partitions_counter = 0;
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
Partition* CRMG::registerThread()
{
  Partition* p = new Partition(partitions_counter++);
  std::unique_lock guard(mutex);
  all_threads.insert(p);
  return p;
}
// -------------------------------------------------------------------------------------
void CRMG::removeThread(Partition* p)
{
  std::unique_lock guard(mutex);
  all_threads.erase(p);
  delete p;
}
// -------------------------------------------------------------------------------------
void Partition::startTX(Transaction::TYPE tx_type)
{
  WALEntry& entry = wal.reserve(0);
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
}
// -------------------------------------------------------------------------------------
void Partition::commitTX()
{
  assert(tx().state == Transaction::STATE::STARTED);
  assert(tx().current_gsn > 0);
  WALEntry& entry = wal.reserve(0);
  entry.size = sizeof(WALEntry) + 0;
  entry.type = WALEntry::TYPE::TX_COMMIT;
  entry.lsn = wal.lsn_counter++;
  entry.gsn = tx().current_gsn++;
  wal.submit(0);
  // -------------------------------------------------------------------------------------
  if (active_tx == &system_tx) {
    active_tx = &user_tx;
  }
  // -------------------------------------------------------------------------------------
  // TODO:
  // -------------------------------------------------------------------------------------
  tx().state = Transaction::STATE::COMMITED;
}
// -------------------------------------------------------------------------------------
void Partition::abortTX()
{
  assert(false);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
