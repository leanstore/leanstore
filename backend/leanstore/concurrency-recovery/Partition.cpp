#include "Partition.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
Partition::Partition(u64 partition_id) : partition_id(partition_id), wal(partition_id) {}
Partition::~Partition() {}
// -------------------------------------------------------------------------------------
void Partition::startTX(Transaction::TYPE tx_type)
{
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
void Partition::commitTX()
{
  assert(tx().state == Transaction::STATE::STARTED);
  assert(current_gsn > 0);
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
void Partition::abortTX()
{
  assert(false);
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
