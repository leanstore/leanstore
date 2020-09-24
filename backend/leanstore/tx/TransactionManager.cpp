#include "TransactionManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace tx
{
// -------------------------------------------------------------------------------------
s64 thread_local TXMG::thread_id = -1;
std::mutex TXMG::mutex;
// -------------------------------------------------------------------------------------
TXMG::TXMG()
{
  per_thread = make_unique<ThreadData[]>(FLAGS_worker_threads);
}
// -------------------------------------------------------------------------------------
void TXMG::registerThread()
{
  std::unique_lock guard(mutex);
  ensure(threads_counter < FLAGS_worker_threads);
  thread_id = threads_counter++;
}

// -------------------------------------------------------------------------------------
void TXMG::startTX()
{
  my().tx.state = Transaction::STATE::STARTED;
}
// -------------------------------------------------------------------------------------
u8* TXMG::reserveEntry(u64 requested_size) {

}
// -------------------------------------------------------------------------------------
void TXMG::commitTX()
{
  my().tx.state = Transaction::STATE::COMMITED;
}
// -------------------------------------------------------------------------------------
void TXMG::abortTX()
{
  my().tx.state = Transaction::STATE::ABORTED;
}
// -------------------------------------------------------------------------------------
}  // namespace tx
}  // namespace leanstore
