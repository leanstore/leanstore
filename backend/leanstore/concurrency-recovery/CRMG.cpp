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
  removeAllThreads();
}
// -------------------------------------------------------------------------------------
void CRMG::removeAllThreads()
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
  if (all_threads.count(p) > 0) {
    all_threads.erase(p);
  }
  guard.unlock();
  delete p;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
