#include "CRMG.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
thread_local CRManager::TLSHandler CRManager::tls_handler;
std::mutex CRManager::mutex;
std::set<Partition*> CRManager::all_threads;
u64 CRManager::partitions_counter = 0;
// -------------------------------------------------------------------------------------
CRManager::CRManager()
{
   // std::thread group_commiter([&]() {
   //    while (true) {
   //       LID min_written_gsn = std::numeric_limits<LID>::max();
   //       {
   //          std::unique_lock guard(mutex);
   //          for (auto& t : all_threads) {
   //             min_written_gsn = std::min<LID>(min_written_gsn, t->wal.max_written_gsn);
   //          }
   //       }
   //       sleep(1);
   //    }
   // });
   // group_commiter.detach();
}
CRManager::~CRManager()
{
   std::unique_lock guard(mutex);
   for (auto& t : all_threads) {
      delete t;
   }
   all_threads.clear();
}
// -------------------------------------------------------------------------------------
Partition* CRManager::registerThread()
{
   u64 partition_id = partitions_counter++;
   Partition* p = new Partition(partition_id);
   {
      std::string thread_name("worker_" + std::to_string(partition_id));
      pthread_setname_np(pthread_self(), thread_name.c_str());
   }
   std::unique_lock guard(mutex);
   all_threads.insert(p);
   return p;
}
// -------------------------------------------------------------------------------------
void CRManager::removeThread(Partition* p)
{
   std::unique_lock guard(mutex);
   all_threads.erase(p);
   delete p;
   guard.unlock();
}
}  // namespace cr
}  // namespace leanstore
