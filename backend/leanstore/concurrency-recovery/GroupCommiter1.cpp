#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std::chrono_literals;
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
void CRManager::groupCommiter1()
{
   std::thread group_cordinator([&]() {
      running_threads++;
      std::string thread_name("commit_cordinator");
      pthread_setname_np(pthread_self(), thread_name.c_str());
      CPUCounters::registerThread(thread_name, false);
      // -------------------------------------------------------------------------------------
      while (keep_running) {
         fdatasync(ssd_fd);
         fsync_counter++;
      }
      running_threads--;
   });
   group_cordinator.detach();
   // -------------------------------------------------------------------------------------
   std::thread group_writer([&]() {
      running_threads++;
      std::string thread_name("wal_writer");
      pthread_setname_np(pthread_self(), thread_name.c_str());
      CPUCounters::registerThread(thread_name, false);
      // -------------------------------------------------------------------------------------
      while (keep_running) {
      }
      running_threads--;
   });
   group_writer.detach();
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
