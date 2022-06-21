#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
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
void CRManager::groupCommitCordinator()
{
   std::thread log_cordinator([&]() {
      running_threads++;
      std::string thread_name("log_cordinator");
      pthread_setname_np(pthread_self(), thread_name.c_str());
      CPUCounters::registerThread(thread_name, false);
      // -------------------------------------------------------------------------------------
      LID min_all_workers_gsn;  // For Remote Flush Avoidance
      LID max_all_workers_gsn;  // Sync all workers to this point
      TXID min_all_workers_hardened_commit_ts;
      while (keep_running) {
         min_all_workers_gsn = std::numeric_limits<LID>::max();
         max_all_workers_gsn = 0;
         min_all_workers_hardened_commit_ts = std::numeric_limits<TXID>::max();
         // -------------------------------------------------------------------------------------
         if (FLAGS_wal_fsync) {
            fdatasync(ssd_fd);
         }
         fsync_counter++;
         fsync_counter.notify_all();
         // -------------------------------------------------------------------------------------
         for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
            Worker& worker = *workers[w_i];
            min_all_workers_gsn = std::min<LID>(min_all_workers_gsn, worker.logging.hardened_gsn);
            max_all_workers_gsn = std::max<LID>(max_all_workers_gsn, worker.logging.hardened_gsn);
            min_all_workers_hardened_commit_ts = std::min<TXID>(min_all_workers_hardened_commit_ts, worker.logging.hardened_commit_ts);
         }
         // -------------------------------------------------------------------------------------
         assert(Worker::Logging::global_min_gsn_flushed.load() <= min_all_workers_gsn);
         Worker::Logging::global_min_commit_ts_flushed.store(min_all_workers_hardened_commit_ts, std::memory_order_release);
         Worker::Logging::global_min_gsn_flushed.store(min_all_workers_gsn, std::memory_order_release);
         Worker::Logging::global_sync_to_this_gsn.store(max_all_workers_gsn, std::memory_order_release);
         // -------------------------------------------------------------------------------------
         CRCounters::myCounters().gct_rounds += 1;
      }
      running_threads--;
   });
   log_cordinator.detach();
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
