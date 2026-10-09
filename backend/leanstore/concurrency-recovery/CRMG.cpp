#include "CRMG.hpp"

#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// Threads id order: workers (xN) -> Group Committer Thread (x1) -> Page Provider Threads (xP)
CRManager* CRManager::global = nullptr;
std::atomic<u64> CRManager::fsync_counter = 0;
std::atomic<u64> CRManager::g_ssd_offset = 0;
// -------------------------------------------------------------------------------------
CRManager::CRManager(HistoryTreeInterface& versions_space, s32 ssd_fd, u64 end_of_block_device)
    : ssd_fd(ssd_fd), end_of_block_device(end_of_block_device), versions_space(versions_space)
{
   workers_count = FLAGS_worker_threads;
   g_ssd_offset = end_of_block_device;
   ensure(workers_count < MAX_WORKER_THREADS);
   // -------------------------------------------------------------------------------------
   Worker::global_workers_current_snapshot = std::make_unique<atomic<u64>[]>(workers_count);
   // -------------------------------------------------------------------------------------
   worker_threads.reserve(workers_count);
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      worker_threads.emplace_back([&, t_i]() {
         std::string thread_name("worker_" + std::to_string(t_i));
         pthread_setname_np(pthread_self(), thread_name.c_str());
         if (FLAGS_pin_threads) {
            utils::pinThisThread(t_i);
         }
         // -------------------------------------------------------------------------------------
         if (FLAGS_cpu_counters) {
            CPUCounters::registerThread(thread_name, false);
         }
         WorkerCounters::myCounters().worker_id = t_i;
         CRCounters::myCounters().worker_id = t_i;
         // -------------------------------------------------------------------------------------
         workers[t_i] = new Worker(t_i, workers, workers_count, versions_space, ssd_fd);
         Worker::tls_ptr = workers[t_i];
         // -------------------------------------------------------------------------------------
         running_threads++;
         while (running_threads != (workers_count + FLAGS_wal))
            ;
         auto& meta = worker_threads_meta[t_i];
         while (keep_running) {
            std::unique_lock guard(meta.mutex);
            meta.cv.wait(guard, [&]() { return keep_running == false || meta.job_set; });
            if (!keep_running) {
               break;
            }
            meta.wt_ready = false;
            meta.job();
            meta.wt_ready = true;
            meta.job_done = true;
            meta.job_set = false;
            meta.cv.notify_one();
         }
         running_threads--;
      });
   }
   for (auto& t : worker_threads) {
      t.detach();
   }
   // -------------------------------------------------------------------------------------
   // Wait until all worker threads are initialized
   while (running_threads < workers_count) {
   }
   // -------------------------------------------------------------------------------------
   if (FLAGS_wal) {
      if (FLAGS_wal_variant == 0) {
         std::thread group_commiter([&]() {
            if (FLAGS_pin_threads) {
               utils::pinThisThread(workers_count);
            }
            groupCommiter();
         });
         group_commiter.detach();
      } else {
         groupCommitCordinator();
         if (FLAGS_wal_variant == 1) {
            groupCommiter1();
         } else if (FLAGS_wal_variant == 2) {
            groupCommiter2();
         }
      }
   }
}
// -------------------------------------------------------------------------------------
void CRManager::registerMeAsSpecialWorker()
{
   cr::Worker::tls_ptr = new Worker(std::numeric_limits<WORKERID>::max(), workers, workers_count, versions_space, ssd_fd, true);
}

void CRManager::deleteSpecialWorker()
{
   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobSync(u64 t_i, std::function<void()> job)
{
   setJob(t_i, job);
   joinOne(t_i, [&](WorkerThread& meta) { return meta.job_done; });
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobAsync(u64 t_i, std::function<void()> job)
{
   setJob(t_i, job);
}
// -------------------------------------------------------------------------------------
void CRManager::scheduleJobs(u64 workers, std::function<void()> job)
{
   for (u32 t_i = 0; t_i < workers; t_i++) {
      setJob(t_i, job);
   }
}
void CRManager::scheduleJobs(u64 workers, std::function<void(u64 t_i)> job)
{
   for (u32 t_i = 0; t_i < workers; t_i++) {
      setJob(t_i, [=]() { return job(t_i); });
   }
}

// -------------------------------------------------------------------------------------
void CRManager::joinAll()
{
   for (u32 t_i = 0; t_i < workers_count; t_i++) {
      joinOne(t_i, [&](WorkerThread& meta) { return meta.wt_ready && !meta.job_set; });
   }
}
// -------------------------------------------------------------------------------------
void CRManager::setJob(u64 t_i, std::function<void()> job)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
   meta.job_set = true;
   meta.job_done = false;
   meta.job = job;
   guard.unlock();
   meta.cv.notify_one();
}
// -------------------------------------------------------------------------------------
void CRManager::joinOne(u64 t_i, std::function<bool(WorkerThread&)> condition)
{
   ensure(t_i < workers_count);
   auto& meta = worker_threads_meta[t_i];
   std::unique_lock guard(meta.mutex);
   meta.cv.wait(guard, [&]() { return condition(meta); });
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> CRManager::serialize()
{
   std::unordered_map<std::string, std::string> map;
   map["global_logical_clock"] = std::to_string(Worker::ConcurrencyControl::global_clock.load());
   return map;
}
// -------------------------------------------------------------------------------------
void CRManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   Worker::ConcurrencyControl::global_clock = std::stol(map["global_logical_clock"]);
   Worker::global_all_lwm = std::stol(map["global_logical_clock"]);
}
// -------------------------------------------------------------------------------------
CRManager::~CRManager()
{
   keep_running = false;
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      worker_threads_meta[t_i].cv.notify_one();
   }
   while (running_threads) {
   }
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      delete workers[t_i];
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
