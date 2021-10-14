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
// -------------------------------------------------------------------------------------
CRManager::CRManager(VersionsSpaceInterface& versions_space, s32 ssd_fd, u64 end_of_block_device)
    : ssd_fd(ssd_fd), end_of_block_device(end_of_block_device), versions_space(versions_space)
{
   workers_count = FLAGS_worker_threads;
   ensure(workers_count < MAX_WORKER_THREADS);
   // -------------------------------------------------------------------------------------
   Worker::global_workers_in_progress_txid = std::make_unique<atomic<u64>[]>(workers_count);
   Worker::global_workers_rv_start = std::make_unique<atomic<u64>[]>(workers_count);
   Worker::global_workers_oltp_lwm = std::make_unique<atomic<u64>[]>(workers_count);
   Worker::global_workers_olap_lwm = std::make_unique<atomic<u64>[]>(workers_count);
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
         WorkerCounters::myCounters().worker_id = t_i;
         CPUCounters::registerThread(thread_name, false);
         // -------------------------------------------------------------------------------------
         workers[t_i] = new Worker(t_i, workers, workers_count, versions_space, ssd_fd);
         Worker::tls_ptr = workers[t_i];
         // -------------------------------------------------------------------------------------
         running_threads++;
         while (running_threads != (FLAGS_worker_threads + FLAGS_wal))
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
      std::thread group_commiter([&]() {
         if (FLAGS_pin_threads) {
            utils::pinThisThread(workers_count);
         }
         groupCommiter();
      });
      group_commiter.detach();
   }
}
// -------------------------------------------------------------------------------------
void CRManager::registerMeAsSpecialWorker()
{
   cr::Worker::tls_ptr = new Worker(std::numeric_limits<WORKERID>::max(), workers, workers_count, versions_space, ssd_fd, true);
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
