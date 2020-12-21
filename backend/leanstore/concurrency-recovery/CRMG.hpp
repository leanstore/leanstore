#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "Worker.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
/*
  Manages a fixed number of worker threads, each one gets a partition
 */
class CRManager
{
  public:
   static constexpr u64 MAX_WORKER_THREADS = 256;
   static CRManager* global;
   Worker* workers[MAX_WORKER_THREADS];
   // -------------------------------------------------------------------------------------
   std::atomic<u64> running_threads = 0;
   std::atomic<bool> keep_running = true;
   // -------------------------------------------------------------------------------------
   struct WorkerThread {
      std::mutex mutex;
      std::condition_variable cv;
      std::function<void()> job;
      bool wt_ready = true;   // Idle
      bool job_set = false;   // Has job
      bool job_done = false;  // Job done
   };
   std::vector<std::thread> worker_threads;
   WorkerThread worker_threads_meta[MAX_WORKER_THREADS];
   u32 workers_count;
   // -------------------------------------------------------------------------------------
   const s32 ssd_fd;
   const u64 end_of_block_device;
   // -------------------------------------------------------------------------------------
   CRManager(s32 ssd_fd, u64 end_of_block_device);
   ~CRManager();
   // -------------------------------------------------------------------------------------
   void groupCommiter();
   // -------------------------------------------------------------------------------------
   void scheduleJobs(u64 workers, std::function<void()> job);
   void scheduleJobs(u64 workers, std::function<void(u64 t_i)> job);
   void scheduleJobAsync(u64 t_i, std::function<void()> job);
   void scheduleJobSync(u64 t_i, std::function<void()> job);
   void joinAll();

  private:
   void setJob(u64 t_i, std::function<void()> job);
   void joinOne(u64 t_i, std::function<bool(WorkerThread&)> condition);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
