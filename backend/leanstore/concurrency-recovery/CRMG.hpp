#pragma once
#include "Worker.hpp"
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <mutex>
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
   Worker* workers[MAX_WORKER_THREADS];
   // -------------------------------------------------------------------------------------
   std::atomic<u64> running_threads = 0;
   std::atomic<bool> keep_running = true;
   // -------------------------------------------------------------------------------------
   struct WorkerThread {
      std::mutex mutex;
      std::condition_variable cv;
      std::function<void()> job;
      bool wt_ready = true;
      bool job_set = false;
      bool job_done = false;
   };
   std::vector<std::thread> worker_threads;
   WorkerThread worker_threads_meta[MAX_WORKER_THREADS];
   u64 wt_counter;
   // -------------------------------------------------------------------------------------
   CRManager();
   ~CRManager();
   // -------------------------------------------------------------------------------------
   void scheduleJobAsync(u64 t_i, std::function<void()> job);
   void scheduleJobSync(u64 t_i, std::function<void()> job);
   void joinAll();
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
