#pragma once
#include "Exceptions.hpp"
#include "HistoryTreeInterface.hpp"
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
   static constexpr u64 MAX_WORKER_THREADS = std::numeric_limits<WORKERID>::max();
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
   HistoryTreeInterface& versions_space;
   // -------------------------------------------------------------------------------------
   CRManager(HistoryTreeInterface&, s32 ssd_fd, u64 end_of_block_device);
   ~CRManager();
   // -------------------------------------------------------------------------------------
   void registerMeAsSpecialWorker();
   void deleteSpecialWorker();
   // -------------------------------------------------------------------------------------
   /**
    * @brief Schedule same job on specific amount of workers.
    *
    * @param workers amount of workers
    * @param job Job to do. Same for each worker.
    */
   void scheduleJobs(u64 workers, std::function<void()> job);
   /**
    * @brief Schedule worker_id specific job on specific amount of workers.
    *
    * @param workers amount of workers
    * @param job Job to do. Different for each worker.
    */
   void scheduleJobs(u64 workers, std::function<void(u64 t_i)> job);
   /**
    * @brief Schedules one job asynchron on specific worker.
    *
    * @param t_i worker to compute job
    * @param job job
    */
   void scheduleJobAsync(u64 t_i, std::function<void()> job);
   /**
    * @brief Schedules one job on one specific worker and waits for completion.
    *
    * @param t_i worker to compute job
    * @param job job
    */
   void scheduleJobSync(u64 t_i, std::function<void()> job);
   /**
    * @brief Waits for all Workers to complete.
    *
    */
   void joinAll();
   // -------------------------------------------------------------------------------------
   // State Serialization
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);

  private:
   static std::atomic<u64> fsync_counter;
   static std::atomic<u64> g_ssd_offset;
   // -------------------------------------------------------------------------------------
   void groupCommiter();
   void groupCommitCordinator();
   void groupCommiter1();
   void groupCommiter2();
   // -------------------------------------------------------------------------------------
   /**
    * @brief Set the Job to specific worker.
    *
    * @param t_i specific worker
    * @param job job
    */
   void setJob(u64 t_i, std::function<void()> job);
   /**
    * @brief Wait for one worker to complete.
    *
    * @param t_i worker_id.
    * @param condition what is the completion condition?
    */
   void joinOne(u64 t_i, std::function<bool(WorkerThread&)> condition);
};
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
