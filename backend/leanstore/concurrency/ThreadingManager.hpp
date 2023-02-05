#pragma once
// -------------------------------------------------------------------------------------
#include "BlockedRange.hpp"
#include "MessageHandler.hpp"
#include "Task.hpp"
#include "ThreadBase.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
// -------------------------------------------------------------------------------------
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
class ThreadWithJump : public Thread
{
   jumpmu::JumpMUContext jump_context;
  public:
   // -------------------------------------------------------------------------------------
   struct meta {
      std::mutex mutex;
      std::condition_variable cv;
      TaskFunction task;
      bool wt_ready = true;
      bool job_set = false;
      bool job_done = false;
   } meta;
   leanstore::cr::Worker* this_worker;
   ThreadWithJump(std::function<void()> fun, std::string name = "Thread", int id = -1) : Thread(fun, name, id) {}
   int process() override
   {
      jumpmu::thread_local_jumpmu_ctx = &jump_context;
      fun();
      jumpmu::thread_local_jumpmu_ctx = nullptr;
      return 0;
   }
   void sendTask(TaskFunction taskFun) {
      std::unique_lock guard(meta.mutex);
      meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
      meta.job_set = true;
      meta.job_done = false;
      meta.task = taskFun;
      guard.unlock();
      meta.cv.notify_one();
      //guard.lock();
      //meta.cv.wait(guard, [&]() { return meta.job_done; });
   }
   void sendTaskBlocking(TaskFunction taskFun) {
      sendTask(taskFun);
      std::unique_lock guard(meta.mutex);
      meta.cv.wait(guard, [&]() { return meta.job_done; });
   }
   void shutdown() {
      stop();
      meta.cv.notify_one();
   }
};
// -------------------------------------------------------------------------------------
class ThreadingManager
{
   int total_threads_count;
   std::atomic<int> running_threads;
   std::vector<std::unique_ptr<ThreadWithJump>> all_threads;
   int max_exclusive_threads;
   std::atomic<int> exclusiveThreadCounter = 0;
   std::unordered_map<int, std::reference_wrapper<ThreadWithJump>> exclusiveThreads;
   static constexpr int MAX_WORKER_THREADS = 2048;
  public:
   leanstore::cr::Worker* workers[MAX_WORKER_THREADS];
   // -------------------------------------------------------------------------------------
   ~ThreadingManager();
   // -------------------------------------------------------------------------------------
   // env
   // -------------------------------------------------------------------------------------
   void init(int workerThreads, [[maybe_unused]] int exclusvieThreads, IoOptions ioOptions, [[maybe_unused]] int threadAffinityOffset = 0);
   void start(TaskFunction taskFun);
   void shutdown();
   void join();
   std::string stats();
   void adjustWorkerCount(int workerThreads);
   void registerPageProvider(void* bf_ptr, int partitions_count);
   std::string printCountersHeader();
   std::string printCounters(int te_id);
   // -------------------------------------------------------------------------------------
   // exec
   // -------------------------------------------------------------------------------------
   int execId();
   IoChannel& execIoChannel();
   // -------------------------------------------------------------------------------------
   // task
   // -------------------------------------------------------------------------------------
   void registerExclusiveThread(std::string name, int t_i, TaskFunction fun);
   void registerPoller(int to, TaskFunction poller);
   void parallelFor(BlockedRange range, std::function<void(BlockedRange, std::atomic<bool>& cancelable)> fun, int tasks, s64 bbgranularity = -1);
   void scheduleTaskSync(TaskFunction fun);
   void yield(TaskState ts);
   void blockingIo(IoRequestType type, char* data, s64 addr, u64 len);
   Task& this_task();
   // -------------------------------------------------------------------------------------
   // int getFd();
   // -------------------------------------------------------------------------------------
   int workerCount();
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
