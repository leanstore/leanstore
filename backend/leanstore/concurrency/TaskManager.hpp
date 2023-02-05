#pragma once
// -------------------------------------------------------------------------------------
#include "BlockedRange.hpp"
#include "TaskExecutor.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/io/IoInterface.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <iostream>
#include <memory>
#include <vector>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
class TaskManager
{
   std::atomic<int> runningExecs = 0;
   std::vector<std::unique_ptr<TaskExecutor>> execs;
   std::atomic<int> exclusiveThreadCounter = 0;
   std::unordered_map<int, std::reference_wrapper<TaskExecutor>> exclusiveThreadsMap;
   std::vector<std::unique_ptr<IoChannel>> remoteChannels;
   std::unique_ptr<MessageHandlerManager> messageManager = nullptr;
   int exclusiveThreads;
   int threadAffinityOffset;
   static constexpr u64 MAX_WORKER_THREADS = 256;
   s64 partitions_count = -1;
   BufferManager* buffer_manager = nullptr;
  public:
   leanstore::cr::Worker* workers[MAX_WORKER_THREADS]; // TODO
   // -------------------------------------------------------------------------------------
   ~TaskManager();
   // -------------------------------------------------------------------------------------
   // env
   // -------------------------------------------------------------------------------------
   void init(int workerThreads, int exclsuiveThreads, IoOptions ioOptions, int threadAffinityOffset = 0);
   void start(TaskFunction taskFun);
   void shutdown();
   void join();
   std::string printCountersHeader();
   std::string printCounters(int te_id);
   int workerCount();
   void adjustWorkerCount(int workerThreads);
   void registerPageProvider(void* bf_ptr, int partitions_count);
   void reflowPageProviderPartitions();
   // -------------------------------------------------------------------------------------
   // exec
   // -------------------------------------------------------------------------------------
   TaskExecutor* localExec();
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
   // -------------------------------------------------------------------------------------
   TaskExecutor& getExec(int id);
   int size() const;
  private:
   void sendTask(int to, TaskFunction taskFun);
   IoChannelCounterAggregator printAggregateExecs(std::ostream& ss, int fromExcecId, int toExecId, bool printDetailed);
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
