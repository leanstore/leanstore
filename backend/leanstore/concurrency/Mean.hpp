#pragma once
// -------------------------------------------------------------------------------------
#include "BlockedRange.hpp"
#include "Task.hpp"
#include "YieldLock.hpp"
#include "leanstore/io/IoInterface.hpp"
// -------------------------------------------------------------------------------------
#include <functional>
#include <string>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
#if !defined(MEAN_USE_TASKING) && !defined(MEAN_USE_THREADING)
#define MEAN_USE_TASKING
#endif

#ifdef MEAN_USE_THREADING
using mutex = std::mutex;
#endif
#ifdef MEAN_USE_TASKING
using mutex = YieldLock;
#endif

using TaskFunction = std::function<void()>;  // std::add_pointer_t<void()>;
// -------------------------------------------------------------------------------------
namespace env
{
void init(int workerThreads, int exclusiveThreads, IoOptions ioOptions, int threadAffinityOffset = 0);
// ExecEnv& instance();
void start(TaskFunction fun);
void shutdown();
void join();
void sleepAll(float sleep);
int workerCount();
void adjustWorkerCount(int workerThreads);
void registerPageProvider(void* bm_ptr, u64 partitions_count);
std::string printCountersHeader();
std::string printCounters(int te_id);
template <typename impl>
impl& implementation();  // for internal use only
}  // namespace env
// -------------------------------------------------------------------------------------
namespace exec
{
// void* exec();
IoChannel& ioChannel();
int getId();
}  // namespace exec
// -------------------------------------------------------------------------------------
namespace task
{
void registerExclusiveThread(std::string name, int id, TaskFunction fun);
void registerPoller(int to, TaskFunction poller);
void parallelFor(BlockedRange bb, std::function<void(BlockedRange, std::atomic<bool>&)> fun, int tasks, s64 granularity = -1);
void scheduleTaskSync(TaskFunction fun);
// -------------------------------------------------------------------------------------
void yield(TaskState ts = TaskState::Ready);
void read(char* data, s64 addr, u64 len);
void write(char* data, s64 addr, u64 len);
Task& this_task();
}  // namespace task
// -------------------------------------------------------------------------------------
}  // namespace mean
