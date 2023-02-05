// -------------------------------------------------------------------------------------
#include "Mean.hpp"
#include <libaio.h>
// -------------------------------------------------------------------------------------
#include "TaskManager.hpp"
#include "ThreadingManager.hpp"
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
// defaults set in hpp
#ifdef MEAN_USE_THREADING
using ExecEnv = ThreadingManager;
#endif
#ifdef MEAN_USE_TASKING
using ExecEnv = TaskManager;
#endif
// -------------------------------------------------------------------------------------
namespace env
{
ExecEnv _instance;
void init(int workerThreads, int exclusiveThreads, mean::IoOptions ioOptions, int threadAffinityOffset)
{
   _instance.init(workerThreads, exclusiveThreads, ioOptions, threadAffinityOffset);
}
// -------------------------------------------------------------------------------------
void start(TaskFunction fun)
{
   _instance.start(fun);
}
void shutdown()
{
   _instance.shutdown();
}
void join()
{
   _instance.join();
}
int workerCount() {
   return _instance.workerCount();
}
void adjustWorkerCount(int workerThreads) {
   _instance.adjustWorkerCount(workerThreads);
}
void registerPageProvider(void* bm_ptr, u64 partitions_count) {
   env::_instance.registerPageProvider(bm_ptr, partitions_count);
}
std::string printCountersHeader()
{
   return _instance.printCountersHeader();
}
std::string printCounters(int te_id)
{
   return _instance.printCounters(te_id);
}
/*
int fd() {
        return _instance.getFd();
}
*/
template <>
ExecEnv& implementation<ExecEnv>()
{
   return _instance;
}
}  // namespace env
namespace exec
{
/*
void* exec() {
        return env::_instance.localExec();
}
*/
int getId()
{
   return env::_instance.execId();
}
IoChannel& ioChannel()
{
   return env::_instance.execIoChannel();
}
}  // namespace exec
namespace task
{
void registerExclusiveThread(std::string name, int id, TaskFunction fun)
{
   env::_instance.registerExclusiveThread(name, id, fun);
}
void registerPoller(int to, TaskFunction poller)
{
   env::_instance.registerPoller(to, poller);
}
void parallelFor(BlockedRange bb, std::function<void(BlockedRange,std::atomic<bool>&)> fun, int tasks, s64 granularity)
{
   env::_instance.parallelFor(bb, fun, tasks, granularity);
}
void scheduleTaskSync(TaskFunction fun)
{
   env::_instance.scheduleTaskSync(fun);
}
void yield(TaskState ts)
{
   env::_instance.yield(ts);
}
void read(char* data, s64 addr, u64 len)
{
   env::_instance.blockingIo(IoRequestType::Read, data, addr, len);
}
void write(char* data, s64 addr, u64 len)
{
   env::_instance.blockingIo(IoRequestType::Write, data, addr, len);
}
Task& this_task() {
   return env::_instance.this_task();
}
}  // namespace task
}  // namespace mean
