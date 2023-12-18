// -------------------------------------------------------------------------------------
#include "TaskManager.hpp"
#include "ConnectedIoChannel.hpp"
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/concurrency/Task.hpp"
#include "leanstore/io/IoInterface.hpp"
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
TaskManager::~TaskManager()
{
   shutdown();
}
// -------------------------------------------------------------------------------------
// env
// -------------------------------------------------------------------------------------
void TaskManager::init(int workerThreads, int exclusiveThreads, IoOptions ioOptions, int threadAffinityOffset)
{
   if (ioOptions.engine == "auto") {
      if (ioOptions.path.find("traddr") != std::string::npos) {
         ioOptions.engine = "spdk";
      } else {
         ioOptions.engine = "libaio";
      }
   }
   this->exclusiveThreads = exclusiveThreads;
   this->threadAffinityOffset = threadAffinityOffset;
   // -------------------------------------------------------------------------------------
   // all exclusive threds get an exclusibe channel
   // worker threads must share with a message handler
   messageManager = std::make_unique<MessageHandlerManager>(workerThreads + exclusiveThreads);
   IoInterface::initInstance(ioOptions);
   // exclusive
   for (int i = 0; i < exclusiveThreads; i++) {
      execs.push_back(std::make_unique<TaskExecutor>(messageManager->getMessageHandler(i), IoInterface::instance().getIoChannel(i), i));
      execs.back()->setCpuAffinityBeforeStart(i + threadAffinityOffset);
      workers[i] = new leanstore::cr::Worker(i, workers, workerThreads + exclusiveThreads);
      execs.back()->this_worker = workers[i];
   }
   // workers
   adjustWorkerCount(workerThreads);
}
int TaskManager::workerCount() {
   return runningExecs;
}
// -------------------------------------------------------------------------------------
void TaskManager::adjustWorkerCount(int workerThreads) {
   if (runningExecs > workerThreads) {
      // shut them down
      for (int i = workerThreads; i < runningExecs; i++) {
            execs[i]->stop();
            execs[i]->join();
      }
      execs.resize(workerThreads);
   } else {
      int ioChannels = IoInterface::instance().channelCount();
      // ioChannels = 1;
      assert(ioChannels > exclusiveThreads);
      for (int i = runningExecs; i < workerThreads; i++) {
         int id = i + exclusiveThreads;
         ensure(id < ioChannels);
         // physical channel
         execs.push_back(std::make_unique<TaskExecutor>(messageManager->getMessageHandler(id), IoInterface::instance().getIoChannel(id), id));
         execs.back()->setCpuAffinityBeforeStart(id + threadAffinityOffset);
         workers[id] = new leanstore::cr::Worker(id, workers, workerThreads + exclusiveThreads);
         execs.back()->this_worker = workers[id];
      }
   }
   runningExecs = workerThreads;
   reflowPageProviderPartitions();
}
void TaskManager::reflowPageProviderPartitions() {
   if (partitions_count <= 0) {
      return;
   }
   //ensure(runningExecs == (int)buffer_manager->cooling_partitions_count);
   std::cout << "reflowPageProviderPartitions() p_cnt: " << partitions_count << " running: " << runningExecs << " pp gettid: " << gettid() << std::endl;
   for (int t_i = 0; t_i < runningExecs; t_i++) {
      if (!FLAGS_nopp) {
         exclusiveThreads = buffer_manager->cooling_partitions_count; 
      }
      if (t_i < (int)buffer_manager->cooling_partitions_count){
         sendTask(t_i, [=] { TaskExecutor::localExec().registerPageProvider(buffer_manager, t_i); });
      }
   }
}
// -------------------------------------------------------------------------------------
void TaskManager::start(TaskFunction taskFun)
{
   for (auto& exe : execs) {
      exe->start();
   }
   for (auto& exe : execs) {
      while (!exe->ready()) {
      }
   }
   auto task = new Task(taskFun);
   messageManager->dbgSendMessage(
       exclusiveThreads, exclusiveThreads,
       [](void*, uintptr_t task) {
          auto t = reinterpret_cast<Task*>(task);
          // ensure(TaskManager::instance().exclusiveThreads.find(this_task::exec().id()) == TaskManager::instance().exclusiveThreads.end());
          TaskExecutor::localExec().pushTask(t);
       },
       reinterpret_cast<uint64_t>(task));
}
// -------------------------------------------------------------------------------------
void TaskManager::shutdown()
{
   for (auto& exe : execs) {
      exe->stop();
   }
}
// -------------------------------------------------------------------------------------
void TaskManager::join()
{
   for (auto& exe : execs) {
      exe->join();
   }
}
void TaskManager::sleepAll(float sleep)
{
   for (auto& exe : execs) {
      exe->sleep = sleep;
   }
}
// -------------------------------------------------------------------------------------
IoChannelCounterAggregator TaskManager::printAggregateExecs(std::ostream& ss, int fromExcecId, int toExecId, bool printDetailed)
{
   IoChannelCounterAggregator aggr;
   for (int i = fromExcecId; i < toExecId; i++) {
      TaskExecutor& exe = *execs[i];
      aggr.aggregate(exe.ioChannel.counters);
      if (printDetailed) {
         ss << exe.id() << ": " << exe.getName() << " ";
         exe.counters.printCounters(ss);
         ss << "\t";
         exe.ioChannel.printCounters(ss);
         ss << std::endl;
      }
      exe.counters.reset();
      exe.ioChannel.counters.reset();
   }
   return aggr;
}
std::string TaskManager::printCountersHeader()
{
   std::stringstream ss;
   /*
   bool printDetailed = true;
   IoChannelCounterAggregator aggr = printAggregateExecs(ss, 0, exclusiveThreadCounter, printDetailed);
   ss << "e[" << exclusiveThreadCounter << "]: aggr: ";
   aggr.print(ss);
   ss << std::endl;
   aggr = printAggregateExecs(ss, exclusiveThreadCounter, execs.size(), printDetailed);
   ss << "w[" << execs.size() - exclusiveThreadCounter << "]: aggr: ";
   aggr.print(ss);
   ss << std::endl;
   */
   execs[0]->counters.printCountersHeader(ss);
   ss << ",";
   execs[0]->ioChannel.counters.printCountersHeader(ss);
   return ss.str();
}
std::string TaskManager::printCounters(int te_id)
{
   std::stringstream ss;
   execs[te_id]->counters.printCounters(ss); 
   execs[te_id]->counters.reset();
   ss << ",";
   execs[te_id]->ioChannel.counters.printCounters(ss);
   execs[te_id]->ioChannel.counters.reset();
   return ss.str();
}
// -------------------------------------------------------------------------------------
// exec
// -------------------------------------------------------------------------------------
TaskExecutor* TaskManager::localExec()
{
   return &TaskExecutor::localExec();
}
int TaskManager::execId()
{
   return TaskExecutor::localExec().id();
}
// -------------------------------------------------------------------------------------
IoChannel& TaskManager::execIoChannel()
{
   return TaskExecutor::localExec().ioChannel;
}
// -------------------------------------------------------------------------------------
// task
// -------------------------------------------------------------------------------------
void TaskManager::registerExclusiveThread(std::string name, int, TaskFunction fun)
{
   int thr = exclusiveThreadCounter++;
   ensure(thr < exclusiveThreads, "There is no worker left. Increase thread count.");  // one worker must remain
   auto& exec = *execs[thr];
   exclusiveThreadsMap.emplace(exec.id(), std::ref(exec));
   sendTask(thr, [name, fun] {
      TaskExecutor::localExec().setNameBeforeStart(name);
      fun();
   });
}
void TaskManager::registerPoller(int to, TaskFunction poller)
{
   sendTask(to, [poller] { TaskExecutor::localExec().pushPoller(poller); });
}
void TaskManager::registerPageProvider(void* bf_ptr, int partitions_count) {
   this->buffer_manager = static_cast<BufferManager*>(bf_ptr);
   this->partitions_count = partitions_count;
   reflowPageProviderPartitions();
}
// -------------------------------------------------------------------------------------
/*
 * Simple parallel for implementation, runs the function over all threads with a number of tasks.
 * The default granularity is ((end-start)/threads/tasks/some factor)
 * If a single cycle through the loop is very short, bbgranularity should be set accordingly higher.
 */
void TaskManager::parallelFor(BlockedRange bb, std::function<void(BlockedRange, std::atomic<bool>& cancelable)> fun, const int tasks, s64 bbgranularity)
{
   ensure(tasks > 0);
   const int threads = workerCount();
   int originExecId = TaskExecutor::localExec().id();
   Task* originTask = &TaskExecutor::localExec().currentTask();
   if (bbgranularity < 1) { bbgranularity = std::max(1ul, (bb.end - bb.begin)/threads/tasks/20); }
   //std::cout << "threads: " << threads << " tasks: " << tasks << " granularity: " << bbgranularity << std::endl;
   const unsigned int totalTasks = threads*tasks;
   std::atomic<u64> bbnow = bb.begin;
   std::atomic<u64> doneTasks = 0;
   std::atomic<bool> cancelable = false;
   int startedTasks = 0;
   for (int thr = 0; thr < threads; thr++) {
      for (int ta = 0; ta < tasks; ta++) {
         startedTasks++;
         sendTask(thr + exclusiveThreads, [&bbnow, &bb, &cancelable, &doneTasks, totalTasks, fun, bbgranularity, originTask, originExecId] {
            // work stealing
            u64 start = 0;
            u64 end = 0;
            while (start < bb.end && !cancelable) {
               bool ok = false;
               while (!ok) {
                  start = bbnow.load();
                  if (start < bb.begin || start >= bb.end) { break; } // all done
                  end = std::min(start + bbgranularity, bb.end);
                  ok = bbnow.compare_exchange_strong(start, end);
               }
               //TaskExecutor::localExec().disableMessagePoller = true;
               if (ok && !cancelable) {
                  assert(start >= bb.begin && start < bb.end);
                  //std::string s = "load: start: " + std::to_string(start) + " end: " + std::to_string(end) + " bbs: " + std::to_string(bb.begin) +  " bbe: " + std::to_string(bb.end);
                  //std::cout << s << std::endl;
                  fun(BlockedRange(start, end), cancelable);
               }
               //TaskExecutor::localExec().disableMessagePoller = false;
            }
            doneTasks++;
            //std::cout << "dones task: " << doneTasks << " toal: " << totalTasks << std::endl;
            if (doneTasks == totalTasks) { // last one hast to wake up the original thread.
            // sendMessage to origin Exec, in it move origin Task from waiting to ready and let it continue
               std::cout << "dones task: " << doneTasks << " toal: " << totalTasks << std::endl;
               TaskExecutor::localExec().sendMessage(
                     originExecId,
                     [](void*, uintptr_t taskPtr) {
                     Task* task = reinterpret_cast<Task*>(taskPtr);
                     TaskExecutor::localExec().moveReady(task);
                     },
                     reinterpret_cast<uintptr_t>(originTask));
            }
         });
         //std::cout << "startedTasks: " << startedTasks << std::endl;
      }
   }
   // yield, and push to waitingTasks
   TaskExecutor::localExec().yieldCurrentTask(TaskState::Waiting);
   std::cout << "parallel for done" << std::endl;
}
void TaskManager::scheduleTaskSync(TaskFunction fun)
{
   fun();
}
// -------------------------------------------------------------------------------------
void TaskManager::yield(TaskState ts)
{
   TaskExecutor::yieldCurrentTask(ts);
}
// -------------------------------------------------------------------------------------
void TaskManager::blockingIo(IoRequestType type, char* data, s64 addr, u64 len)
{
   UserIoCallback cb;
   cb.callback =  [](IoBaseRequest* req) {
           auto this_task = reinterpret_cast<Task*>(req->user.user_data2.val.ptr);
              // std::cout << "completion addr: " << req->addr <<  std::endl << std::flush;
              assert(TaskExecutor::localExec().id() == req->user.user_data.val.s);  //
              TaskExecutor::localExec().moveReady(this_task);
              // TODO maybe push Task to top.
           },
   cb.user_data.val.s = TaskExecutor::localExec().id();
   cb.user_data2.val.ptr = &TaskExecutor::localExec().currentTask();

   TaskExecutor::localExec().ioChannel.push(type, data, addr, len, cb);
   //TaskExecutor::localExec().ioChannel.submit();
   TaskExecutor::yieldCurrentTask(TaskState::WaitIo);
}
// -------------------------------------------------------------------------------------
Task& TaskManager::this_task() {
   return TaskExecutor::localExec().currentTask();
}
// -------------------------------------------------------------------------------------
// other
// -------------------------------------------------------------------------------------
TaskExecutor& TaskManager::getExec(int id)
{
   return *execs[id];
}
void TaskManager::sendTask(int to, TaskFunction taskFun)
{
   auto task = new Task(taskFun);
   TaskExecutor::localExec().sendMessage(
       to,
       [](void*, uintptr_t task) {
          auto t = reinterpret_cast<Task*>(task);
          // ensure(TaskManager::instance().exclusiveThreads.find(this_task::exec().id()) == TaskManager::instance().exclusiveThreads.end());
          TaskExecutor::localExec().pushTask(t);
       },
       reinterpret_cast<uint64_t>(task));
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
int TaskManager::size() const
{
   return execs.size();
}
// -------------------------------------------------------------------------------------
}  // namespace mean
