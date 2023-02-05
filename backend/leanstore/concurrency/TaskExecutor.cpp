// -------------------------------------------------------------------------------------
#include "TaskExecutor.hpp"
// -------------------------------------------------------------------------------------
#include "Exceptions.hpp"
#include "MessageHandler.hpp"
#include "ThreadBase.hpp"
#include "Time.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/concurrency/Task.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "boost/context/continuation.hpp"
// -------------------------------------------------------------------------------------
#include <boost/context/preallocated.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
#if true //MACRO_COUNTERS_ALL
#define DEBUG_TASK_COUNTERS_BLOCK(X) X
#else
#define DEBUG_TASK_COUNTERS_BLOCK(X)
#endif
//std::atomic<bool> TaskExecutor::pause = true;
//std::atomic<int> TaskExecutor::pause_seen = 0;
// -------------------------------------------------------------------------------------
TaskExecutor::TaskExecutor(MessageHandler& msg, IoChannel& ioChannel, int id)
    : ThreadBase("te_" /*+ std::to_string(id)*/, id), messageHandler(msg), ioChannel(ioChannel)
{
   jumpmu::thread_local_jumpmu_ctx = &defaultExecutorContext;
}
TaskExecutor::~TaskExecutor() {}
// -------------------------------------------------------------------------------------
void TaskExecutor::runUserThread(Task& task)
{
   // std::cout << "runUserThread swap oucp: " << std::hex <<  current_uctx << " ucp: " << &th->context  << std::dec <<  std::endl << std::flush;
   _currentTask = &task;
   jumpmu::thread_local_jumpmu_ctx = &task.jumpctx;
   // -------------------------------------------------------------------------------------
    // -------------------------------------------------------------------------------------
   if (!task.context.init) {
      // std::cout << "xx runUserThread init " << std::endl;
      task.context.init = true;
      task.context.this_task_context =
          boost::context::callcc(std::allocator_arg, boost::context::fixedsize_stack(8 * 1024 * 1024), [&task](boost::context::continuation&& sink) {
             // task.context.this_task_context = boost::context::callcc([&task](boost::context::continuation&& sink_process_context){
             // std::cout << "callcc lamda " <<std::endl << std::flush;
             task.context.sink_process_context = &sink;
             task.fun();
             task.state = TaskState::Done;
             // std::cout << "callcc lamda end" <<std::endl << std::flush;
             return std::move(sink);
          });
      // std::cout << "xx runUserThread init done  " << std::endl;
   } else {
      ////std::cout << "xx runUserThread resume " << std::endl;
      task.context.this_task_context = task.context.this_task_context.resume();
   }
   _currentTask = nullptr;
   jumpmu::thread_local_jumpmu_ctx = &defaultExecutorContext;
   // std::cout << "rundUserThread swap end " <<  std::endl << std::flush;
}
// -------------------------------------------------------------------------------------
void TaskExecutor::yieldCurrentTask(TaskState ts)
{
   // std::cout << "yield" << std::endl << std::flush;
   Task& task = currentTask();
   task.state = ts;
   // cycle(); TODO directly run scheduler and jump to next context
   // this would return to process task.
   *task.context.sink_process_context = task.context.sink_process_context->resume();
   // Careful: function will continue here only when the task is being resumed
   // std::cout << "continue" << std::endl << std::flush;
}
// -------------------------------------------------------------------------------------
int TaskExecutor::process()
{
   ensure(tasks.size() == 0);
   // std::cout << "boost fixedsize_tack size: " << boost::context::fixedsize_stack::traits_type::default_size() << std::endl;
   // std::cout << "taskexec gettid: " << gettid() << std::endl;
   // -------------------------------------------------------------------------------------
   leanstore::cr::Worker::tls_ptr = this_worker;
   leanstore::CPUCounters::registerThread(std::to_string(id()), false);
   // -------------------------------------------------------------------------------------
   cycle();
   // std::cout << "# task exec end return " << id() << " #" << std::endl << std::flush;
   return 0;
}
#ifdef ENABLE_CYLCLETRACE
std::vector<std::pair<char, u64>> cyTrace;
u64 cyTracePos = 0;
void initCyTrace() {
   cyTrace.resize(1000000);
}
inline void pushCyTrace(char type, u64 tp) {
   if (cyTracePos == cyTrace.size())
      cyTracePos = 0;
   cyTrace[cyTracePos++] = std::pair<char,u64>(type, tp);
}
void printCyTrace() {
   int pos = cyTracePos;
   int cycle = 0;
   for (int i = 0; i < cyTrace.size(); i++) {
      if (pos == cyTrace.size())
         pos = 0;
      auto& c = cyTrace[pos++];
      std::cout << i << "," << c.first << "," << c.second << "," << cycle << std::endl;
      if (c.first == 'm')
         cycle++;
   }
}
#else
void initCyTrace() {}
inline void pushCyTrace(char type, u64 tp) { }
void printCyTrace() { }
#endif
bool TaskExecutor::popTask(Task*& task) {
   if (tasks_io_done.try_pop(task)) {
      return true;
   }
   return tasks.try_pop(task);
}
void TaskExecutor::cycle()
{
   initCyTrace();
   // -------------------------------------------------------------------------------------
   DEBUG_TASK_COUNTERS_BLOCK(auto lastCycle = readTSC(); pushCyTrace('s', lastCycle); );
   u64 cycles = 0;
   u64 cyclesNothingRun = 0;
   const u64 sleepIfNothingRunForCycles = 100000;
   u64 delaySubmitUntilCycle = 0;
   auto counterUpdateTime = getSeconds();
   random_generator.seed(mean::exec::getId());
   std::cout << "r: " << leanstore::utils::RandomGenerator::getRand(0, 1000) << std::endl;
   while (_keep_running) {
      cycles++;
      // good for ycsb 60 thr: p: 2, pp: 32, d: 0
      constexpr int everyPoll = 64;
      constexpr int everyPP = 32;
      constexpr int delaySubmit = 32;
      DEBUG_TASK_COUNTERS_BLOCK(
         const auto ioPollEnd = readTSC(); counters.ioPollDuration += ioPollEnd - lastCycle; pushCyTrace('i', ioPollEnd);
         counters.taskCount = tasks.size(); counters.taskWaitingCount = waitingTaskCount;
         )
         // run poll Routines
         // TODO for runs with >> 60 threads, this hast to be changed
         if (cycles % (8*1024) == 0 || cyclesNothingRun > sleepIfNothingRunForCycles) {
            messageHandler.poll(this);
            counters.msgPollCalled++;
         }
      DEBUG_TASK_COUNTERS_BLOCK(
         const auto pollerStart = readTSC(); counters.msgPollDuration += (pollerStart - ioPollEnd) > 0 ? pollerStart - ioPollEnd : 0 ; pushCyTrace('m', pollerStart);
         )
         /*
            if (pollers.size() > 0) {
         // std::cout << "run pollers..." << std::endl;
         for (auto& p : pollers) {
         // std::cout << "run pollers i " << std::endl;
         // p->poll();
         runUserThread(*p);
         //	std::cout << "done run pollers i " << std::endl;
         }
         }
         */
         if (cycles % everyPP  == 0) {
            pageProviderCycle();
         }
      DEBUG_TASK_COUNTERS_BLOCK(
         const auto ioSubStart = readTSC();
         const auto pollerDuration = ioSubStart - pollerStart;
         counters.pollerDuration += pollerDuration;
         pushCyTrace('p', ioSubStart);
         if (pollerDuration > leanstore::ThreadCounters::myCounters().exec_cycl_max_task_us) { leanstore::ThreadCounters::myCounters().exec_cycl_max_subm_us = pollerDuration; }
         )
         if (delaySubmit == 0) {
            int submitted = ioChannel.submit();
            DEBUG_TASK_COUNTERS_BLOCK(
               counters.submitCalls++;
               counters.submitted += submitted;
            )
         } else {
            if (ioChannel.submitable() > 0) {
               if (delaySubmitUntilCycle < cycles) {
                  delaySubmitUntilCycle = cycles + delaySubmit;
               } else if (delaySubmitUntilCycle == cycles) { 
                  int submitted = ioChannel.submit();
                  DEBUG_TASK_COUNTERS_BLOCK(
                     counters.submitCalls++;
                     counters.submitted += submitted;
                  )
               }
            }
         }
      if (cycles % everyPoll == 0) {
         leanstore::ThreadCounters::myCounters().exec_cycles += everyPoll;
         counters.cycles = cycles;
         ioChannel.poll();
         if (cycles % (8*1024)) {
            auto now = getSeconds();
            if (now - counterUpdateTime > 0.99999) {
               counterUpdateTime = now;
               //COUNTERS_BLOCK() { ioChannel.counters.updateLeanStoreCounters(); }
               //COUNTERS_BLOCK() { ioChannel.counters.reset(); }
            }
         }
      }

      DEBUG_TASK_COUNTERS_BLOCK(
         const auto taskStart = readTSC();
         const auto subDration = (taskStart - ioSubStart) > 0 ? taskStart - ioSubStart: 0;
         if (subDration > leanstore::ThreadCounters::myCounters().exec_cycl_max_task_us) { leanstore::ThreadCounters::myCounters().exec_cycl_max_subm_us = subDration; }
         counters.ioSubDuration += subDration;
         pushCyTrace('s', taskStart);
         )
         // -------------------------------------------------------------------------------------
         // schedule next task
         const int maxTasksRun = 1;
      int tasksRun = 0;
      //*
      Task* task;
      while (tasksRun < maxTasksRun && popTask(task)) { // pop after maxTaskRun check
         //std::cout << this->getName() << " runTask: " << task << " task.size: " << tasks.size() << std::endl;
         counters.tasksRun++;
         //DEBUG_TASK_COUNTERS_BLOCK(auto start = readTSC();)
         if (task->state == TaskState::ReadyLock) {
            if (!task->lock->try_lock()) {
               tasks.push_back(task);
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_ready_lckskip++; }
               break;
            }
         }
         tasksRun++;
         runUserThread(*task);
         //DEBUG_TASK_COUNTERS_BLOCK(auto time = readTSC() - start; counters.taskDurationNet += time;)
         switch (task->state) {
            case TaskState::Done:
               counters.tasksCompleted++;
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_comp++; }
               //memset((void*)task, 0, sizeof(Task));
               //delete task; //FIXME
               break;
            case TaskState::Waiting:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_wait++; }
               counters.tasksWaiting++;
               waitingTaskCount++;
               break;
            case TaskState::WaitIo:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_wait_io++; }
               counters.tasksWaiting++;
               waitingTaskCount++;
               waitIoTaskCount++;
#ifndef NDEBUG
               waiting_tasks[task] = std::tuple(TaskState::WaitIo, getTimePoint(), false);
#endif
               break;
            case TaskState::ReadyMem:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_ready_mem++; }
               counters.tasksReady++;
               tasks.push_back(task);
               break;
            case TaskState::ReadyLock:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_ready_lck++; }
               counters.tasksReady++;
               tasks.push_back(task);
               break;
            case TaskState::ReadyJumpLock:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_ready_jumplck++; }
               counters.tasksReady++;
               tasks.push_back(task);
               break;
            case TaskState::Ready:
               COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_st_ready++; }
               counters.tasksReady++;
               tasks.push_back(task);
               break;
            default:
               throw std::logic_error("should never happen");
         }
         COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_tasks_run++; }
      }
#ifndef NDEBUG
      auto now = getTimePoint();
      for (auto& tt: waiting_tasks) {
         auto diff = timePointDifferenceMs(now, std::get<1>(tt.second));
         if (diff > 1000 && !std::get<2>(tt.second)) {
            std::get<2>(tt.second) = true;
            std::cout << "io is stuck " << tt.first << " diff: " << std::dec << diff << std::endl;
            //raise(SIGINT);
         }
      }
#endif
      if (tasksRun == 0) {
         COUNTERS_BLOCK() { leanstore::ThreadCounters::myCounters().exec_no_tasks_run++; }
         cyclesNothingRun++;
         ///*
         if (cyclesNothingRun > sleepIfNothingRunForCycles) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (waitingTaskCount > 0 && cyclesNothingRun == sleepIfNothingRunForCycles + 5000) { // thread did not move in 5 seconds
               //ensure(false);
               std::cout << "something might be stuck: thread: " << mean::exec::getId() << " native: " << std::hex <<std::this_thread::get_id() << std::dec;
               std::cout << " waiting: " << waitingTaskCount << " tasks: " << tasks.contains << " io_done: " << tasks_io_done.contains  << std::endl;
               //raise(SIGINT);
            } 
         }
         //*/
      } else {
         cyclesNothingRun = 0;
      }
      DEBUG_TASK_COUNTERS_BLOCK(
         const auto taskEnd = readTSC();
         const auto taskDuration = taskEnd - taskStart;
         counters.taskDuration += taskDuration;
         const auto totalDuration = taskEnd - lastCycle;
         if (taskDuration > leanstore::ThreadCounters::myCounters().exec_cycl_max_task_us) { leanstore::ThreadCounters::myCounters().exec_cycl_max_task_us = taskDuration; }
         if (totalDuration > leanstore::ThreadCounters::myCounters().exec_cycl_max_dur) { leanstore::ThreadCounters::myCounters().exec_cycl_max_dur = totalDuration; }
         /*
            if (totalDuration / 2 / 1000 > 10000) {
            std::cout << "long cycle.. thr: " << mean::exec::getId() << " duration: " << totalDuration / 2 / 1000<<  std::endl;
            }
            */
         lastCycle = taskEnd; pushCyTrace('t', taskEnd);
         )
         // std::this_thread::sleep_for(std::chrono::seconds(1));
   }
   printCyTrace();
}

// -------------------------------------------------------------------------------------
void TaskExecutor::pushPoller(TaskFunction poller)
{
   pollers.push_back(std::make_unique<Task>(poller));
}
void TaskExecutor::registerPageProvider(void* bm_ptr, u64 partition_id) {
   this->partition_id = partition_id;
   this->buffer_manager = static_cast<BufferManager*>(bm_ptr);
   ensure(buffer_manager->cooling_partitions_count > partition_id);
   buffer_manager->cooling_partitions[partition_id].state.debug_thread = mean::exec::getId();
}
void TaskExecutor::pageProviderCycle() {
   /*
      if (TaskExecutor::pause) {
   // all ios must be done to have a save partition reflow
   if (local_pause_seen) {
   return;
   }
   bool allDone = true;
   for (u64 p_i = p_begin; p_i < p_end; p_i++) {
   allDone &= buffer_manager->partitions[p_i].state.submitted == buffer_manager->partitions[p_i].state.done;
   }
   if (allDone) {
   TaskExecutor::pause_seen++;
   local_pause_seen = true;
   }
   return;
   } else if (local_pause_seen) {
   local_pause_seen = false;
   }
   */
   if (buffer_manager && partition_id >= 0) {
      buffer_manager->pageProviderCycle(partition_id);
   }
}
void TaskExecutor::pushTask(Task* task)
{
   tasks.push_back(task);
}
void TaskExecutor::pushTask(TaskFunction fun)
{
   auto task = new Task(fun);
   tasks.push_back(task);
}
void TaskExecutor::moveReady(Task* task)
{
   task->state = TaskState::Ready;
#ifndef NDEBUG
   waiting_tasks.erase(task);
#endif
   waitingTaskCount--;
   waitIoTaskCount--;
   tasks_io_done.push_back(task);
}
int TaskExecutor::taskCount()
{
   return tasks.size();
}
void TaskExecutor::sendMessage(int toId, MessageFunction fun, uintptr_t userData)
{
   messageHandler.sendMessage(toId, fun, userData);
}
TaskExecutor& TaskExecutor::localExec()
{
   return static_cast<TaskExecutor&>(ThreadBase::this_thread());
}
Task& TaskExecutor::currentTask()
{
   auto t = localExec()._currentTask;
   ensure(t);
   return *t;
}
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
