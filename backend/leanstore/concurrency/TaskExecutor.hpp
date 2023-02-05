#pragma once
// -------------------------------------------------------------------------------------
#include "Exceptions.hpp"
#include "MessageHandler.hpp"
#include "Task.hpp"
#include "ThreadBase.hpp"
#include "Units.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/io/IoAbstraction.hpp"
#include "leanstore/profiling/counters/TaskExecutorCounters.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/storage/btree/core/BTreeInterface.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
// -------------------------------------------------------------------------------------
#include "boost/context/continuation.hpp"
#include "boost/context/continuation_fcontext.hpp"
// -------------------------------------------------------------------------------------
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
class TaskExecutor : public ThreadBase
{
   static const int MAX_TASKS = 256;
   leanstore::utils::RingBuffer<Task*> tasks{MAX_TASKS};
   leanstore::utils::RingBuffer<Task*> tasks_io_done{MAX_TASKS};
#ifndef NDEBUG
   std::unordered_map<Task*, std::tuple<TaskState, TimePoint, bool>> waiting_tasks;
#endif
   std::deque<std::unique_ptr<Task>> pollers;
   Task* _currentTask = nullptr;
   // -------------------------------------------------------------------------------------
   //
   MessageHandler& messageHandler;
   jumpmu::JumpMUContext defaultExecutorContext;
   boost::context::continuation main_process_context;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   u64 waitingTaskCount = 0;
   u64 waitIoTaskCount = 0;
   // -------------------------------------------------------------------------------------
   void runUserThread(Task& task);
   int process() override;
   void cycle();
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   s64 partition_id = -1;
   BufferManager* buffer_manager;
   s64 pp_required = 0;
   bool local_pause_seen = false;
  public:
   //static std::atomic<bool> pause;
   //static std::atomic<int> pause_seen;
   bool disableMessagePoller = false;
   IoChannel& ioChannel;
   TaskExecutorCounters counters;
   // -------------------------------------------------------------------------------------
   leanstore::cr::Worker* this_worker;
   // -------------------------------------------------------------------------------------
   TaskExecutor(MessageHandler& msg, IoChannel& io, int id);
   ~TaskExecutor();
   // -------------------------------------------------------------------------------------
   TaskExecutor(const TaskExecutor&) = delete;
   TaskExecutor(TaskExecutor&&) = delete;
   TaskExecutor& operator=(const TaskExecutor&) = delete;
   TaskExecutor& operator=(TaskExecutor&&) = delete;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   bool popTask(Task*& task);
   // -------------------------------------------------------------------------------------
   void pushPoller(TaskFunction poller);
   void registerPageProvider(void*, u64 partition_id);
   void pageProviderCycle();
   void pushTask(Task* task);
   void pushTask(TaskFunction fun);
   void moveReady(Task* task);
   int taskCount();
   void sendMessage(int toId, MessageFunction fun, uintptr_t userData);
   // -------------------------------------------------------------------------------------
   static TaskExecutor& localExec();
   static Task& currentTask();
   static void yieldCurrentTask(TaskState ts);
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
