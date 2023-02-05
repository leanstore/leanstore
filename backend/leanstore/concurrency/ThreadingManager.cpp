// -------------------------------------------------------------------------------------
#include "ThreadingManager.hpp"
#include "leanstore/concurrency/ConnectedIoChannel.hpp"
#include "leanstore/concurrency/Task.hpp"
#include "leanstore/io/IoInterface.hpp"
#include "leanstore/io/impl/LibaioImpl.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <mutex>
#include <condition_variable>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
ThreadingManager::~ThreadingManager()
{
   shutdown();
}
// -------------------------------------------------------------------------------------
// env
// -------------------------------------------------------------------------------------
void ThreadingManager::init(int workers_count, int exclusiveThreads, IoOptions ioOptions, [[maybe_unused]] int threadAffinityOffset)
{
   ensure(ioOptions.engine == "libaio" || ioOptions.engine == "liburing");
   total_threads_count = workers_count + exclusiveThreads;
   max_exclusive_threads = exclusiveThreads;
   ensure(max_exclusive_threads > 0, "in threading mode there must be at least one pp thread. Be sure to not use --nopp flag.");
   IoInterface::initInstance(ioOptions);
   ensure(total_threads_count < MAX_WORKER_THREADS);

   for (int t_i = 0; t_i < total_threads_count; t_i++) {
      auto thread = std::make_unique<ThreadWithJump>([&, t_i]() {
        // -------------------------------------------------------------------------------------
        std::string name = std::to_string(t_i);
        leanstore::CPUCounters::registerThread(name, false);
        // -------------------------------------------------------------------------------------
        workers[t_i] = new leanstore::cr::Worker(t_i, workers, workers_count);
        leanstore::cr::Worker::tls_ptr = workers[t_i];
        // -------------------------------------------------------------------------------------
        running_threads++;
        while (ThreadBase::this_thread().keepRunning()) {
           auto& meta = static_cast<ThreadWithJump*>(&ThreadBase::this_thread())->meta;
           std::unique_lock guard(meta.mutex);
           meta.cv.wait(guard, [&]() { return  ThreadBase::this_thread().keepRunning() == false || meta.job_set; });
           if (!ThreadBase::this_thread().keepRunning()) {
              break;
           }
           meta.wt_ready = false;
           meta.task();
           meta.wt_ready = true;
           meta.job_done = true;
           meta.job_set = false;
           meta.cv.notify_one();
        }
        running_threads--;
      }, "w_" + std::to_string(t_i), t_i);
      if (t_i < max_exclusive_threads) {
         thread->setCpuAffinityBeforeStart(t_i);
         thread->setNameBeforeStart("x_" + std::to_string(t_i));
      }
      all_threads.push_back(std::move(thread));
      all_threads.back()->start();
   }
   //for (auto& t : all_threads) {
   //   t.detach();
   //}
   // -------------------------------------------------------------------------------------
   // Wait until all worker threads are initialized
   while (running_threads < total_threads_count) {
   }
}
// -------------------------------------------------------------------------------------
void ThreadingManager::start(TaskFunction taskFun)
{
   //all_threads[max_exclusive_threads]->sendTask(taskFun);
   taskFun();
}
// -------------------------------------------------------------------------------------
void ThreadingManager::shutdown()
{
   for (auto& exe : all_threads) {
      exe->shutdown();
   }
}
// -------------------------------------------------------------------------------------
void ThreadingManager::join()
{
   for (auto& exe : all_threads) {
      exe->join();
   }
}
// -------------------------------------------------------------------------------------
std::string ThreadingManager::stats()
{
   /*
   std::stringstream ss;
   for (auto& exe: execs) {
           ss << exe->id()  << ": "<<  exe->getName() << " ";
           exe->counters.printCounters(ss);
           exe->counters.reset();
           ss << "\t";
           exe->ioChannel.printCounters(ss);
           ss << std::endl;
   }
   ss << std::endl;
   return ss.str();
   */
   return "";
}
void ThreadingManager::adjustWorkerCount(int workerThreads) {

}
std::string ThreadingManager::printCountersHeader() {
   return "a";
}
std::string ThreadingManager::printCounters(int te_id) {
   return "a";
}
// -------------------------------------------------------------------------------------
// exec
// -------------------------------------------------------------------------------------
int ThreadingManager::execId()
{
   return ThreadBase::this_thread().id();
}
// -------------------------------------------------------------------------------------
IoChannel& ThreadingManager::execIoChannel()
{
   int this_id = execId();
   // TODO check if in exclusive thread
   return IoInterface::instance().getIoChannel(this_id);
}
// -------------------------------------------------------------------------------------
// task
// -------------------------------------------------------------------------------------
void ThreadingManager::registerExclusiveThread(std::string name, int, TaskFunction taskFun)
{
   int id = exclusiveThreadCounter++;
   auto& ex = *all_threads[id];
   ex.setNameBeforeStart(name);
   ex.sendTask(taskFun);
}
void ThreadingManager::registerPageProvider(void* bf_ptr, int partitions_count) {
   auto buffer_manager = static_cast<leanstore::storage::BufferManager*>(bf_ptr);
   for (int t_i = 0; t_i < partitions_count; t_i++) {
      registerExclusiveThread("pp", t_i, [buffer_manager, t_i, this](){
         while (true) {
            buffer_manager->pageProviderCycle(t_i);
            execIoChannel().submit();
            execIoChannel().poll();
         }
      });
   }
}
// -------------------------------------------------------------------------------------
void ThreadingManager::registerPoller([[maybe_unused]] int to, TaskFunction poller)
{
   registerExclusiveThread("poller", -1, poller);
}
// -------------------------------------------------------------------------------------
void ThreadingManager::parallelFor(BlockedRange bb, std::function<void(BlockedRange, std::atomic<bool>& cancelable)> fun, const int tasks, s64 bbgranularity)
{
   ensure(tasks > 0);
   std::mutex allDoneMutex;
   std::condition_variable allDone;
   std::atomic<int> threadsDone = 0;
   const int threads = workerCount();
   std::atomic<bool> cancelable = false;
   u64 range = (bb.end - bb.begin) / threads;
   u64 remaining = (bb.end - bb.begin) % threads;
   if (range == 0) {
      range = 1;
      remaining = 0;
   }
   u64 start = bb.begin;
   for (int thr = 0; thr < threads; thr++) {
      BlockedRange rangePart(start,start + range);
      if (remaining > 0) {
         rangePart.end += 1;
         start += 1;
         remaining--;
      }
      // rangePart = bb;
      all_threads.at(thr + max_exclusive_threads)->sendTask([&threadsDone, &allDone, threads, fun, rangePart, &cancelable] {
        fun(rangePart, cancelable);
        threadsDone++;
        if (threadsDone == threads) {
           allDone.notify_one();
        }
      });
      start += range;
   }
   std::unique_lock<std::mutex> lk(allDoneMutex);
   allDone.wait(lk);
}
// -------------------------------------------------------------------------------------
void ThreadingManager::scheduleTaskSync(TaskFunction fun)
{
   all_threads.at(0 + max_exclusive_threads)->sendTaskBlocking(fun);
}
// -------------------------------------------------------------------------------------
void ThreadingManager::yield([[maybe_unused]]TaskState ts)
{
   // do nothing?
}
// -------------------------------------------------------------------------------------
void ThreadingManager::blockingIo(IoRequestType type, char* data, s64 addr, u64 len)
{
   execIoChannel().pushBlocking(type, data, addr, len);
}
Task& ThreadingManager::this_task() {
   throw std::logic_error("cannot be called when running with threads");
}
// -------------------------------------------------------------------------------------
// other
// -------------------------------------------------------------------------------------
int ThreadingManager::workerCount()
{
   return total_threads_count - max_exclusive_threads;
}
// -------------------------------------------------------------------------------------
}  // namespace mean
