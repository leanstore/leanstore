#pragma once
// -------------------------------------------------------------------------------------
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <sys/types.h>
#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
class ThreadBase;
extern thread_local ThreadBase* _this_thread;
class ThreadBase
{
  protected:
   std::string name;
   std::atomic<bool> _keep_running = false;
   std::atomic<bool> _wait_for_init = true;
   std::atomic<bool> _ready = false;
   int cpuAffinity = -2;
   const int _id = -2;
   std::thread tWorker;

   int _process()
   {
      while (_wait_for_init) { } // wait until parent thread is done creating this thread
      _this_thread = this;
      setNameThisThread(name);
      setCpuAffinityThisThread(cpuAffinity);
      int pid = getpid();
      int which = PRIO_PROCESS;
      posix_check(setpriority(which, pid, 39) == 0, "prio could not be set");

      _ready = true;
      // u32   tid = gettid();
      // std::cout << " page provider tid: " << tid << std::endl;
      int ret = process();
      //_this_thread = nullptr;
      return ret;
   }

  public:
   ThreadBase(std::string name, int id) : name(name), _id(id) {}
   /*
   ThreadBase(const ThreadBase& other)
           : name(other.name), _keep_running(other._keep_running.load()), _ready(other._ready.load()), cpuAffinity(other.cpuAffinity)
   {

   }
   */
   virtual ~ThreadBase(){};

   ThreadBase(const ThreadBase& other) = delete;
   ThreadBase(ThreadBase&& other) = delete;
   ThreadBase& operator=(const ThreadBase& other) = delete;
   ThreadBase& operator=(ThreadBase&& other) = delete;

   virtual int process() = 0;

   void start()
   {
      _keep_running = true;
      tWorker = std::thread(&ThreadBase::_process, this);
      _wait_for_init = false;
   }

   bool ready() { return _ready; }

   void stop() { _keep_running = false; }

   bool keepRunning() { return _keep_running; }

   void join()
   {
      if (tWorker.joinable()) {
         tWorker.join();
      }
   }

   int id() const { return _id; }

   std::string getName() const { return name; }

   void setNameBeforeStart(std::string name) { this->name = name; }

   void setNameThisThread(std::string name)
   {
      this->name = name;
      posix_check(pthread_setname_np(pthread_self(), name.c_str()) == 0);
      if (pthread_self() != thread_impl().native_handle()) {
         std::cout << "looks like somebody set the wrong thread?" << std::endl;
      }
   }

   void setCpuAffinityThisThread(int cpuAffinity) {
      this->cpuAffinity = cpuAffinity;
      if (cpuAffinity >= 0) {
         cpu_set_t cpuset;
         CPU_ZERO(&cpuset);
         CPU_SET(cpuAffinity, &cpuset);
         auto thread = pthread_self();
         if (pthread_self() != thread_impl().native_handle()) {
            std::cout << "looks like somebody set the wrong thread?" << std::endl;
         }
         int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
         if (s != 0) {
            ensure(false, "Affinity could not be set.");
         }
         s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
         if (s != 0) {
            ensure(false, "Affinity could not be set.");
         }
      }
   }

   void setCpuAffinityBeforeStart(int cpuAffinity) { this->cpuAffinity = cpuAffinity; }

   int getCpuAffinity() const { return cpuAffinity; }

   std::thread& thread_impl() {
      return tWorker;
   }

   static ThreadBase& this_thread()
   {
      assert(_this_thread);
      return *_this_thread;
   }
};
class Thread : public ThreadBase
{
  protected:
   std::function<void()> fun;

  public:
   Thread(std::function<void()> fun, std::string name = "Thread", int id = -1) : ThreadBase(name, id), fun(fun) {}
   int process() override
   {
      fun();
      return 0;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
