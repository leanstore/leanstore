#pragma once
// -------------------------------------------------------------------------------------
#include "Units.hpp"
#include "Time.hpp"
// -------------------------------------------------------------------------------------
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <unordered_set>
namespace mean
{
// -------------------------------------------------------------------------------------
struct TaskExecutorCounters {
   std::atomic<u64> cycles = 0;
   std::atomic<u64> tasksRun = 0;
   std::atomic<u64> tasksCompleted = 0;
   std::atomic<u64> tasksWaiting = 0;
   std::atomic<u64> tasksReady = 0;
   // -------------------------------------------------------------------------------------
   std::atomic<u64> taskCount = 0;
   std::atomic<u64> taskWaitingCount = 0;
   // -------------------------------------------------------------------------------------
   std::atomic<u64> taskDuration = 0;
   std::atomic<u64> taskDurationNet = 0;
   std::atomic<u64> pollerDuration = 0;
   std::atomic<u64> ioPollDuration = 0;
   std::atomic<u64> ioSubDuration = 0;
   std::atomic<u64> msgPollDuration = 0;
   std::atomic<u64> msgPollCalled = 0;
   std::atomic<u64> cycleDuration = 0;
   std::atomic<u64> submitCalls = 0;
   std::atomic<u64> submitted = 0;
   // -------------------------------------------------------------------------------------
   std::atomic<u64> ppPhase1Picked = 0;
   std::atomic<u64> ppRequired = 0;
   std::atomic<u64> ppPhase1 = 0;
   std::atomic<u64> ppPhase2 = 0;
   // -------------------------------------------------------------------------------------
   std::atomic<u64> lastReset = readTSC();
   // do not forget reset if you add something

   void reset()
   {
      cycles = 0ul;
      tasksRun = 0ul;
      tasksCompleted = 0ul;
      tasksWaiting = 0ul;
      tasksReady = 0ul;
      tasksRun = 0;
      cycleDuration = 0;
      taskDuration = 0;
      taskDurationNet = 0;
      ioPollDuration = 0;
      ioSubDuration = 0;
      pollerDuration = 0;
      msgPollDuration = 0;
      msgPollCalled = 0;
      ppPhase1Picked = 0;
      ppRequired = 0;
      ppPhase1 = 0;
      ppPhase2 = 0;
      submitCalls = 0;
      submitted = 0;
      lastReset = readTSC();
   }
   void printCountersHeader(std::ostream& ss)
   {
     ss << "schedcylces_k,tasksrun_k,que,wait,tlatavg_us,task_p,pol_p,iop_p,isub_p,msgp_p,pp_phase1_picked,pp_requiredx,pp_phase1,pp_phase2,submit_calls,submitted,msg_poll_called";
   }
   void printCounters(std::ostream& ss)
   {
      auto time = readTSC() - lastReset;
      ss << std::setprecision(3);
      ss << cycles / KILO << ",";
      ss << tasksRun / KILO << ",";
      //ss << " (wait: " << (float)tasksWaiting/tasksRun*100 << "%";
      //ss << " ready: " << (float)tasksReady/tasksRun*100 << "%";
      //ss << " comp: " << (float)tasksCompleted/tasksRun*100 << "%)";
      ss << taskCount << ",";
      ss << taskWaitingCount << ",";
      ss <<(float)taskDuration / tasksRun / 1000 << ",";
      ss <<(float)taskDuration / time * 100 << ",";
      //ss << " tctnet: " << (float)taskDurationNet / time * 100 << "%";
      ss << (float)pollerDuration / time * 100 << ",";
      ss << (float)ioPollDuration / time * 100 << ",";
      ss << (float)ioSubDuration / time * 100 << ",";
      ss << (float)msgPollDuration / time * 100 << ",";
      ss << ppPhase1Picked << ",";
      ss << ppRequired << ",";
      ss << ppPhase1 << ",";
      ss << ppPhase2 << ",";
      ss << submitCalls << ",";
      ss << submitted << ",";
      ss << msgPollCalled << "";
      // do not forget reset if you add something
      // do not forget reset if you add something
      // do not forget reset if you add something
   }
};
}; //namespace mean
