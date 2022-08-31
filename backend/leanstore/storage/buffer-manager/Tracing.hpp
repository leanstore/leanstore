#pragma once
#include "BufferFrame.hpp"
#include "FreeList.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class Tracing
{
  public:
   static std::mutex mutex;
   static std::unordered_map<PID, u64> pid_tracing;
   static void printStatus(PID pid)
   {
      mutex.lock();
      if (pid_tracing.contains(pid)) {
         cout << pid << " was written out: " << pid_tracing[pid] << " times" << endl;
      } else {
         cout << pid << " was never written out" << endl;
      }
      mutex.unlock();
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
