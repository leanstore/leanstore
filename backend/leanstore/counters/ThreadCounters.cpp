#include "ThreadCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
u64 ThreadCounters::id = 0;
std::unordered_map<u64, ThreadCounters> ThreadCounters::thread_counters;
std::mutex ThreadCounters::mutex;
// -------------------------------------------------------------------------------------
u64 ThreadCounters::registerThread(string name)
{
  std::unique_lock guard(mutex);
  thread_counters[id] = {.e = std::make_unique<PerfEvent>(), .name = name};
  return id++;
}
void ThreadCounters::removeThread(u64 id)
{
  std::unique_lock guard(mutex);
  thread_counters.erase(id);
}
}  // namespace leanstore
