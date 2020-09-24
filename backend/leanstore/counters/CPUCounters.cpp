#include "CPUCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
u64 CPUCounters::id = 0;
std::unordered_map<u64, CPUCounters> CPUCounters::thread_counters;
std::mutex CPUCounters::mutex;
// -------------------------------------------------------------------------------------
u64 CPUCounters::registerThread(string name)
{
  std::unique_lock guard(mutex);
  thread_counters[id] = {.e = std::make_unique<PerfEvent>(), .name = name};
  return id++;
}
void CPUCounters::removeThread(u64 id)
{
  std::unique_lock guard(mutex);
  thread_counters.erase(id);
}
}  // namespace leanstore
