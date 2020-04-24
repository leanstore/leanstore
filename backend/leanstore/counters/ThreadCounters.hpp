#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <mutex>
#include <string>
#include <unordered_map>
#include <memory>
// -------------------------------------------------------------------------------------
// Sadly, we can not use TBB enumerable thread specific here because it does not automatically remove its entry upon thread destroy
namespace leanstore
{
struct ThreadCounters {
  std::unique_ptr<PerfEvent> e;
  string name;
  // -------------------------------------------------------------------------------------
  static u64 id;
  static std::unordered_map<u64, ThreadCounters> thread_counters;
  static std::mutex mutex;
  static u64 registerThread(string name);
  static void removeThread(u64 id);
};
}  // namespace leanstore
