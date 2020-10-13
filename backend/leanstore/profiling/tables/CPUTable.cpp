#include "CPUTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
std::string CPUTable::getName()
{
   return "cpu";
}
// -------------------------------------------------------------------------------------
void CPUTable::open()
{
   PerfEvent e;
   for (const auto& event_name : e.getEventsName()) {
      workers_agg_events[event_name] = 0;
      pp_agg_events[event_name] = 0;
      ww_agg_events[event_name] = 0;
      columns.emplace(event_name, [](Column&) {});
   }
   columns.emplace("key", [](Column&) {});
}
// -------------------------------------------------------------------------------------
void CPUTable::next()
{
   clear();
   // -------------------------------------------------------------------------------------
   for (auto& c : workers_agg_events) {
      c.second = 0;
   }
   for (auto& c : pp_agg_events) {
      c.second = 0;
   }
   for (auto& c : ww_agg_events) {
      c.second = 0;
   }
   // -------------------------------------------------------------------------------------
   {
      std::unique_lock guard(CPUCounters::mutex);
      for (auto& thread : CPUCounters::threads) {
         thread.second.e->stopCounters();
         auto events_map = thread.second.e->getCountersMap();
         columns.at("key") << thread.second.name;
         for (auto& event : events_map) {
            double event_value;
            if (std::isnan(event.second)) {
               event_value = 0;
            } else {
               event_value = event.second;
            }
            if (thread.second.name.rfind("worker", 0) == 0) {
               workers_agg_events[event.first] += event_value;
            } else if (thread.second.name.rfind("pp", 0) == 0) {
               pp_agg_events[event.first] += event_value;
            } else if (thread.second.name.rfind("ww") == 0) {
               ww_agg_events[event.first] += event_value;
            }
            columns.at(event.first) << event.second;
         }
         thread.second.e->startCounters();
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
