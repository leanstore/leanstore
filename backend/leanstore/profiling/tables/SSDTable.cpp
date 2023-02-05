#include "SSDTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/SSDCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
#include "leanstore/concurrency/Mean.hpp"
// -------------------------------------------------------------------------------------
#include <iomanip>
#include <string>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
SSDTable::SSDTable() : ProfilingTable() {}
// -------------------------------------------------------------------------------------
std::string SSDTable::getName()
{
   return "ssd";
}
// -------------------------------------------------------------------------------------
void SSDTable::open()
{
   columns.emplace("key", [&](Column& col) { col << ssd; });
   columns.emplace("name", [&](Column& col) {
         col << mean::IoInterface::instance().getDeviceInfo().devices[ssd].name; 
         assert(ssd == mean::IoInterface::instance().getDeviceInfo().devices[ssd].id); 
   });
   columns.emplace("pushed_k", [&](Column& col) { col << local_pushed / KILO;});
   columns.emplace("polled_l", [&](Column& col) { col << local_polled / KILO;});
   columns.emplace("outstanding_max", [&](Column& col) { col << sum(SSDCounters::ssd_counters, &SSDCounters::outstandingx_max, ssd);});
   columns.emplace("outstanding_min", [&](Column& col) { col << sum(SSDCounters::ssd_counters, &SSDCounters::outstandingx_min, ssd);});
   columns.emplace("reads_k", [&](Column& col) { col << sum(SSDCounters::ssd_counters, &SSDCounters::reads, ssd) / KILO;});
   columns.emplace("writes_k", [&](Column& col) { col << sum(SSDCounters::ssd_counters, &SSDCounters::writes, ssd) / KILO;});
   columns.emplace("lat_read_50pm_us", [&](Column& col) { col << utils::threadlocal::thr_aggr_max(SSDCounters::ssd_counters, &SSDCounters::read_latncy50p, ssd); });
   columns.emplace("lat_read_99p9m_us", [&](Column& col) { col << utils::threadlocal::thr_aggr_max(SSDCounters::ssd_counters, &SSDCounters::read_latncy99p9, ssd); });
   columns.emplace("lat_read_max_us", [&](Column& col) { col << utils::threadlocal::thr_aggr_max(SSDCounters::ssd_counters, &SSDCounters::read_latncy_max, ssd); });
   columns.emplace("lat_write_50pm_us", [&](Column& col) { col << utils::threadlocal::thr_aggr_max(SSDCounters::ssd_counters, &SSDCounters::write_latncy50p, ssd); });
   columns.emplace("lat_write_99p9m_us", [&](Column& col) { col << utils::threadlocal::thr_aggr_max(SSDCounters::ssd_counters, &SSDCounters::write_latncy99p9, ssd); });
}
// -------------------------------------------------------------------------------------
void SSDTable::next()
{
   clear();
   int ssds = mean::IoInterface::instance().getDeviceInfo().devices.size();
   for (int i = 0; i < ssds; i++) {
      ssd = i;
      local_pushed = sum(SSDCounters::ssd_counters, &SSDCounters::pushed, ssd);
      local_polled = sum(SSDCounters::ssd_counters, &SSDCounters::polled, ssd);
      for (auto& c : columns) {
         c.second.generator(c.second);
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
