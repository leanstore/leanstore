#include "CpuTopology.hpp"
#include <string>

#include "Files.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// search for optimal communication partne in a range. e.g. cpu 0 to 60
int optimalThreadPartner(int rangeFrom, int rangeTo, int cpuId) {
   // first try the core sibling
   // if not in range use the first patner that shares the lowes cache level
   std::string cpu = "/sys/devices/system/cpu/cpu";
   std::string cacheindex = "/cache/index";
   std::string list = "/shared_cpu_list";
   std::string cpucacheindex = cpu + std::to_string(cpuId) + cacheindex;
   int cache_id = 0;
   int partner = -1;
   while (fileExists(cpucacheindex + std::to_string(cache_id) + list) ) {
      std::string shared_list = LoadFileToMemory(cpucacheindex + std::to_string(cache_id) + list);

      cache_id++;
   }
   return 0;
}
}  // namespace utils
}  // namespace leanstore
// -------------------------------------------------------------------------------------
