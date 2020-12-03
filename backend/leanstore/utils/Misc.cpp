#include "Misc.hpp"

#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
#include "CRC.hpp"
// -------------------------------------------------------------------------------------
#include <execinfo.h>

#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
u32 getBitsNeeded(u64 input)
{
   return std::max(std::floor(std::log2(input)) + 1, 1.0);
}
// -------------------------------------------------------------------------------------
double calculateMTPS(std::chrono::high_resolution_clock::time_point begin, std::chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
void pinThisThreadRome()
{
   static atomic<u64> a_t_i = 0;
   u64 t_i = a_t_i++;
   pinThisThreadRome(t_i);
}
// -------------------------------------------------------------------------------------
// Hard coded for Rome
void pinThisThreadRome(const u64 t_i)
{
   u64 pin_id;
   if (FLAGS_smt) {
      u64 cpu = t_i / 8;
      u64 l_cpu = t_i % 8;
      bool is_upper = l_cpu > 3;
      pin_id = (is_upper) ? (64 + (cpu * 4) + (l_cpu % 4)) : ((cpu * 4) + (l_cpu % 4));
   } else {
      pin_id = t_i;
   }
   pinThisThread(pin_id);
}
// -------------------------------------------------------------------------------------
void pinThisThread(const u64 t_i)
{
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(t_i, &cpuset);
   pthread_t current_thread = pthread_self();
   if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0)
      throw;
}
// -------------------------------------------------------------------------------------
void printBackTrace()
{
   void* array[10];
   size_t size;
   char** strings;
   size_t i;

   size = backtrace(array, 10);
   strings = backtrace_symbols(array, size);

   for (i = 0; i < size; i++)
      printf("%s\n", strings[i]);

   free(strings);
}
// -------------------------------------------------------------------------------------
u32 CRC(const u8* src, u64 size)
{
   return CRC::Calculate(src, size, CRC::CRC_32());
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
