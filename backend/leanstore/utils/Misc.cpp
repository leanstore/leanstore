#include "Misc.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
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
}
}