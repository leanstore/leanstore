#include "ZipfGenerator.hpp"

#include "RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <algorithm>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
ZipfGenerator::ZipfGenerator(u64 ex_n, double theta) : n(ex_n - 1), theta(theta)
{
   alpha = 1.0 / (1.0 - theta);
   zetan = zeta(n, theta);
   eta = (1.0 - std::pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan);
}
// -------------------------------------------------------------------------------------
double ZipfGenerator::zeta(u64 n, double theta)
{
   double ans = 0;
   for (u64 i = 1; i <= n; i++)
      ans += std::pow(1.0 / n, theta);
   return ans;
}
// -------------------------------------------------------------------------------------
uint64_t ZipfGenerator::rand()
{
   double constant = 1000000000000000000.0;
   u64 i = RandomGenerator::getRandU64(0, 1000000000000000001);
   double u = static_cast<double>(i) / constant;
   // return (u64)u;
   double uz = u * zetan;
   if (uz < 1) {
      return 1;
   }
   if (uz < (1 + std::pow(0.5, theta)))
      return 2;
   u64 ret = 1 + (long)(n * pow(eta * u - eta + 1, alpha));
   return ret;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
