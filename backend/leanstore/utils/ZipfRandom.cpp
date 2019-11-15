#include "ZipfRandom.hpp"
#include <algorithm>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
ZipfRandom::dist_type ZipfRandom::MakeDistribution(uint64_t N, double factor, uint64_t seed, bool shuffle, double shift)
{
   std::vector<double> buffer(N + 1);
   for (unsigned rank = 1; rank<=N; ++rank) {
      buffer[rank] = std::pow(rank + shift, -factor);
   }

   if (shuffle) {
      std::shuffle(buffer.begin() + 1, buffer.end(), std::mt19937(seed));
   }

   return ZipfRandom::dist_type(buffer.begin() + 1, buffer.end());
}
// -------------------------------------------------------------------------------------
ZipfRandom::ZipfRandom(uint64_t N, double factor, uint64_t seed, bool shuffle, double shift)
        : zipfdist(MakeDistribution(N, factor, seed, shuffle, shift))
          , generator(seed)
{
}
// -------------------------------------------------------------------------------------
uint64_t ZipfRandom::rand()
{
   return zipfdist(generator);
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------