#pragma once
#include "GenericRandom.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <random>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// A Zipf distributed random number generator
//  - Generates [0, N) randam integer
//
// http://stackoverflow.com/questions/9983239/how-to-generate-zipf-distributed-numbers-efficiently
// http://coderepos.org/share/browser/lang/cplusplus/boost-supplement/trunk/boost_supplement/random/zipf_distribution.hpp?rev=5908
// http://en.wikipedia.org/wiki/Zipf-Mandelbrot_law.
// -------------------------------------------------------------------------------------
class ZipfRandom : public GenericRandom
{
  // zipf distribution
  using dist_type = std::discrete_distribution<int>;
  dist_type zipfdist;
  dist_type MakeDistribution(uint64_t N, double factor, uint64_t seed, bool shuffle, double shift);

  // engine
  std::mt19937 generator;

 public:
  const static uint64_t kDefaultSeed = 88172645463325252ull;

  // N: the zipf distribution will create values between [0, N] with N inclusive
  // factor: how zipfy is the distribution. Higher values mean more skew
  // seed: the seed for the random number generator
  // shuffle: when not shuffled 0 is most likely, then 1, then 2 ..
  // shift: no idea, just leave it zero
  ZipfRandom(uint64_t N, double factor, uint64_t seed = kDefaultSeed, bool shuffle = true, double shift = 0);

  uint64_t rand();
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------