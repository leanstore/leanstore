#pragma once
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <random>
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
class MersenneTwister
{
  private:
   static const int NN = 312;
   static const int MM = 156;
   static const uint64_t MATRIX_A = 0xB5026F5AA96619E9ULL;
   static const uint64_t UM = 0xFFFFFFFF80000000ULL;
   static const uint64_t LM = 0x7FFFFFFFULL;
   uint64_t mt[NN];
   int mti;
   void init(uint64_t seed);

  public:
   MersenneTwister(uint64_t seed = 19650218ULL);
   uint64_t rnd();
};
}  // namespace utils
}  // namespace leanstore
// -------------------------------------------------------------------------------------
static thread_local leanstore::utils::MersenneTwister mt_generator;
static thread_local std::mt19937 random_generator;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
class RandomGenerator
{
  public:
   // ATTENTION: open interval [min, max)
   static u64 getRandU64(u64 min, u64 max)
   {
      u64 rand = min + (mt_generator.rnd() % (max - min));
      assert(rand < max);
      assert(rand >= min);
      return rand;
   }
   static u64 getRandU64() { return mt_generator.rnd(); }
   static u64 getRandU64STD(u64 min, u64 max)
   {
      std::uniform_int_distribution<u64> distribution(min, max - 1);
      return distribution(random_generator);
   }
   template <typename T>
   static inline T getRand(T min, T max)
   {
      u64 rand = getRandU64(min, max);
      return static_cast<T>(rand);
   }
   static void getRandString(u8* dst, u64 size);
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------