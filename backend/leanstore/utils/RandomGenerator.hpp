#pragma once
// -------------------------------------------------------------------------------------
#include <random>
// -------------------------------------------------------------------------------------
static thread_local std::mt19937 random_generator;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace utils {
// -------------------------------------------------------------------------------------
class RandomGenerator {
public:
   // ATTENTION: open interval [min, max)
   template<typename T>
   static T getRand(T min, T max)
   {
      std::uniform_int_distribution<T> distribution(min, max - 1);
      return distribution(random_generator);
   }
   static void getRandString(u8 *dst, u64 size);
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------