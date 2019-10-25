#pragma once
// -------------------------------------------------------------------------------------
#include <random>
// -------------------------------------------------------------------------------------
class RandomGenerator {
public:
   static thread_local std::mt19937 random_generator;
   template<typename T>
   static T getRand(T min, T max) {
       std::uniform_int_distribution<int> distribution(min,max);
       return distribution(random_generator);
   }
};
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
