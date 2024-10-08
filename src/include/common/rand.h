#pragma once

#include "common/typedefs.h"

#include "share_headers/db_types.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <random>
#include <utility>
#include <vector>

class MersenneTwister {
 private:
  static constexpr int NN       = 312;
  static constexpr int MM       = 156;
  static constexpr u64 MATRIX_A = 0xB5026F5AA96619E9ULL;
  static constexpr u64 UM       = 0xFFFFFFFF80000000ULL;
  static constexpr u64 LM       = 0x7FFFFFFFULL;
  u64 mt_[NN];
  int mti_;
  void Init(u64 seed);

 public:
  explicit MersenneTwister(u64 seed = 19650218ULL);
  auto Rand() -> u64;
};

class ZipfGenerator {
 private:
  double norm_c_;                 // Normalization constant
  int n_elements_;                // Number of elements
  std::vector<double> sum_prob_;  // pre calculate the sum probabilities
 public:
  ZipfGenerator(double theta, int n_elements);
  auto Rand() -> int;
  auto NoElements() -> int;
};

class RandomGenerator {
 public:
  // ATTENTION: interval [min, max)
  static auto GetRandU64(u64 min, u64 max) -> u64;
  static auto GetRandU64() -> u64;
  static thread_local std::minstd_rand prng;

  // ATTENTION: interval [min, max)
  template <typename T>
  static auto GetRand(T min, T max) -> T {
    u64 rand = GetRandU64(min, max);
    return static_cast<T>(rand);
  }

  static void GetRandString(u8 *dst, u64 size);
  static void GetRandRepetitiveString(u8 *dst, u64 rep_size, u64 size);
};

auto RandBool() -> bool;
auto Rand(Integer n) -> Integer;                                          // [0, n)
auto UniformRand(Integer low, Integer high) -> Integer;                   // [low, high]
auto UniformRandExcept(Integer low, Integer high, Integer v) -> Integer;  // [low, high]
auto RandomNumeric(Numeric min, Numeric max) -> Numeric;
auto NonUniformRand(Integer a, Integer x, Integer y, Integer c = 42) -> Integer;

template <int MaxLength>
auto RandomString(Integer min_len, Integer max_len) -> Varchar<MaxLength> {
  assert(max_len <= MaxLength);
  Integer len = Rand(max_len - min_len + 1) + min_len;
  Varchar<MaxLength> result;
  for (auto index = 0; index < len; index++) {
    Integer i = Rand(62);
    if (i < 10) {
      result.Append(48 + i);
    } else if (i < 36) {
      result.Append(64 - 10 + i);
    } else {
      result.Append(96 - 36 + i);
    }
  }
  return result;
}

template <int Length>
auto RandomNumberString() -> Varchar<Length> {
  Varchar<Length> result;
  for (auto i = 0; i < Length; i++) { result.Append(48 + Rand(10)); }
  return result;
}

template <int Length, uint8_t Low, uint8_t High>  // [low, high]
auto RandomByteArray() -> BytesPayload<Length> {
  BytesPayload<Length> result;
  for (auto i = 0; i < Length; i++) { result.value[i] = UniformRand(Low, High); }
  return result;
}

template <typename T>
auto Sample(UInteger size, const std::vector<T> &v) -> std::vector<T> {
  std::vector<T> result;
  std::sample(v.begin(), v.end(), std::back_inserter(result), size, RandomGenerator::prng);
  return result;
}