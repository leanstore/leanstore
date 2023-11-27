#pragma once

#include "share_headers/db_types.h"

#include <atomic>
#include <cmath>
#include <cstdlib>
#include <vector>

class MersenneTwister {
 private:
  static constexpr int NN            = 312;
  static constexpr int MM            = 156;
  static constexpr uint64_t MATRIX_A = 0xB5026F5AA96619E9ULL;
  static constexpr uint64_t UM       = 0xFFFFFFFF80000000ULL;
  static constexpr uint64_t LM       = 0x7FFFFFFFULL;
  uint64_t mt_[NN];
  int mti_;
  void Init(uint64_t seed);

 public:
  explicit MersenneTwister(uint64_t seed = 19650218ULL);
  auto Rand() -> uint64_t;
};

class ZipfGenerator {
 private:
  double norm_c_;                 // Normalization constant
  int n_elements_;                // Number of elements
  std::vector<double> sum_prob_;  // pre calculate the sum probabilities
 public:
  ZipfGenerator(double theta, int n_elements);
  auto Rand() -> int;
};

class RandomGenerator {
 public:
  // ATTENTION: open interval [min, max)
  static auto GetRandU64(uint64_t min, uint64_t max) -> uint64_t;
  static auto GetRandU64() -> uint64_t;

  template <typename T>
  static inline auto GetRand(T min, T max) -> T;
  static void GetRandString(uint8_t *dst, uint64_t size);
  static void GetRandRepetitiveString(uint8_t *dst, uint64_t rep_size, uint64_t size);
};

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
  Varchar<16> result;
  for (Integer i = 0; i < Length; i++) { result.Append(48 + Rand(10)); }
  return result;
}