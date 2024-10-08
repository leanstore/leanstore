#include "common/rand.h"

#include <atomic>
#include <cstdlib>

static std::atomic<u64> mt_counter = 0;
static thread_local MersenneTwister mt_generator;
thread_local std::minstd_rand RandomGenerator::prng = std::minstd_rand(std::random_device()());

MersenneTwister::MersenneTwister(u64 seed) : mti_(NN + 1) { Init(seed + (mt_counter++)); }

void MersenneTwister::Init(u64 seed) {
  mt_[0] = seed;
  for (mti_ = 1; mti_ < NN; mti_++) {
    mt_[mti_] = (6364136223846793005ULL * (mt_[mti_ - 1] ^ (mt_[mti_ - 1] >> 62)) + mti_);
  }
}

auto MersenneTwister::Rand() -> u64 {
  int i;
  u64 x;
  static u64 mag01[2] = {0ULL, MATRIX_A};

  if (mti_ >= NN) { /* generate NN words at one time */
    for (i = 0; i < NN - MM; i++) {
      x      = (mt_[i] & UM) | (mt_[i + 1] & LM);
      mt_[i] = mt_[i + MM] ^ (x >> 1) ^ mag01[static_cast<int>(x & 1ULL)];
    }
    for (; i < NN - 1; i++) {
      x      = (mt_[i] & UM) | (mt_[i + 1] & LM);
      mt_[i] = mt_[i + (MM - NN)] ^ (x >> 1) ^ mag01[static_cast<int>(x & 1ULL)];
    }
    x           = (mt_[NN - 1] & UM) | (mt_[0] & LM);
    mt_[NN - 1] = mt_[MM - 1] ^ (x >> 1) ^ mag01[static_cast<int>(x & 1ULL)];
    mti_        = 0;
  }

  x = mt_[mti_++];
  x ^= (x >> 29) & 0x5555555555555555ULL;
  x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
  x ^= (x << 37) & 0xFFF7EEE000000000ULL;
  x ^= (x >> 43);

  return x;
}

ZipfGenerator::ZipfGenerator(double theta, int n_elements) : n_elements_(n_elements) {
  norm_c_ = 0;
  for (auto i = 1; i <= n_elements; ++i) { norm_c_ += 1.0 / pow(static_cast<double>(i), theta); }
  norm_c_ = 1.0 / norm_c_;
  sum_prob_.reserve(n_elements + 1);
  sum_prob_[0] = 0;
  for (int i = 1; i <= n_elements; ++i) {
    sum_prob_[i] = sum_prob_[i - 1] + norm_c_ / pow(static_cast<double>(i), theta);
  }
}

auto ZipfGenerator::Rand() -> int {
  double z;

  // Pull a uniform random number (0 < z < 1)
  do { z = drand48(); } while ((z == 0) || (z == 1));

  // Map z to the value
  int low  = 1;
  int high = n_elements_;
  while (low <= high) {
    auto mid = (low + high) / 2;
    if (sum_prob_[mid] >= z) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return low;
}

auto ZipfGenerator::NoElements() -> int { return n_elements_; }

auto RandomGenerator::GetRandU64(u64 min, u64 max) -> u64 {
  u64 rand = min + (mt_generator.Rand() % (max - min));
  assert(rand < max);
  assert(rand >= min);
  return rand;
}

auto RandomGenerator::GetRandU64() -> u64 { return mt_generator.Rand(); }

void RandomGenerator::GetRandRepetitiveString(u8 *dst, u64 rep_size, u64 size) {
  if (rep_size > size) { rep_size = size; }
  GetRandString(dst, rep_size);

  for (u64 t_i = rep_size; t_i < size; t_i += rep_size) { std::memcpy(&dst[t_i], dst, std::min(rep_size, size - t_i)); }
}

void RandomGenerator::GetRandString(u8 *dst, u64 size) {
  for (u64 t_i = 0; t_i < size; t_i++) { dst[t_i] = GetRand(48, 123); }
}

// NOLINTNEXTLINE
auto RandBool() -> bool { return RandomGenerator::GetRand<bool>(0, 2); }

auto Rand(Integer n) -> Integer { return RandomGenerator::GetRand(0, n); }

auto UniformRand(Integer low, Integer high) -> Integer { return Rand(high - low + 1) + low; }

auto UniformRandExcept(Integer low, Integer high, Integer v) -> Integer {
  if (high <= low) { return low; }
  auto r = Rand(high - low) + low;
  if (r < v) { return r; }
  return (r < high) ? (r + 1) : (r - 1);
}

auto RandomNumeric(Numeric min, Numeric max) -> Numeric {
  Numeric range = (max - min);
  Numeric div   = RAND_MAX / range;
  return min + (RandomGenerator::GetRandU64() / div);
}

auto NonUniformRand(Integer a, Integer x, Integer y, Integer c) -> Integer {
  // TPC-C random is [a,b] inclusive
  // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) +
  // x return (((rnd(a + 1) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
  return (((UniformRand(0, a) | UniformRand(x, y)) + c) % (y - x + 1)) + x;
}