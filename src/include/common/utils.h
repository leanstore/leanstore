#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/env.h"

#include <sys/mman.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>

#include <linux/exmap.h>

#ifdef __x86_64__
#include <immintrin.h>
#endif

namespace leanstore {

auto AllocHuge(size_t size) -> void *;
auto HashFn(u64 k) -> u64;
auto ComputeCRC(const u8 *src, u64 size) -> u32;
auto UpAlign(u64 x) -> u64;
auto DownAlign(u64 x) -> u64;
void PinThisThread(wid_t t_i);
auto IsAligned(u64 align_size, const void *p, size_t p_size = 0) -> bool;
auto ExmapAction(int exmapfd, exmap_opcode op, leng_t len) -> int;

void AsmBarrier();
void AsmYield([[maybe_unused]] u64 counter = 0);

template <class T>
auto LoadUnaligned(void *p) -> T;

template <typename T>
void UpdateMax(std::atomic<T> &atomic_val, T value);

template <typename T>
constexpr auto Square(T a) -> T {
  return a * a;
}

template <typename T>
constexpr auto Power(T a, size_t n) -> T {
  return n == 0 ? 1 : Square(Power(a, n / 2)) * (n % 2 == 0 ? 1 : a);
}

template <typename T, std::size_t N>
constexpr auto ArraySum(const std::array<T, N> array) -> T {
  T sum = 0;
  for (std::size_t i = 0; i < N; i++) { sum += array[i]; }
  return sum;
};

}  // namespace leanstore