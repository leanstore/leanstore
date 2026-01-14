#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "leanstore/env.h"

#include "liburing.h"

#include <sys/mman.h>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <functional>
#include <numeric>
#include <string>
#include <type_traits>

#ifdef __x86_64__
#include <immintrin.h>
#endif

namespace leanstore {

// -------------------------------------------------------------------------------------
/* misc utilities */
auto HashFn(u64 k) -> u64;
auto ComputeCRC(const u8 *src, u64 size) -> u32;
void WarningMessage(const std::string &msg);
void UringPoll(struct io_uring *ring, u32 submit_cnt);
void UringSubmit(struct io_uring *ring, u32 submit_cnt, const std::function<void()> &fn = {});

template <typename T>
void WriteSequenceToFile(const T &sequence, u32 sample_ratio, const std::string &filename);
auto ListFilesWithExt(const std::string &directory, const std::string &ext) -> std::vector<std::string>;

template <typename T, typename... Rest>
void HashCombine(std::size_t &seed, const T &v, const Rest &...rest) {
  seed ^= std::hash<T>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  (HashCombine(seed, rest), ...);
}

// -------------------------------------------------------------------------------------
/* linux short-cut */
auto AllocHuge(size_t size) -> void *;
void AsmBarrier();
void AsmYield([[maybe_unused]] u64 counter = 0);
auto PageTableSize() -> std::string;
void PinThisThread(wid_t t_i);
auto StorageCapacity(const char *path) -> u64;

// -------------------------------------------------------------------------------------
/* alignment utilities */
template <class T>
auto LoadUnaligned(void *p) -> T;
auto UpAlign(u64 x, u64 align_size) -> u64;
auto DownAlign(u64 x, u64 align_size) -> u64;
auto IsAligned(u64 align_size, const void *p, size_t p_size = 0) -> bool;

template <typename T>
struct AlignedDeleter {
  void operator()(T *ptr) { free(ptr); }
};

template <u32 Alignment = BLK_BLOCK_SIZE>
auto AlignedPtr(u64 size) {
  return std::unique_ptr<u8, AlignedDeleter<u8>>(static_cast<u8 *>(std::aligned_alloc(Alignment, sizeof(u8) * size)));
}

// -------------------------------------------------------------------------------------
/* atomic utilities */
template <typename T>
void UpdateMax(std::atomic<T> &atomic_val, T value);

template <typename T>
void UpdateMin(std::atomic<T> &atomic_val, T value);

// -------------------------------------------------------------------------------------
/* constexpr utilities */
template <typename Container>
constexpr auto Percentile(Container &v, double percent) {
  if (v.empty()) { return 0UL; }
  auto nth = v.size() * percent / 100;
  std::nth_element(v.begin(), v.begin() + nth, v.end());
  return v[nth];
}

template <typename Container>
constexpr auto Average(const Container &v) -> double {
  if (v.empty()) { return 0; }
  return static_cast<double>(std::reduce(v.begin(), v.end())) / v.size();
}

template <typename T>
constexpr auto BitLength(T a) -> int {
  assert(a > 0);
  return std::bit_width(std::max(a - 1, static_cast<T>(1)));
}

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

template <typename T>
constexpr auto Ceil(double f) -> T {
  const T i = static_cast<T>(f);
  return f > i ? i + 1 : i;
}

template <typename E>
constexpr auto ToUnderlying(E e) noexcept {
  return static_cast<std::underlying_type_t<E>>(e);
}

}  // namespace leanstore