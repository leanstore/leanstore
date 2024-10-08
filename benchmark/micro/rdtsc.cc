#include <emmintrin.h>
#include <x86intrin.h>

#include <chrono>
#include <cmath>
#include <iostream>

using TimePoint = std::chrono::time_point<std::chrono::high_resolution_clock>;

auto Rdtsc() -> uint64_t {
  uint32_t hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
}

inline auto TimePointDifference(TimePoint end, TimePoint start) -> uint64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

auto CyclesPerNs() -> double {
  double cycles_per_ns;
  double old_cycles = 0;

  while (true) {
    auto start_chrono = std::chrono::high_resolution_clock::now();
    auto start        = __rdtsc();
    _mm_sfence();
    while (true) {
      _mm_lfence();
      auto end           = __rdtsc();
      auto end_chrono    = std::chrono::high_resolution_clock::now();
      auto elapsed_nanos = TimePointDifference(end_chrono, start_chrono);
      if (elapsed_nanos > 10000000UL) {
        cycles_per_ns = static_cast<double>(end - start) / elapsed_nanos;
        break;
      }
    }
    if ((std::abs(cycles_per_ns - old_cycles) / cycles_per_ns) < 0.0001) { break; }
    old_cycles = cycles_per_ns;
  }
  return cycles_per_ns;
}

auto MeassureRdtsc(int seconds) -> std::tuple<uint64_t, uint64_t> {
  auto startChrono = std::chrono::high_resolution_clock::now();
  // auto start = Rdtsc();
  auto start = __rdtsc();
  _mm_sfence();

  while (TimePointDifference(std::chrono::high_resolution_clock::now(), startChrono) < seconds * 1000000000ull - 60) {}

  _mm_lfence();
  auto end = __rdtsc();
  // auto end = Rdtsc();
  auto endChrono = std::chrono::high_resolution_clock::now();

  return {end - start, TimePointDifference(endChrono, startChrono)};
}

int main() {
  std::cout << "start" << std::endl;

  // calibrate
  auto cali     = MeassureRdtsc(10);
  auto tsc      = std::get<0>(cali);
  auto chr      = std::get<1>(cali);
  auto tscPerNs = (double)(std::get<0>(cali) /*Hz*/) / std::get<1>(cali) /*ns*/;
  auto nsPer    = (double)(std::get<1>(cali) /*Hz*/) / std::get<0>(cali) /*ns*/;

  std::cout << "calibration done" << std::endl;

  std::cout << "readTSC diff: " << tsc << " cycles" << std::endl;
  std::cout << "chrono diff: " << chr << " ns" << std::endl;
  std::cout << "tscPerNs convert: " << tscPerNs << std::endl;
  std::cout << "nsPerTsc convert: " << nsPer << std::endl;
  std::cout << "readTSC diff converted: " << (int64_t)(tsc / tscPerNs)
            << " difference to chrono: " << tsc / tscPerNs - (int64_t)chr << " ns" << std::endl;
  std::cout << "conversion rate used waitForSeconds1 TSC cycle = " << 1 / (double)tscPerNs << " ns" << std::endl;

  std::cout << "recalibrate: " << CyclesPerNs() << std::endl;
  return 0;
}