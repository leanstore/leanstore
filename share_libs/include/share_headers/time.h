#pragma once

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <x86intrin.h>

namespace tsctime {

inline auto ReadTSC() -> uint64_t {
  uint32_t hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return static_cast<uint64_t>(lo)|(static_cast<uint64_t>(hi)<<32);
}

inline auto CyclesPerNs() -> double {
  double cycles_per_ns;
  double old_cycles = 0;

  while (true) {
    auto start_chrono = std::chrono::high_resolution_clock::now();
    auto start        = ReadTSC();
    _mm_sfence();
    while (true) {
      _mm_lfence();
      auto end           = ReadTSC();
      auto end_chrono    = std::chrono::high_resolution_clock::now();
      auto elapsed_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end_chrono - start_chrono).count();
      if (elapsed_nanos > 10000000L) {
        cycles_per_ns = static_cast<double>(end - start) / elapsed_nanos;
        break;
      }
    }
    if ((std::abs(cycles_per_ns - old_cycles) / cycles_per_ns) < 0.00001) { break; }
    old_cycles = cycles_per_ns;
  }
  return cycles_per_ns;
}


static const double TSC_PER_NS = CyclesPerNs();

inline auto TscToNs(uint64_t tsc) -> uint64_t { return tsc / TSC_PER_NS; }

inline auto TscDifferenceNs(uint64_t start, uint64_t end) -> uint64_t {
	return (end - start) / TSC_PER_NS;
}

inline auto TscDifferenceUs(uint64_t start, uint64_t end) -> uint64_t {
	return TscDifferenceNs(end, start) / 1000;
}

inline auto TscDifferenceMs(uint64_t start, uint64_t end) -> uint64_t {
	return TscDifferenceNs(end, start) / 1000000;
}

inline auto TscDifferenceS(uint64_t start, uint64_t end) -> uint64_t {
	return TscDifferenceNs(end, start) / 1000000000UL;
}

} // namespace time