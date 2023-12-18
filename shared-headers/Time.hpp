#pragma once

#include "Units.hpp"

#include <cstdint>
#include <iostream>
#include <chrono>
#include <x86intrin.h>

namespace mean {
// -------------------------------------------------------------------------------------
inline uint64_t readTSC() {
	const uint64_t tsc = __rdtsc();
	return tsc;
}
inline uint64_t readTSCfenced() {
   _mm_mfence();
	const uint64_t tsc = __rdtsc();
   _mm_mfence();
	return tsc;
}
inline uint64_t tscDifferenceNs(uint64_t end, uint64_t start) {
	return (end - start) * 0.500942; // = 2 GHz
}
inline uint64_t tscDifferenceUs(uint64_t end, uint64_t start) {
	return tscDifferenceNs(end, start) / 1000;
}
inline uint64_t tscDifferenceMs(uint64_t end, uint64_t start) {
	return tscDifferenceNs(end, start) / 1000000;
}
inline uint64_t tscDifferenceS(uint64_t end, uint64_t start) {
	return tscDifferenceNs(end, start) / 1000000000ull;
}
inline uint64_t nsToTSC(uint64_t ns) {
	return ns*1.9962386720; // = 2 GHz
}
static auto _staticStartTsc = readTSC();
inline u64 nanoFromTsc(uint64_t tp) {
	return tscDifferenceNs(tp, _staticStartTsc); 
}
using TimePoint = std::chrono::time_point<std::chrono::high_resolution_clock>;
inline TimePoint getTimePoint() {
	return std::chrono::high_resolution_clock::now();	
}
inline uint64_t timePointDifference(TimePoint end, TimePoint start) {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}
inline uint64_t timePointDifferenceUs(TimePoint end, TimePoint start) {
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}
inline uint64_t timePointDifferenceMs(TimePoint end, TimePoint start) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}
static auto _staticStartTimingPoint = getTimePoint();
inline u64 nanoFromTimePoint(TimePoint tp) {
	return timePointDifference(tp, _staticStartTimingPoint); 
}
inline float getSeconds() {
	auto tp = getTimePoint();
	return nanoFromTimePoint(tp) * NANO; 
}
inline float getRoundSeconds() {
	static auto last = getTimePoint();
	auto now = getTimePoint();
	auto diff = timePointDifference(now, last) * NANO;
	last = now;
	return diff; 
}

// -------------------------------------------------------------------------------------
} // namespace mean
// -------------------------------------------------------------------------------------
