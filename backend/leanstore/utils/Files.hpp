#pragma once
// -------------------------------------------------------------------------------------
#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// Polyfill for offsetof for gcc
#ifndef offsetof
#define offsetof(type, member) __builtin_offsetof(type, member)
#endif
// -------------------------------------------------------------------------------------
// Create a file with count entries created by the given factory function
bool CreateTestFile(const std::string& fileName, uint64_t count, std::function<int32_t(int32_t)> factory);
// -------------------------------------------------------------------------------------
// Read a file and invoke callback function on every entry
bool ForeachInFile(const std::string& fileName, std::function<void(uint32_t)> callback);
// -------------------------------------------------------------------------------------
// Create a file with random(not set) data of given size
bool CreateDirectory(const std::string& directory_name);
bool CreateFile(const std::string& fileName, const uint64_t bytes);
bool CreateFile(const std::string& fileName, const std::string& content);
// -------------------------------------------------------------------------------------
// Delete the given file if it exists
void DeleteFile(const std::string& fileName);
// -------------------------------------------------------------------------------------
// Reads the length of the file
uint64_t GetFileLength(const std::string& fileName);
// -------------------------------------------------------------------------------------
// Checks if the given file exists
bool fileExists(const std::string& fileName);
bool directoryExists(const std::string& fileName);
bool pathExists(const std::string& fileName);
// -------------------------------------------------------------------------------------
// Loads the complete file into memory
std::string LoadFileToMemory(const std::string& fileName);
// -------------------------------------------------------------------------------------
inline uint64_t FieldOffset(void* base, void* field)
{
   assert(base <= field);
   return (uintptr_t)field - (uintptr_t)base;
}
// -------------------------------------------------------------------------------------
// Converts the given time in ns into a usable unit depending on its size
std::string FormatTime(std::chrono::nanoseconds ns, uint32_t precision);
// -------------------------------------------------------------------------------------
// Check alignment
template <uint32_t byteCount>
bool IsAlignedAt(const void* ptr)
{
   return ((uint64_t)ptr) % byteCount == 0;
}
uint8_t* AlignedAlloc(uint64_t alignment, uint64_t size);
// -------------------------------------------------------------------------------------
// Converts the data to and from hex string
const std::string DataToHex(uint8_t* data, uint32_t len, bool spaces = false);
const std::string StringToHex(const std::string& str, bool spaces = false);
const std::vector<uint8_t> HexToData(const std::string& str, bool spaces = false);
const std::string HexToString(const std::string& str, bool spaces = false);
// -------------------------------------------------------------------------------------
// Converts the string to a lower/upper case string
std::string ToLower(const std::string& str);
std::string ToUpper(const std::string& str);
bool IsLower(const std::string& str);
bool IsUpper(const std::string& str);
// -------------------------------------------------------------------------------------
// Just returns the number of nano seconds since epoch
uint64_t TimeSinceEpoch();
// -------------------------------------------------------------------------------------
// Generate a vector with random tuple ids
// @param locality: a value between 0 and 100, indicating how many tuples should
// be on the same page: 0 -> uniform distribution and 100 -> all tuples on the
// same page
// @param count: how many random tuple ids should be generated
// @param repeat: the value which is used locality/100 % of the time
// @param generator: called for all other values
template <class T>
std::vector<T> GenerateNonUniformDistribution(uint32_t locality, uint32_t count, T repeat, std::function<T(void)> generator)
{
   assert(0 <= locality && locality <= 100);
   uint32_t sameTupleCount = count / 100 * locality;
   std::vector<T> result(count);
   for (uint32_t i = 0; i < count - sameTupleCount; ++i) {
      result[i] = generator();
   }
   for (uint32_t i = count - sameTupleCount; i < count; ++i) {
      result[i] = repeat;
   }
   std::random_shuffle(result.begin(), result.end());
   return result;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
   // -------------------------------------------------------------------------------------