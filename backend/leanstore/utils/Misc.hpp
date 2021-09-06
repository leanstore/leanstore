#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <chrono>
#include <cmath>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
u32 getBitsNeeded(u64 input);
// -------------------------------------------------------------------------------------
double calculateMTPS(std::chrono::high_resolution_clock::time_point begin, std::chrono::high_resolution_clock::time_point end, u64 factor);
// -------------------------------------------------------------------------------------
void pinThisThreadRome();
void pinThisThreadRome(const u64 t_i);
void pinThisThread(const u64 t_i);
// -------------------------------------------------------------------------------------
void printBackTrace();
// -------------------------------------------------------------------------------------
inline u64 upAlign(u64 x)
{
   return (x + 511) & ~511ul;
}
// -------------------------------------------------------------------------------------
inline u64 downAlign(u64 x)
{
   return x - (x & 511);
}
// -------------------------------------------------------------------------------------
u32 CRC(const u8* src, u64 size);
// -------------------------------------------------------------------------------------
// Fold functions convert integers to a lexicographical comparable format
template <typename T>
u64 voidFoldOrUnfold(u8* writer, const T& x)
{
   *reinterpret_cast<T*>(writer) = x;
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const u64& x)
{
   return voidFoldOrUnfold<u64>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const u32& x)
{
   return voidFoldOrUnfold<u32>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const u16& x)
{
   return voidFoldOrUnfold<u16>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const u8& x)
{
   return voidFoldOrUnfold<u8>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(u8* writer, const u64& x)
{
   return voidFoldOrUnfold<u64>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(u8* writer, const u32& x)
{
   return voidFoldOrUnfold<u32>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(u8* writer, const u16& x)
{
   return voidFoldOrUnfold<u16>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(u8* writer, const u8& x)
{
   return voidFoldOrUnfold<u8>(writer, x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const s32& x)
{
   *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(const u8* input, s32& x)
{
   x = __builtin_bswap32(*reinterpret_cast<const u32*>(input)) ^ (1ul << 31);
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
inline u64 fold(u8* writer, const s64& x)
{
   *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
inline u64 unfold(const u8* input, s64& x)
{
   x = __builtin_bswap64(*reinterpret_cast<const u64*>(input)) ^ (1ul << 63);
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
