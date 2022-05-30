#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
// -------------------------------------------------------------------------------------
using Integer = s32;
using Timestamp = u64;
using Numeric = double;
static constexpr Integer minInteger = std::numeric_limits<int>::min();
// -------------------------------------------------------------------------------------
template <int maxLength>
struct Varchar {
   int16_t length;
   char data[maxLength] = {0};  // not '\0' terminated

   Varchar() : length(0) {}
   Varchar(const char* str)
   {
      int l = strlen(str);
      assert(l <= maxLength);
      length = l;
      memcpy(data, str, l);
   }
   template <int otherMaxLength>
   Varchar(const Varchar<otherMaxLength>& other)
   {
      assert(other.length <= maxLength);
      length = other.length;
      memcpy(data, other.data, length);
   }

   void append(char x)
   {
      assert(length < maxLength);
      data[length++] = x;
   };

   std::string toString() const { return std::string(data, length); };

   template <int otherMaxLength>
   Varchar<maxLength> operator||(const Varchar<otherMaxLength>& other) const
   {
      Varchar<maxLength> tmp;
      assert((static_cast<int32_t>(length) + other.length) <= maxLength);
      tmp.length = length + other.length;
      memcpy(tmp.data, data, length);
      memcpy(tmp.data + length, other.data, other.length);
      return tmp;
   }

   bool operator==(const Varchar<maxLength>& other) const { return (length == other.length) && (memcmp(data, other.data, length) == 0); }

   bool operator!=(const Varchar<maxLength>& other) const { return !operator==(other); }

   bool operator<(const Varchar<maxLength>& other) const
   {
      int cmp = memcmp(data, other.data, (length < other.length) ? length : other.length);
      if (cmp)
         return cmp < 0;
      else
         return length < other.length;
   }
};
// -------------------------------------------------------------------------------------
// Fold functions convert integers to a lexicographical comparable format
unsigned fold(u8* writer, const Integer& x)
{
   *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned fold(u8* writer, const s64& x)
{
   *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned fold(u8* writer, const u64& x)
{
   *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
template <int len>
unsigned fold(u8* writer, const Varchar<len>& x)
{
   memcpy(writer, x.data, x.length);
   writer[x.length] = 0;
   return x.length + 1;
}
// -------------------------------------------------------------------------------------
unsigned unfold(const u8* input, Integer& x)
{
   x = __builtin_bswap32(*reinterpret_cast<const u32*>(input)) ^ (1ul << 31);
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned unfold(const u8* input, s64& x)
{
   x = __builtin_bswap64(*reinterpret_cast<const u64*>(input)) ^ (1ul << 63);
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
unsigned unfold(const u8* input, u64& x)
{
   x = __builtin_bswap64(*reinterpret_cast<const u64*>(input));
   return sizeof(x);
}
// -------------------------------------------------------------------------------------
template <int len>
unsigned unfold(const u8* input, Varchar<len>& x)
{
   int l = strlen(reinterpret_cast<const char*>(input));
   assert(l <= len);
   memcpy(x.data, input, l);
   x.length = l;
   return l + 1;
}
