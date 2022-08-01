//
// Created by dev on 29.07.22.
//
#include <immintrin.h>
#include "Units.hpp"


struct U8 {
   union {
      __m256i reg;
      u32 entry[8];
   };

   U8(u32 x) { reg = _mm256_set1_epi32(x); }
   U8(u32 x0, u32 x1, u32 x2, u32 x3, u32 x4, u32 x5, u32 x6, u32 x7) { reg = _mm256_setr_epi32(x0, x1, x2, x3, x4, x5, x6, x7); }
   U8(const void* ptr) { reg = _mm256_loadu_si256((const __m256i_u*)ptr); }
   U8(__m256i reg) : reg(reg) {}

   operator __m256i() { return reg; }

   friend std::ostream& operator<< (std::ostream& stream, const U8& v) {
      for (auto& e : v.entry)
         stream << e << ",";
      return stream;
   }
};

inline U8 operator-(const U8& a, const U8& b) { return _mm256_sub_epi32(a.reg, b.reg); }

struct F8 {
   union {
      __m256 reg;
      float entry[8];
   };

   F8(float x) { reg = _mm256_set1_ps(x); }
   F8(float x0, float x1, float x2, float x3, float x4, float x5, float x6, float x7) { reg = _mm256_setr_ps(x0, x1, x2, x3, x4, x5, x6, x7); }
   F8(const void* ptr) { reg = _mm256_loadu_ps((const float*)ptr); }
   F8(__m256 reg) : reg(reg) {}
   F8(U8 x) : reg(_mm256_cvtepi32_ps(x.reg)) {}

   operator __m256() { return reg; }

   friend std::ostream& operator<< (std::ostream& stream, const F8& v) {
      for (auto& e : v.entry)
         stream << e << ",";
      return stream;
   }
};

inline F8 operator/(const F8& a, const F8& b) { return _mm256_div_ps(a.reg, b.reg); }

inline float minn(float a, float b) { return a<b ? a : b; }
inline float maxx(float a, float b) { return a>b ? a : b; }

inline float maxV(F8 x) {
   F8 r1(_mm256_permute2f128_ps(x, x, 1));
   F8 r2(_mm256_max_ps(x, r1));
   F8 r3(_mm256_permute_ps(r2, 14));
   F8 r4(_mm256_max_ps(r2, r3));
   return maxx(r4.entry[0], r4.entry[1]);
}
