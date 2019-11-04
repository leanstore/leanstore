#pragma once
// -------------------------------------------------------------------------------------
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
struct BufferFrame; // Forward declaration
// -------------------------------------------------------------------------------------
struct Swip {
   // -------------------------------------------------------------------------------------
   static const u64 unswizzle_bit = u64(1) << 63;
   static const u64 unswizzle_mask = ~(u64(1) << 63);
   static_assert(unswizzle_bit == 0x8000000000000000, "");
   static_assert(unswizzle_mask == 0x7FFFFFFFFFFFFFFF, "");
public:
   u64 swizzle_value;
   // -------------------------------------------------------------------------------------
   Swip(u64 pid );
   template <typename T>
   Swip(T* ptr ) {
      //exchange
      swizzle_value = u64(ptr);
   }
   Swip() : swizzle_value(0) {}
   // -------------------------------------------------------------------------------------
   bool isSwizzled();
   u64 asInteger();
   u64 asPageID();
   void swizzle(BufferFrame *);
   BufferFrame &getBufferFrame();
   bool operator==(const Swip &other) const
   {
      return (swizzle_value == other.swizzle_value);
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
namespace std {
template<>
struct hash<leanstore::Swip> {
   size_t operator()(const leanstore::Swip &k) const
   {
      // Compute individual hash values for two data members and combine them using XOR and bit shifting
      return hash<u64>()(k.swizzle_value);
   }
};
}
// -------------------------------------------------------------------------------------
