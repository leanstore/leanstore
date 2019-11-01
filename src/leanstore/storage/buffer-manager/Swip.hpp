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
   static const u64 swizzle_bit = u64(1) << 63;
   static const u64 unswizzle_mask = ~(u64(1) << 63);
   static_assert(swizzle_bit == 0x8000000000000000, "");
   static_assert(unswizzle_mask == 0x7FFFFFFFFFFFFFFF, "");
public:
   PID pid;
   // -------------------------------------------------------------------------------------
   Swip(u64 pid );
   template <typename T>
   Swip(T* ptr ) {
      //exchange
      pid = u64(ptr) | swizzle_bit;
   }
   Swip() : pid(0) {}
   // -------------------------------------------------------------------------------------
   bool isSwizzled();
   u64 asInteger();
   void swizzle(BufferFrame *);
   BufferFrame &getBufferFrame();
   bool operator==(const Swip &other) const
   {
      return (pid == other.pid);
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
      return hash<u64>()(k.pid);
   }
};
}
// -------------------------------------------------------------------------------------
