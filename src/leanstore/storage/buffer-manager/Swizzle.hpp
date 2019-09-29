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
struct Swizzle {
   // -------------------------------------------------------------------------------------
   static const u64 swizzle_bit = u64(1) << 63;
   static const u64 unswizzle_mask = ~(u64(1) << 63);
   static_assert(swizzle_bit == 0x8000000000000000, "");
   static_assert(unswizzle_mask == 0x7FFFFFFFFFFFFFFF, "");
public:
   std::atomic<PID> pid;
   // -------------------------------------------------------------------------------------
   Swizzle(u64 pid );
   // -------------------------------------------------------------------------------------
   bool isSwizzled();
   u64 asInteger();
   void swizzle(BufferFrame *);
   BufferFrame &getBufferFrame();
   bool operator==(const Swizzle &other) const
   {
      return (pid == other.pid);
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
namespace std {
template<>
struct hash<leanstore::Swizzle> {
   size_t operator()(const leanstore::Swizzle &k) const
   {
      // Compute individual hash values for two data members and combine them using XOR and bit shifting
      return hash<u64>()(k.pid);
   }
};
}
// -------------------------------------------------------------------------------------
