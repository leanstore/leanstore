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
struct SwipValue {
   // -------------------------------------------------------------------------------------
   static const u64 unswizzle_bit = u64(1) << 63;
   static const u64 unswizzle_mask = ~(u64(1) << 63);
   static_assert(unswizzle_bit == 0x8000000000000000, "");
   static_assert(unswizzle_mask == 0x7FFFFFFFFFFFFFFF, "");
public:
   u64 val;
   // -------------------------------------------------------------------------------------
   SwipValue(u64 pid)
           : val(pid | unswizzle_bit)
   {
   }
   template<typename T2>
   SwipValue(T2* ptr ) {
      //exchange
      val = u64(ptr);
   }
   SwipValue() : val(0) {}
   // -------------------------------------------------------------------------------------
   bool operator==(const SwipValue &other) const
   {
      return (val == other.val);
   }
   // -------------------------------------------------------------------------------------
   bool isSwizzled()
   {
      return (val & unswizzle_mask);
   }
   u64 asInteger() { return val; }
   u64 asPageID() { return val & unswizzle_mask; }
   void swizzle(BufferFrame *bf)
   {
      val = u64(bf);
   }
   void unswizzle(PID pid) {
      val = pid | unswizzle_bit;
   }
   BufferFrame &getBufferFrame()
   {
      return *reinterpret_cast<BufferFrame *>(val);
   }
};
// -------------------------------------------------------------------------------------
template<typename T>
struct Swip {
   SwipValue value;
   template<typename... Args>
   Swip(Args &&... args) : value(std::forward<Args>(args)...) {
   }
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
