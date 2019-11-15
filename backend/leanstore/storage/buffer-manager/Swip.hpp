#pragma once
// -------------------------------------------------------------------------------------
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
struct BufferFrame; // Forward declaration
// -------------------------------------------------------------------------------------
using SwipValue = u64;
template<typename T>
class Swip {
   // -------------------------------------------------------------------------------------
   static const u64 unswizzle_bit = u64(1) << 63;
   static const u64 unswizzle_mask = ~(u64(1) << 63);
   static_assert(unswizzle_bit == 0x8000000000000000, "");
   static_assert(unswizzle_mask == 0x7FFFFFFFFFFFFFFF, "");
public:
   union{
      u64 pid;
      BufferFrame *bf;
   };
   // -------------------------------------------------------------------------------------
   Swip() = default;
   Swip(BufferFrame *bf) : bf(bf) {}
   template<typename T2>
   Swip(Swip<T2> &other) : pid(other.pid) {}
   // -------------------------------------------------------------------------------------
   bool operator==(const Swip &other) const
   {
      return (pid == other.val);
   }
   // -------------------------------------------------------------------------------------
   bool isSwizzled()
   {
      return !(pid & unswizzle_bit);
   }
   u64 asPageID() { return pid & unswizzle_mask; }
   BufferFrame& asBufferFrame() {
      return *bf;
   }
   // -------------------------------------------------------------------------------------
   template<typename T2>
   void swizzle(T2 *bf)
   {
      this->bf = bf;
   }
   void unswizzle(PID pid) {
      this->pid = pid | unswizzle_bit;
   }
   // -------------------------------------------------------------------------------------
   template<typename T2>
   Swip<T2> &cast() {
      return *reinterpret_cast<Swip<T2>*>(this);
   }
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
