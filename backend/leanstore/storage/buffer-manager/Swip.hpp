#pragma once
// -------------------------------------------------------------------------------------
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct BufferFrame;  // Forward declaration
// -------------------------------------------------------------------------------------
template <typename T>
class Swip
{
   // -------------------------------------------------------------------------------------
   // 1xxxxxxxxxxxx evicted, 01xxxxxxxxxxx cooling, 00xxxxxxxxxxx hot
   // static const u64 evicted_bit = MSB;
   // static const u64 evicted_mask = MSB_MASK;
   // static const u64 cool_bit = MSB2;
   // static const u64 cool_mask = MSB2_MASK;
   // static const u64 hot_mask = ~(MSB | MSB2);
   // static_assert(evicted_bit == 0x8000000000000000, "");
   // static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");
   // static_assert(hot_mask == 0x3FFFFFFFFFFFFFFF, "");
   static const u64 evicted_bit = u64(1) << 63;
   static const u64 evicted_mask = ~(u64(1) << 63);
   static const u64 cool_bit = u64(1) << 62;
   static const u64 cool_mask = ~(u64(1) << 62);
   static const u64 hot_mask = ~(u64(3) << 62);
   static_assert(evicted_bit == 0x8000000000000000, "");
   static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");
   static_assert(hot_mask == 0x3FFFFFFFFFFFFFFF, "");
  public:
   union {
      u64 pid;
      BufferFrame* bf;
   };
   // -------------------------------------------------------------------------------------
   Swip() = default;
   Swip(BufferFrame* bf) : bf(bf) {}
   template <typename T2>
   Swip(Swip<T2>& other) : pid(other.pid)
   {
   }
   // -------------------------------------------------------------------------------------
   bool operator==(const Swip& other) const { return (raw() == other.raw()); }
   // -------------------------------------------------------------------------------------
   bool isHOT() { return (pid & (evicted_bit | cool_bit)) == 0; }
   bool isCOOL() { return pid & cool_bit; }
   bool isEVICTED() { return pid & evicted_bit; }
   // -------------------------------------------------------------------------------------
   u64 asPageID() { return pid & evicted_mask; }
   BufferFrame& bfRef() { return *bf; }
   BufferFrame* bfPtr() { return bf; }
   BufferFrame* bfPtrAsHot() { return reinterpret_cast<BufferFrame*>(pid & hot_mask); }
   BufferFrame& bfRefAsHot() { return *bfPtrAsHot(); }
   u64 raw() const { return pid; }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   void warm(T2* bf)
   {
      this->bf = bf;
   }
   void warm()
   {
      assert(isCOOL());
      this->pid = pid & ~cool_bit;
   }
   // -------------------------------------------------------------------------------------
   void cool() { this->pid = pid | cool_bit; }
   // -------------------------------------------------------------------------------------
   void evict(PID pid) { this->pid = pid | evicted_bit; }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   Swip<T2>& cast()
   {
      return *reinterpret_cast<Swip<T2>*>(this);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
