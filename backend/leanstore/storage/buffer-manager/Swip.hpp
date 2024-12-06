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
   static const u64 evicted_bit = u64(1) << 63;
   static const u64 evicted_mask = ~(u64(1) << 63);
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
   bool isHOT() { return !isEVICTED();}
   bool isEVICTED() { return pid & evicted_bit; }
   // -------------------------------------------------------------------------------------
   u64 asPageID() { return pid & evicted_mask; }
   BufferFrame& asBufferFrame() { return *bf; }
   BufferFrame& asBufferFrameMasked() { return *reinterpret_cast<BufferFrame*>(pid & hot_mask); }
   u64 raw() const { return pid; }
   // -------------------------------------------------------------------------------------
   template <typename T2>
   void setBF(T2* bf)
   {
      this->bf = bf;
   }
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
