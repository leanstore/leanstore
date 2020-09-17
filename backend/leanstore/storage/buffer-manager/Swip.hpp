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
namespace buffermanager
{
// -------------------------------------------------------------------------------------
struct BufferFrame;  // Forward declaration
// -------------------------------------------------------------------------------------
template <typename T>
class Swip
{
  // -------------------------------------------------------------------------------------
  // 1xxxxxxxxxxxx unswizzled, 01xxxxxxxxxxx cooled, 00xxxxxxxxxxx swizzled
  // Swizzle: HOT, Cool: in-memory but cool, Unswizzle: Evicted (cold)
  static const u64 evicted_bit = u64(1) << 63;
  static const u64 evicted_mask = ~(u64(1) << 63);
  static const u64 cooling_bit = u64(1) << 62;
  static const u64 cooling_mask = ~(u64(1) << 62);
  static_assert(evicted_bit == 0x8000000000000000, "");
  static_assert(evicted_mask == 0x7FFFFFFFFFFFFFFF, "");

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
  bool isHOT() { return (pid & (evicted_bit | cooling_bit)) == 0; }
  bool isCOLD() { return pid & cooling_bit; }
  bool isEVICTED() { return pid & evicted_bit; }
  // -------------------------------------------------------------------------------------
  u64 asPageID() { return pid & evicted_mask; }
  BufferFrame& asBufferFrame() { return *bf; }
  u64 raw() const { return pid; }
  // -------------------------------------------------------------------------------------
  template <typename T2>
  void swizzle(T2* bf)
  {
    this->bf = bf;
  }
  void evict(PID pid) { this->pid = pid | evicted_bit; }
  // -------------------------------------------------------------------------------------
  void cool() { this->pid = pid | cooling_bit; }
  void warm() { this->pid = pid & ~cooling_bit; }
  // -------------------------------------------------------------------------------------
  template <typename T2>
  Swip<T2>& cast()
  {
    return *reinterpret_cast<Swip<T2>*>(this);
  }
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
