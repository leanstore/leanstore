#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
#include <list>
#include <memory>
#include <vector>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
// -------------------------------------------------------------------------------------
// Makes sense for single-writer multiple-readers pattern where the write is short
template <typename T>
class OptimisticSpinStruct
{
  public:
   T current_value;
   std::atomic<u64> optimistic_latch;

  public:
   T getSync()
   {
   retry : {
      u64 version = optimistic_latch.load();
      while (version & LSB) {
         version = optimistic_latch.load();
      }
      T copy = current_value;
      if (version != optimistic_latch.load()) {
         goto retry;
      }
      copy.version = version;
      return copy;
   }
   }
   // Only writer should call this
   T getNoSync() { return current_value; }
   void pushSync(T next_value)
   {
      optimistic_latch.store(optimistic_latch.load() + LSB, std::memory_order_release);
      current_value = next_value;
      optimistic_latch.store(optimistic_latch.load() + LSB, std::memory_order_release);
   }
   // -------------------------------------------------------------------------------------
   template <class AttributeType>
   void updateAttribute(AttributeType T::*a, AttributeType new_value)
   {
      optimistic_latch.store(optimistic_latch.load() + LSB, std::memory_order_release);
      current_value.*a = new_value;
      optimistic_latch.store(optimistic_latch.load() + LSB, std::memory_order_release);
   }
   // -------------------------------------------------------------------------------------
   void wait(T& copy) { optimistic_latch.wait(copy.version); }
};
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace leanstore
