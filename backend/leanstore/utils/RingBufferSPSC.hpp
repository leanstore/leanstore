#pragma once

#include "leanstore/concurrency/YieldLock.hpp"

#include <mutex>
#include <stdexcept>
#include <vector>
#include <atomic>
#include "Units.hpp"

#if true
#define DEBUG_COUNTER(x) x
#else
#define DEBUG_COUNTER(x)
#endif

namespace leanstore {
namespace utils {
template<typename TValue>
class RingBufferSPSC {
   std::vector<TValue> values;
   TValue *const vec_first;
   TValue *const vec_last;
   alignas(64) std::atomic<TValue*> _write_ptr;
   alignas(64) std::atomic<TValue*> _read_ptr;
   DEBUG_COUNTER(
         u64 inserted = 0;
         u64 erased = 0;
   )
public:
   static constexpr int POP_MAX = 32;
   RingBufferSPSC(int max_size) : values(max_size+1), // +1 as one always stays empty. 
      vec_first(&values[0]), vec_last(&values[max_size]), 
      _write_ptr(&values[0]), _read_ptr(&values[0])  {
   }
   RingBufferSPSC(RingBufferSPSC const&) = delete;
   TValue& push_back(const TValue& value) {
      TValue*const current = _write_ptr.load(std::memory_order_relaxed);
      TValue* next = current + 1;
      if (next > vec_last) { // overflow
         next = vec_first;
      }
      if (next == _read_ptr.load(std::memory_order_acquire)) { // full
         throw std::logic_error("full");
      }
      DEBUG_COUNTER() {inserted++;}
      assert(inserted > erased);
      *current = value; // write value here
      _write_ptr.store(next, std::memory_order_release); // move forward
      return *current; 
   }
   bool empty() {
      TValue*const current_read = _read_ptr.load(std::memory_order_relaxed);
      return current_read == _write_ptr.load(std::memory_order_relaxed);
   }
   bool try_pop(TValue& ret) {
      TValue*const current_read = _read_ptr.load(std::memory_order_relaxed); // only accesed by a single thread 
      if (current_read == _write_ptr.load(std::memory_order_acquire)) {
         return false;
      }
      DEBUG_COUNTER() {erased++;}
      assert(inserted >= erased);
      TValue* next = current_read + 1;
      if (next > vec_last) { // overflow
         next = vec_first;
      }
      ret = *current_read;
      _read_ptr.store(next, std::memory_order_release); // make it visible to producer
      return true;
   };
   int pop_multiple(std::array<TValue, POP_MAX>& pop_into, int pop_max) {
      TValue* current_read = _read_ptr.load(std::memory_order_relaxed); // only accesed by a single thread 
      TValue*const current_write = _write_ptr.load(std::memory_order_acquire);
      int popped = 0;
      while (current_read != current_write && popped < POP_MAX && popped < pop_max) {
         DEBUG_COUNTER() {erased++;}
         assert(inserted >= erased);
         pop_into[popped++] = *current_read;
         current_read++;
         if (current_read > vec_last) { // overflow
            current_read = vec_first;
         }
      }
      if (popped > 0) {
         _read_ptr.store(current_read, std::memory_order_release); // make it visible to producers
      }
      return popped;
   };
};
} //namespace utils
} //namespace leanstor

