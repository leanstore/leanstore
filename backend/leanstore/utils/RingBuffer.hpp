#pragma once

#include <stdexcept>
#include <vector>
#include "Units.hpp"

#if false //NDEBUG
#define RB_DEBUG_COUNTER(x)
#else
#define RB_DEBUG_COUNTER(x) x
#endif

namespace leanstore {
namespace utils {
template<typename TValue>
class RingBuffer {
   std::vector<TValue> values;
   TValue *const vec_first;
   TValue *const vec_last;
   TValue* _read_ptr;
   TValue* _write_ptr;
public:
   RB_DEBUG_COUNTER(
         u64 inserted = 0;
         u64 erased = 0;
   )
   static constexpr int POP_MAX = 32;
   u64 contains = 0;
   const u64 max_size;
   RingBuffer(u64 max_size) : values(max_size+1), // +1 as one always stays empty. 
      vec_first(&values[0]), vec_last(&values[max_size]), 
      _read_ptr(&values[0]), _write_ptr(&values[0]), max_size(max_size) {
   }
   RingBuffer(RingBuffer const&) = delete;
   TValue& push_back(const TValue& value) {
      TValue*const current = _write_ptr;
      TValue* next = _write_ptr + 1;
      if (next > vec_last) { // overflow
         next = vec_first;
      }
      if (next == _read_ptr) { // full
         throw std::logic_error("full");
      }
      assert(contains < max_size);
      contains++;
      RB_DEBUG_COUNTER(inserted++;);
      assert(inserted > erased);
      *_write_ptr = value; // write value here
      _write_ptr = next; // move forward
      return *current; 
   }
   TValue& front() {
      if (_read_ptr == _write_ptr) {
         throw std::logic_error("empty");
      }
      return *_read_ptr;
   };
   bool try_pop(TValue& ret) {
      if (_read_ptr == _write_ptr) {
         return false;
      }
      RB_DEBUG_COUNTER(erased++;);
      assert(inserted >= erased);
      contains--;
      TValue* next = _read_ptr + 1;
      if (next > vec_last) { // overflow
         next = vec_first;
      }
      ret = *_read_ptr;
      RB_DEBUG_COUNTER(*_read_ptr = nullptr;)
      _read_ptr = next;
      return true;
   };
   int pop_multiple(std::array<TValue, POP_MAX>& pop_into, int pop_max) {
      TValue* current_read = _read_ptr;
      TValue*const current_write = _write_ptr;
      int popped = 0;
      while (current_read != current_write && popped < POP_MAX && popped < pop_max) {
         RB_DEBUG_COUNTER(erased++;);
         assert(inserted >= erased);
         pop_into[popped++] = *current_read;
         RB_DEBUG_COUNTER(*current_read = nullptr;)
         current_read++;
         if (current_read > vec_last) { // overflow
            current_read = vec_first;
         }
      }
      if (popped > 0) {
         _read_ptr = current_read; // make it visible to producers
      }
      contains -= popped;
      return popped;
   };
   u64 size() {
      return contains;//prealloc.size();
   }
   bool full() {
      TValue*const current = _write_ptr;
      TValue* next = _write_ptr + 1;
      if (next > vec_last) { // overflow
         next = vec_first;
      }
      if (next == _read_ptr) {
        return true; 
      }
      return false;
   }
   bool empty() {
      return _read_ptr == _write_ptr;
   }
};
} //namespace utils
} //namespace leanstor

