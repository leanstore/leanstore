#pragma once

#include "PreallocationStack.hpp"
#include "Units.hpp"

#if true
#define DEBUG_COUNTER(x) x
#else
#define DEBUG_COUNTER(x)
#endif

namespace leanstore {
namespace utils {
template<typename TValue>
class SmallForwardList {
   struct Entry {
      TValue value;
      Entry* next = nullptr;
   };
   Entry _begin;
   Entry* last_ptr;
   PreallocationStack<Entry> prealloc;
   DEBUG_COUNTER(
         u64 inserted = 0;
         u64 erased = 0;
   )
   u64 contains = 0;
public:
   class iterator 
   {
   public:
      using iterator_category = std::forward_iterator_tag;
      using pointer = const TValue*;
      using reference = const TValue&;
      using difference_type = std::ptrdiff_t;
      using value_type = TValue;
   private:
      Entry* _ptr;
   public:
      iterator(Entry* ptr) : _ptr(ptr) { 
      }
      reference operator*() const { return _ptr->value; }
      pointer operator->() { return &_ptr->value; }
      iterator& operator++() { _ptr = _ptr->next; return *this; } // prefix
      iterator operator++(int) { iterator tmp = *this; ++(*this); return tmp; } // postfix
      friend bool operator== (const iterator& lh, const iterator& rh) { return lh._ptr == rh._ptr; };
      friend bool operator!= (const iterator& lh, const iterator& rh) { return lh._ptr != rh._ptr; };  
      friend SmallForwardList;
   };
   SmallForwardList(int max_size) : prealloc(max_size) {
      _begin.next = nullptr;
      last_ptr = &_begin;
      _begin.value = TValue();
   }
   SmallForwardList(SmallForwardList const&) = delete;
   TValue& emplace_back(const TValue&& value) {
      Entry* newEnd = prealloc.pop();
      newEnd->next = nullptr;
      newEnd->value = value;
      last_ptr->next = newEnd;
      last_ptr = newEnd;
      DEBUG_COUNTER(inserted++);
      contains++;
      assert(last_ptr->next == nullptr);
      return newEnd->value;
   }
   iterator before_begin() {
      return iterator(&_begin);
   }
   iterator begin() {
      assert(last_ptr->next == nullptr);
      return iterator(_begin.next);
   }
   iterator end() {
      assert(last_ptr->next == nullptr);
      return iterator(nullptr);
   }
   iterator erase_after(iterator pos) {
      assert(pos != end());
      Entry* oldNext = pos._ptr->next;
      assert(oldNext != nullptr);
      if (oldNext == last_ptr) {
         last_ptr = pos._ptr;
         pos._ptr->next = nullptr;
      } else {
         pos._ptr->next = oldNext->next;
      }
      oldNext->next = nullptr;
      prealloc.ret(oldNext);
      DEBUG_COUNTER(erased++);
      contains--;
      assert(last_ptr->next == nullptr);
      return iterator(pos._ptr);
   }
   int size() {
      return contains;//prealloc.size();
   }
};
} //namespace utils
} //namespace leanstor

