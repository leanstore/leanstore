#pragma once
#include <stdexcept>
#include <vector>
namespace leanstore {
namespace utils {
template <typename TEntry>
class PreallocationStack {
   std::vector<TEntry> preallocated_entries;
   std::vector<TEntry*> unused_stack;
   int unused_stack_pos;
public:
   PreallocationStack(int size) {
      preallocated_entries = std::vector<TEntry>(size);
      unused_stack.resize(size);
      for (int i = 0; i < size; i++) {
         unused_stack[i] = &preallocated_entries[i];
      }
      unused_stack_pos = size;
   }
   TEntry* pop() {
      if (unused_stack_pos > 0) {
         return unused_stack[--unused_stack_pos]; 
      } else  {
         throw std::logic_error("Run out of space. Well it's a fixed size stack.");
      }
   }
   void ret(TEntry* e) {
      // if in range
      unused_stack[unused_stack_pos++] = e;
   }
};
} //namespace utils
} //namespace leanstore
