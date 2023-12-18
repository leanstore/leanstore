#pragma once
// -------------------------------------------------------------------------------------
#include <chrono>
#include <memory>
#include <stdexcept>
#include <unordered_set>
#include "Exceptions.hpp"
#include "leanstore/io/IoRequest.hpp"
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
template <typename R>
class RequestStack
{
  public:
   std::unique_ptr<R[]> requests;
   std::unique_ptr<R*[]> free_stack;
   std::unique_ptr<R*[]> submit_stack;
#ifndef NDEBUG
   std::unordered_set<R*> outstanding_set;
#endif

   const int max_entries;
   int free;
   int pushed = 0;

   RequestStack(int max_entries) : max_entries(max_entries), free(max_entries)
   {
      requests = std::make_unique<R[]>(max_entries);
      free_stack = std::make_unique<R*[]>(max_entries);
      submit_stack = std::make_unique<R*[]>(max_entries);
      for (int i = 0; i < max_entries; i++) {
         free_stack[i] = &requests[i];
      }
   };
   ~RequestStack() {}
   int outstanding() { 
      assert(max_entries - free - pushed == outstanding_set.size());
      return max_entries - free - pushed; 
   }
   int submitStackSize() {return pushed; }
   bool full() { return free == 0; }

   /* free -> to user (untracked)*/
   bool popFromFreeStack(R*& out) {
      assert(free >= 0);
      if (free == 0) {
        return false; 
      }
      free--;
      out = free_stack[free];
      return true;
   }
   /* user -> to submit */
   void pushToSubmitStack(R* req) {
      submit_stack[pushed] = req;
      pushed++;
   }
   /* free -> submit / direct path (not like popFromFree and pushToSubmit ) */
   bool moveFreeToSubmitStack(R*& out)
   {
      assert(free >= 0);
      if (free == 0) {
        return false; 
      }
      free--;
      assert(free < max_entries);
      out = free_stack[free];
      assert(pushed < max_entries);
      submit_stack[pushed] = out;
      pushed++;
      return true;
   }
   /* submit -> outstanding */
   void emptySubmitStack()
   {
#ifndef NDEBUG
      for (int i = 0; i < pushed; i++) {
         auto found = outstanding_set.find(submit_stack[i]);
         if (found == outstanding_set.end()) {
            outstanding_set.insert(submit_stack[i]);
         }
         submit_stack[i] = nullptr;
      }
#endif
      pushed = 0;
   }
   /* submit -> outstanding */
   bool popFromSubmitStack(R*& out)
   {
      if (pushed <= 0) {
         return false;
      }
      pushed--;
      out = submit_stack[pushed];
#ifndef NDEBUG
      auto found = outstanding_set.find(out);
      //ensure(found == outstanding_set.end());
      if (found == outstanding_set.end()) {
         outstanding_set.insert(submit_stack[pushed]);
      }
#endif
      return true;
   }
   /* outstanding -> free */
   void returnToFreeList(R* ptr)
   {
#ifndef NDEBUG
      auto found = outstanding_set.find(ptr);
      ensure(found != outstanding_set.end());
      outstanding_set.erase(found);
#endif
      free_stack[free] = ptr;
      free++;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
