#include "FreeList.hpp"

#include "Exceptions.hpp"
#include "leanstore/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
void FreeList::push(BufferFrame& bf)
{
  assert(bf.header.state == BufferFrame::State::FREE);
  assert((bf.header.lock->load() & LATCH_EXCLUSIVE_BIT) == 0);
  bf.header.next_free_bf = head.load();
  while (!head.compare_exchange_strong(bf.header.next_free_bf, &bf))
    ;
  counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::tryPop(JMUW<std::unique_lock<std::mutex>>& lock)
{
  BufferFrame* c_header = head;
  BufferFrame* free_bf = nullptr;
  if (c_header != nullptr) {
    BufferFrame* next = c_header->header.next_free_bf;
    if (head.compare_exchange_strong(c_header, next)) {
      free_bf = c_header;
      free_bf->header.next_free_bf = nullptr;
      counter--;
      assert((free_bf->header.lock->load() & LATCH_EXCLUSIVE_BIT) == 0);
      assert(free_bf->header.state == BufferFrame::State::FREE);
    } else {
      lock->unlock();
      jumpmu::jump();
    }
  } else {
    lock->unlock();
    jumpmu::jump();
  }
  return *free_bf;  // unreachable
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::pop()
{
  BufferFrame* c_header = head;
  BufferFrame* free_bf = nullptr;
  while (c_header != nullptr) {
    BufferFrame* next = c_header->header.next_free_bf;
    if (head.compare_exchange_strong(c_header, next)) {
      free_bf = c_header;
      free_bf->header.next_free_bf = nullptr;
      counter--;
      assert((free_bf->header.lock->load() & LATCH_EXCLUSIVE_BIT) == 0);
      assert(free_bf->header.state == BufferFrame::State::FREE);
      return *free_bf;
    } else {
      //WorkerCounters::myCounters().dt_researchy_1[0]++;
      if (c_header == nullptr) {
        //WorkerCounters::myCounters().dt_researchy_2[0]++;
        jumpmu::jump();
      } else {
        c_header = head.load();
      }
    }
  }
  //WorkerCounters::myCounters().dt_researchy_2[0]++;
  jumpmu::jump();
  return *free_bf;  // unreachable
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
