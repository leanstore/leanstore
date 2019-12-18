#include "FreeList.hpp"
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
  assert((bf.header.lock & WRITE_LOCK_BIT) == 0);
  bf.header.next_free_bf = head.load();
  while (!head.compare_exchange_strong(bf.header.next_free_bf, &bf))
    ;
  counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::pop()
{
  BufferFrame* c_header = head;
  while (c_header != nullptr) {
    BufferFrame* next = c_header->header.next_free_bf;
    if (head.compare_exchange_strong(c_header, next)) {
      BufferFrame& bf = *c_header;
      bf.header.next_free_bf = nullptr;
      counter--;
      assert((bf.header.lock & WRITE_LOCK_BIT) == 0);
      assert(bf.header.state == BufferFrame::State::FREE);
      return bf;
    } else {
      c_header = head.load();
    }
  }
  throw RestartException();
}
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}