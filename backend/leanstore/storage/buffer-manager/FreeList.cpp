#include "FreeList.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
void FreeList::push(leanstore::buffermanager::BufferFrame &bf)
{
   assert(bf.header.state == BufferFrame::State::FREE);
   bf.header.next_free_bf = first.load();
   while ( !first.compare_exchange_strong(bf.header.next_free_bf, &bf));
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame &FreeList::pop()
{
   BufferFrame *c_header = first;
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   while ( true ) {
      while ( c_header == nullptr ) { //spin bf_s_lock
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         c_header = first;
      }
      BufferFrame *next = c_header->header.next_free_bf;
      if ( first.compare_exchange_strong(c_header, next)) {
         BufferFrame &bf = *c_header;
         bf.header.next_free_bf = nullptr;
         counter--;
         assert(bf.header.state == BufferFrame::State::FREE);
         return bf;
      }
   }
}
// -------------------------------------------------------------------------------------
}
}