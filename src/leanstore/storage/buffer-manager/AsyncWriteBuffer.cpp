#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 buffer_size)
        : fd(fd), page_size(page_size)
          , n_buffer_slots(buffer_size)
{
   write_buffer = make_unique<u8[]>(page_size * buffer_size);
   memset(&aio_context, 0, sizeof(aio_context));
   if ( io_setup(buffer_size, &aio_context) != 0 ) {
      throw Generic_Exception("io_setup failed");
   }
   for ( auto i = 0; i < buffer_size; i++ ) {
      buffer_free_slots.push_front(i);
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(leanstore::BufferFrame &bf)
{
   assert(buffer_free_slots.size());
   assert(u64(&bf.page) % 512 == 0);
   auto slot = buffer_free_slots.front();
   buffer_free_slots.pop_front();
   memcpy(write_buffer.get() + (page_size * slot), bf.page, page_size);
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary(std::function<void(BufferFrame &, u64)>, u64 batch_size)
{
   if ( n_buffer_slots - buffer_free_slots.size() >= batch_size ) {
      struct iocb iocbs[batch_size];
      struct iocb *iocbs_ptr[batch_size];
      for ( auto i = 0; i < batch_size; i++ ) {
         io_prep_pwrite(iocbs + i, fd, (void *)(write_buffer + ()))
      }
   }
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------