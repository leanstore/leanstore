#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 buffer_size)
        : fd(fd)
          , page_size(page_size)
          , n_buffer_slots(buffer_size)
{
   write_buffer = make_unique<u8[]>(page_size * buffer_size);
   memset(&aio_context, 0, sizeof(aio_context));
   if ( io_setup(buffer_size, &aio_context) != 0 ) {
      throw Generic_Exception("io_setup failed");
   }
   for ( auto i = 0; i < buffer_size; i++ ) {
      write_buffer_free_slots.push_front(i);
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(leanstore::BufferFrame &bf)
{
   assert(write_buffer_free_slots.size());
   assert(u64(&bf.page) % 512 == 0);
   //TODO: what happens if we run out of slots ?
   auto slot = write_buffer_free_slots.front();
   write_buffer_free_slots.pop_front();
   memcpy(write_buffer.get() + (page_size * slot), bf.page, page_size);
   batch.push_back({&bf, slot});
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary(std::function<void(BufferFrame &, u64)> callback, u64 batch_size)
{
   assert(batch.size() <= batch_size);
   if ( batch.size() == batch_size ) {
      assert(batch.size() == batch_size);
      struct iocb iocbs[batch_size];
      struct iocb *iocbs_ptr[batch_size];
      for ( auto i = 0; i < batch_size; i++ ) {
         auto c_command = batch[i];
         auto bf = reinterpret_cast<BufferFrame *>(write_buffer.get() + (page_size * c_command.write_buffer_slot));
         bf->header.isWB = true;
         void *write_buffer_slot_ptr = bf;
         io_prep_pwrite(iocbs + i, fd, write_buffer_slot_ptr, page_size, page_size * bf->header.pid);
         iocbs_ptr[i] = iocbs + i;
      }
      if ( io_submit(aio_context, batch_size, iocbs_ptr) != 1 ) {
         throw Generic_Exception("io_submit failed");
      }
      for ( auto i = 0; i < batch_size; i++ ) {
         ht.insert({iocbs[i].key, batch[i]});
      }
      batch.clear();
   }
   // -------------------------------------------------------------------------------------
   const u32 event_max_nr = 10;
   struct io_event events[batch_size];
   struct timespec timeout;
   u64 polled_events_nr = 0;
   timeout.tv_sec = 0;
   timeout.tv_nsec = (1 * 10e9);
   polled_events_nr = io_getevents(aio_context, batch_size, event_max_nr, events, &timeout);
   if(polled_events_nr <= event_max_nr)
   for ( auto i = 0; i < polled_events_nr; i++ ) {
      const auto io_key = events[i].obj->key;
      auto itr = ht.find(io_key);
      assert(itr != ht.end());
      auto c_command = itr->second;
      auto written_lsn = reinterpret_cast<BufferFrame *>(write_buffer.get() + (page_size * c_command.write_buffer_slot))->page.LSN;
      callback(*c_command.bf, written_lsn);
      ht.erase(itr);
      write_buffer_free_slots.push_back(c_command.write_buffer_slot);
   }
   sleep(1);
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------