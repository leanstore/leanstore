#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cstring>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 n_buffer_slots)
        : fd(fd)
          , page_size(page_size)
          , n_buffer_slots(n_buffer_slots)
{
   write_buffer = make_unique<BufferFrame::Page[]>(n_buffer_slots);
   write_buffer_commands = make_unique<WriteCommand[]>(n_buffer_slots);
   // -------------------------------------------------------------------------------------
   for ( auto i = 0; i < n_buffer_slots; i++ ) {
      write_buffer_free_slots.push_back(i);
   }
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   if ( io_setup(n_buffer_slots, &aio_context) != 0 ) {
      throw Generic_Exception("io_setup failed");
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(leanstore::BufferFrame &bf)
{
   assert(write_buffer_free_slots.size());
   assert(u64(&bf.page) % 512 == 0);
   //TODO: what happens if we run out of slots ?
   auto slot = write_buffer_free_slots.back();
   write_buffer_free_slots.pop_back();
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   WriteCommand &wc = write_buffer_commands[slot];
   wc.pid = bf.header.pid;
   wc.bf = &bf;
   batch.push_back(slot);
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary(std::function<void(BufferFrame &, u64)> callback, u64 batch_max_size)
{
   assert(batch.size() <= batch_max_size);
   const auto c_batch_size = batch.size();
   if ( c_batch_size == batch_max_size || insistence_counter == insistence_max_value) {
      struct iocb *iocbs = new iocb[c_batch_size];
      struct iocb *iocbs_ptr[c_batch_size];
      for ( auto i = 0; i < c_batch_size; i++ ) {
         auto slot = batch[i];
         WriteCommand &c_command = write_buffer_commands[slot];
         void *write_buffer_slot_ptr = &write_buffer[slot];
         io_prep_pwrite(iocbs + i, fd, write_buffer_slot_ptr, page_size, page_size * c_command.pid);
         iocbs[i].data = write_buffer_slot_ptr;
         iocbs_ptr[i] = iocbs + i;
      }
      const int ret_code = io_submit(aio_context, c_batch_size, iocbs_ptr);
      check(ret_code == c_batch_size);
      batch.clear();
      insistence_counter = 0;
   } else {
      insistence_counter++;
   }
   // -------------------------------------------------------------------------------------
   const u32 event_max_nr = 10;
   struct io_event events[batch_max_size];
   struct timespec timeout;
   u64 polled_events_nr = 0;
   timeout.tv_sec = 0;
   timeout.tv_nsec = (5 * 10e4);
   polled_events_nr = io_getevents(aio_context, batch_max_size, event_max_nr, events, &timeout);
   if ( polled_events_nr <= event_max_nr )
      for ( auto i = 0; i < polled_events_nr; i++ ) {
         const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
         //TODO: check event result code
         auto c_command = write_buffer_commands[slot];
         auto written_lsn = write_buffer[slot].LSN;
         assert(c_command.bf->header.isWB == true);
         callback(*c_command.bf, written_lsn);
         write_buffer_free_slots.push_back(slot);
      }
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------