#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <cstring>
#include <signal.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 4, "");
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
   iocbs = make_unique<struct iocb[]>(n_buffer_slots);
   iocbs_ptr = make_unique<struct iocb *[]>(n_buffer_slots);
   events = make_unique<struct io_event[]>(n_buffer_slots);
   // -------------------------------------------------------------------------------------
   for ( auto i = 0; i < n_buffer_slots; i++ ) {
      write_buffer_free_slots.push_back(i);
   }
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   if ( io_setup(n_buffer_slots, &aio_context) != 0 ) {
      throw Generic_Exception("io_setup failed");
   }
   // -------------------------------------------------------------------------------------
   timeout.tv_sec = 0;
   timeout.tv_nsec = (5 * 10e5);
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::add(leanstore::BufferFrame &bf)
{
   rassert(u64(&bf.page) % 512 == 0);
   if ( !write_buffer_free_slots.size()) {
      return false;
   }
   // -------------------------------------------------------------------------------------
   bf.page.magic_debugging_number = bf.header.pid;
   // -------------------------------------------------------------------------------------
   auto slot = write_buffer_free_slots.back();
   write_buffer_free_slots.pop_back();
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   WriteCommand &wc = write_buffer_commands[slot];
   wc.pid = bf.header.pid;
   wc.bf = &bf;
   batch.push_back(slot);
   return true;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary(std::function<void(BufferFrame &, u64)> callback, u64 batch_max_size)
{
   const auto c_batch_size = std::min(batch.size(), batch_max_size);
   if ( c_batch_size >= batch_max_size || insistence_counter == FLAGS_insistence_limit ) {
      for ( auto i = 0; i < c_batch_size; i++ ) {
         auto slot = batch.back();
         batch.pop_back();
         // -------------------------------------------------------------------------------------
         WriteCommand &c_command = write_buffer_commands[slot];
         void *write_buffer_slot_ptr = &write_buffer[slot];
         io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * c_command.pid);
         iocbs[slot].data = write_buffer_slot_ptr;
         iocbs_ptr[i] = &iocbs[slot];
      }
      const int ret_code = io_submit(aio_context, c_batch_size, iocbs_ptr.get());
      rassert(ret_code == c_batch_size);
      pending_requests += c_batch_size;
      insistence_counter = 0;
   } else {
      insistence_counter++;
   }
   // -------------------------------------------------------------------------------------
   if ( pending_requests ) {
      const int done_requests = io_getevents(aio_context, pending_requests, batch_max_size, events.get(), &timeout);
      rassert(done_requests >= 0);
      pending_requests -= done_requests;
      for ( auto i = 0; i < done_requests; i++ ) {
         rassert(events[i].res == page_size);
         rassert(events[i].res2 == 0);
         // -------------------------------------------------------------------------------------
         const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
         auto c_command = write_buffer_commands[slot];
         auto written_lsn = write_buffer[slot].LSN;
         callback(*c_command.bf, written_lsn);
         write_buffer_free_slots.push_back(slot);
      }
   }
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------