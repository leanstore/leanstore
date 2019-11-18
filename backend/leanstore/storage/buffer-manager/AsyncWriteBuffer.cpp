#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <cstring>
#include <signal.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 4, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
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
   const int ret = io_setup(n_buffer_slots, &aio_context);
   if ( ret != 0 ) {
      throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
   }
   ensure (io_setup(n_buffer_slots, &aio_context) != 0);
   // -------------------------------------------------------------------------------------
   timeout.tv_sec = 0;
   timeout.tv_nsec = (5 * 10e5);
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::add(BufferFrame &bf)
{
   ensure(u64(&bf.page) % 512 == 0);
   if ( !write_buffer_free_slots.size()) {
      return false;
   }
   // -------------------------------------------------------------------------------------
   bf.page.magic_debugging_number = bf.header.pid;
   // -------------------------------------------------------------------------------------
   auto slot = write_buffer_free_slots.back();
   write_buffer_free_slots.pop_back();
   WriteCommand &wc = write_buffer_commands[slot];
   wc.pid = bf.header.pid;
   wc.bf = &bf;
   wc.guard = ReadGuard(bf.header.lock);
   batch.push_back(slot);
   return true;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary(std::function<void(BufferFrame &, u64)> callback, u64 batch_max_size)
{
   const auto c_batch_size = std::min(batch.size(), batch_max_size - pending_requests);
   u32 successfully_copied_bfs = 0;
   auto my_iocbs_ptr = iocbs_ptr.get();
   if ( c_batch_size >= batch_max_size || insistence_counter == FLAGS_insistence_limit ) {
      for ( auto i = 0; i < c_batch_size; i++ ) {
         auto slot = batch.back();
         batch.pop_back();
         // -------------------------------------------------------------------------------------
         WriteCommand &c_command = write_buffer_commands[slot];
         // -------------------------------------------------------------------------------------
         try {
            std::memcpy(&write_buffer[slot], c_command.bf->page, page_size);
            c_command.guard.recheck();
            void *write_buffer_slot_ptr = &write_buffer[slot];
            io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * c_command.pid);
            iocbs[slot].data = write_buffer_slot_ptr;
            *my_iocbs_ptr++ = &iocbs[slot];
            successfully_copied_bfs++;
         } catch ( RestartException e ) {

         }
         // -------------------------------------------------------------------------------------
      }
      pending_requests += successfully_copied_bfs;
      const int ret_code = io_submit(aio_context, successfully_copied_bfs, iocbs_ptr.get());
      ensure(ret_code == successfully_copied_bfs);
      insistence_counter = 0;
   } else {
      insistence_counter++;
   }
   // -------------------------------------------------------------------------------------
   if ( pending_requests ) {
      ensure(pending_requests <= batch_max_size);
      const int done_requests = io_getevents(aio_context, pending_requests, batch_max_size, events.get(), &timeout);
      if ( done_requests < 0 ) {
         throw ex::GenericException("io_getevents failed, res = " + std::to_string(done_requests));
      }
      pending_requests -= done_requests;
      for ( auto i = 0; i < done_requests; i++ ) {
         ensure(events[i].res == page_size);
         ensure(events[i].res2 == 0);
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
}
// -------------------------------------------------------------------------------------