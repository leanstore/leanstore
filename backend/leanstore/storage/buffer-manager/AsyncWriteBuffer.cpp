#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <cstring>
#include <signal.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 1, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size)
        : fd(fd)
          , page_size(page_size)
          , batch_max_size(batch_max_size)
{
   write_buffer = make_unique<BufferFrame::Page[]>(batch_max_size);
   write_buffer_commands = make_unique<WriteCommand[]>(batch_max_size);
   iocbs = make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = make_unique<struct iocb *[]>(batch_max_size);
   events = make_unique<struct io_event[]>(batch_max_size);
   // -------------------------------------------------------------------------------------
   for ( auto i = 0; i < batch_max_size; i++ ) {
      write_buffer_free_slots.push_back(i);
   }
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(batch_max_size, &aio_context);
   if ( ret != 0 ) {
      throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
   }
   ensure (io_setup(batch_max_size, &aio_context) != 0);
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::add(BufferFrame &bf)
{
   ensure(u64(&bf.page) % 512 == 0);
   if ( !write_buffer_free_slots.size()) {
      return false;
   }
   // -------------------------------------------------------------------------------------
   // We are not allowed to modify the bf yet, we accquire a read lock, and in the future
   // we upgrade to write lock, copy the page and maintain the meta data
   auto slot = write_buffer_free_slots.back();
   write_buffer_free_slots.pop_back();
   WriteCommand &wc = write_buffer_commands[slot];
   wc.guard = ReadGuard(bf.header.lock);
   wc.pid = bf.header.pid;
   wc.bf = &bf;
   batch.push_back(slot);
   // -------------------------------------------------------------------------------------
   return true;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::submitIfNecessary()
{
   const auto c_batch_size = std::min(batch.size(), batch_max_size);
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
            ExclusiveGuard x_guard(c_command.guard);
            c_command.bf->page.magic_debugging_number = c_command.bf->header.pid;
            assert(c_command.pid == c_command.bf->header.pid);
            c_command.bf->header.isWB = true;
            std::memcpy(&write_buffer[slot], c_command.bf->page, page_size);
            void *write_buffer_slot_ptr = &write_buffer[slot];
            io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * c_command.pid);
            iocbs[slot].data = write_buffer_slot_ptr;
            *my_iocbs_ptr++ = &iocbs[slot];
            successfully_copied_bfs++;
         } catch ( RestartException e ) {
            write_buffer_free_slots.push_back(slot);
         }
         // -------------------------------------------------------------------------------------
      }
      const int ret_code = io_submit(aio_context, successfully_copied_bfs, iocbs_ptr.get());
      ensure(ret_code == successfully_copied_bfs);
      pending_requests += ret_code;
      insistence_counter = 0;
   } else {
      insistence_counter++;
   }

}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::pollEventsSync()
{
   if ( pending_requests ) {
      const int done_requests = io_getevents(aio_context, pending_requests, batch_max_size, events.get(), NULL);
      if ( u32(done_requests) != pending_requests ) {
         throw ex::GenericException("io_getevents failed, res = " + std::to_string(done_requests));
      }
      pending_requests -= done_requests;
      return done_requests;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::getWrittenBfs(std::function<void(BufferFrame &, u64)> callback, u64 n_events)
{
   for ( u64 i = 0; i < n_events; i++ ) {
      explain(events[i].res == page_size);
      explain(events[i].res2 == 0);
      // -------------------------------------------------------------------------------------
      const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      auto c_command = write_buffer_commands[slot];
      auto written_lsn = write_buffer[slot].LSN;
      callback(*c_command.bf, written_lsn);
      write_buffer_free_slots.push_back(slot);
   }
}
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------