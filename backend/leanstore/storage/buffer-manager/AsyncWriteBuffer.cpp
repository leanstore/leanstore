#include "AsyncWriteBuffer.hpp"

#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "leanstore/profiling/counters/PPCounters.hpp"
// -------------------------------------------------------------------------------------
#include <signal.h>

#include <cstring>
// -------------------------------------------------------------------------------------
DEFINE_uint32(insistence_limit, 1, "");
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size) : fd(fd), page_size(page_size), batch_max_size(batch_max_size)
{
   write_buffer = make_unique<BufferFrame::Page[]>(batch_max_size);
   write_buffer_commands = make_unique<WriteCommand[]>(batch_max_size);
   iocbs = make_unique<struct iocb[]>(batch_max_size);
   iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   events = make_unique<struct io_event[]>(batch_max_size);
   // -------------------------------------------------------------------------------------
   memset(&aio_context, 0, sizeof(aio_context));
   const int ret = io_setup(batch_max_size, &aio_context);
   if (ret != 0) {
      throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
   }
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::full()
{
   return pending_requests > batch_max_size - 3;
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::empty()
{
   return pending_requests == 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(BufferFrame& bf, PID pid)
{
   assert(!full());
   assert(u64(&bf.page) % 512 == 0);
   assert(pending_requests <= batch_max_size);
   // -------------------------------------------------------------------------------------
   auto slot = pending_requests++;
   write_buffer_commands[slot].bf = &bf;
   write_buffer_commands[slot].pid = pid;
   bf.page.magic_debugging_number = pid;
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   void* write_buffer_slot_ptr = &write_buffer[slot];
   io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pid);
   iocbs[slot].data = write_buffer_slot_ptr;
   iocbs_ptr[slot] = &iocbs[slot];
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submitAndWait()
{
   if (pending_requests > 0) {
      int ret_code = io_submit(aio_context, pending_requests, iocbs_ptr.get());
      ensure(ret_code == s32(pending_requests));

      const int done_requests = io_getevents(aio_context, pending_requests, pending_requests, events.get(), NULL);
      if (u32(done_requests) != pending_requests) {
         cerr << "Error in number of Requests: " << done_requests << endl;
         raise(SIGTRAP);
         assert(false);
      }
      pending_requests = 0;
      leanstore::PPCounters::myCounters().total_writes += done_requests;
      return done_requests;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::handleWritten(u32 n_events, Partition& partition)
{
   for (volatile u64 i = 0; i < n_events; i++) {
      const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      assert(events[i].res == page_size);
      explainIfNot(events[i].res2 == 0);
      auto written_lsn = write_buffer[slot].GSN;
      BufferFrame& written_bf = *write_buffer_commands[slot].bf;
      PID out_of_place_pid = write_buffer_commands[slot].pid;
      while (true) {
         jumpmuTry()
         {
            Guard guard(written_bf.header.latch);
            guard.toExclusive();
            assert(written_bf.header.isInWriteBuffer);
            assert(written_bf.header.lastWrittenGSN < written_lsn);
            // -------------------------------------------------------------------------------------
            if (FLAGS_out_of_place) {
               partition.freePage(written_bf.header.pid);
               written_bf.header.pid = out_of_place_pid;
            }
            written_bf.header.lastWrittenGSN = written_lsn;
            written_bf.header.isInWriteBuffer = false;
            PPCounters::myCounters().flushed_pages_counter++;
            // -------------------------------------------------------------------------------------
            guard.unlock();
            jumpmu_break;
         }
         jumpmuCatch() {}
      }
      partition.addNextBufferFrame(write_buffer_commands[slot].bf);
   }
}
// if async_write_buffer has pages: get & handle evicted.
u32 AsyncWriteBuffer::flush(Partition& partition)
{
   if(!empty()) {
      const u32 n_events = submitAndWait();
      handleWritten(n_events, partition);
      return n_events;
//      cout << "Pages Flushed " << n_events << endl;
   }
   return 0;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
