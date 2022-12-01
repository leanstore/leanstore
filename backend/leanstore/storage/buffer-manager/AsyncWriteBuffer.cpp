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
void AsyncWriteBuffer::ensureNotFull(Partition& partition)
{
   if(full()){
      flush(partition);
   }
}

bool AsyncWriteBuffer::full()
{
   return pagesToWrite.size() >= batch_max_size;
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::empty()
{
   return pagesToWrite.empty();
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(BufferFrame* bf, PID pid)
{
   assert(!full());
   assert(u64(&bf->page) % 512 == 0);
   // -------------------------------------------------------------------------------------
   pagesToWrite.push_back({bf, pid});
   u64 slot = pagesToWrite.size()-1;
   // Make copy of page, because we want to write the page in its current stage (and it is locked right now)
   std::memcpy(&write_buffer[slot], bf->page, page_size);
   write_buffer[slot].magic_debugging_number = pid;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::waitAndHandle(Partition& partition)
{
   if(submitted_pages==0){
      return;
   }
   const int done_requests = io_getevents(aio_context, submitted_pages, submitted_pages, events.get(), NULL);
   if (u32(done_requests) != submitted_pages) {
      cerr << "Error in number of Requests: " << done_requests << endl;
      raise(SIGTRAP);
      assert(false);
   }
   handleWritten(partition);
}
void AsyncWriteBuffer::submit()
{
   u64 slot = 0;
   for(auto& pagecombi: pagesToWrite){
      write_buffer_commands[slot].bf = pagecombi.first;
      write_buffer_commands[slot].pid = pagecombi.second;
      void* write_buffer_slot_ptr = &write_buffer[slot];
      io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size *pagecombi.second);
      iocbs[slot].data = write_buffer_slot_ptr;
      iocbs_ptr[slot] = &iocbs[slot];
      slot++;
   }
   submitted_pages = pagesToWrite.size();
   pagesToWrite.clear();
   int ret_code = io_submit(aio_context, submitted_pages, iocbs_ptr.get());
   ensure(ret_code == s32(submitted_pages));

}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::handleWritten(Partition& partition)
{
   PPCounters::myCounters().total_writes += submitted_pages;
   for (volatile u64 i = 0; i < submitted_pages; i++) {
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
void AsyncWriteBuffer::flush(Partition& partition)
{
   if(!empty()) {
      submit();
      waitAndHandle(partition);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
