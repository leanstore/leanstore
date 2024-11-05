#include "AsyncWriteBuffer.hpp"
#include "Tracing.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
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
AsyncWriteBuffer::AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, std::function<Partition&(PID)> getPartition, std::function<void(BufferFrame& write_command)> pageCallback) : fd(fd), page_size(page_size), batch_max_size(batch_max_size), getPartition(getPartition), pageCallback(pageCallback)
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
   return pagesToWrite.size() >= batch_max_size - 2;
}
// -------------------------------------------------------------------------------------
bool AsyncWriteBuffer::empty()
{
   return pagesToWrite.empty();
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::ensureNotFull()
{
   if(full()){
      flush();
   }
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::add(BufferFrame& bf, PID pid)
{
   ensureNotFull();
   assert(!full());
   assert(u64(&bf.page) % 512 == 0);
   COUNTERS_BLOCK() { WorkerCounters::myCounters().dt_page_writes[bf.page.dt_id]++; }
   // -------------------------------------------------------------------------------------
   PARANOID_BLOCK()
   {
      if (FLAGS_pid_tracing && !FLAGS_recycle_pages) {
         Tracing::mutex.lock();
         if (Tracing::ht.contains(pid)) {
            auto& entry = Tracing::ht[pid];
            ensure(std::get<0>(entry) == bf.page.dt_id);
         }
         Tracing::mutex.unlock();
      }
   }
   // -------------------------------------------------------------------------------------
   pagesToWrite.push_back({&bf, pid});
   auto slot = pagesToWrite.size()-1;
   // Make copy of page, because we want to write the page in its current stage (and it is locked right now)
   std::memcpy(&write_buffer[slot], bf.page, page_size);
   write_buffer[slot].magic_debugging_number=pid;
}
// -------------------------------------------------------------------------------------
u64 AsyncWriteBuffer::submit()
{
   for(u64 slot = 0; slot < pagesToWrite.size(); slot++){
      write_buffer_commands[slot].bf = pagesToWrite[slot].first;
      write_buffer_commands[slot].pid = pagesToWrite[slot].second;
      void* write_buffer_slot_ptr = &write_buffer[slot];
      io_prep_pwrite(&iocbs[slot], fd, write_buffer_slot_ptr, page_size, page_size * pagesToWrite[slot].second);
      iocbs[slot].data = write_buffer_slot_ptr;
      iocbs_ptr[slot] = &iocbs[slot];
   }
   u64 submitted_pages = pagesToWrite.size();
   pagesToWrite.clear();

   int ret_code = io_submit(aio_context, submitted_pages, iocbs_ptr.get());
   ensure(ret_code == s32(submitted_pages));
   return submitted_pages;
}
// -------------------------------------------------------------------------------------
void AsyncWriteBuffer::waitAndHandle(u64 submitted_pages)
{
   if (submitted_pages == 0){
      return;
   }
   const int done_requests = io_getevents(aio_context, submitted_pages, submitted_pages, events.get(), NULL);
   if (u32(done_requests) != submitted_pages) {
      cerr << "Error in number of Requests: " << done_requests << " of " << submitted_pages << endl;
      raise(SIGTRAP);
      ensure(false);
   }

   for (u64 i = 0; i < submitted_pages; i++) {
      const auto slot = (u64(events[i].data) - u64(write_buffer.get())) / page_size;
      // -------------------------------------------------------------------------------------
      ensure(events[i].res == page_size);
      explainIfNot(events[i].res2 == 0);
      auto written_lsn = write_buffer[slot].PLSN;
      BufferFrame& bf = *write_buffer_commands[slot].bf;
      PID out_of_place_pid = write_buffer_commands[slot].pid;
      jumpmuTry()
      {
         // When the written back page is being exclusively locked, we should rather waste the write and move on to another page
         // Instead of waiting on its latch because of the likelihood that a data structure implementation keeps holding a parent latch
         // while trying to acquire a new page
         {
            BMOptimisticGuard o_guard(bf.header.latch);
            BMExclusiveGuard ex_guard(o_guard);
            ensure(bf.header.is_being_written_back);
            ensure(bf.header.last_written_plsn < written_lsn);
            // -------------------------------------------------------------------------------------
            if (FLAGS_out_of_place) {  // For recovery, so much has to be done here...
               getPartition(bf.header.pid).freePage(bf.header.pid);
               bf.header.pid = out_of_place_pid;
            }
            bf.header.last_written_plsn = written_lsn;
            bf.header.is_being_written_back = false;
            PPCounters::myCounters().flushed_pages_counter++;
         }
      }
      jumpmuCatch()
      {
         bf.header.crc = 0;
         bf.header.is_being_written_back.store(false, std::memory_order_release);
      }
      // -------------------------------------------------------------------------------------
      pageCallback(bf);
   }
   COUNTERS_BLOCK() { PPCounters::myCounters().total_writes += submitted_pages; }
}
// -------------------------------------------------------------------------------------
// if async_write_buffer has pages: get & handle evicted.
void AsyncWriteBuffer::flush()
{
   if(!empty()) {
      u64 submitted_pages = submit();
      waitAndHandle(submitted_pages);
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
   // -------------------------------------------------------------------------------------
