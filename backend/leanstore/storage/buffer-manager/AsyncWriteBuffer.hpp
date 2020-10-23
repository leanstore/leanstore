#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>

#include <functional>
#include <list>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
   };
   io_context_t aio_context;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;

  public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size);
   // Caller takes care of sync
   bool full();
   void add(BufferFrame& bf, PID pid);
   u64 submit();
   u64 pollEventsSync();
   void getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
