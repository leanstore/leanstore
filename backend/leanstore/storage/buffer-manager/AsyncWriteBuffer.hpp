#pragma once
#include "BufferFrame.hpp"
#include "Partition.hpp"
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
   u64 page_size, batch_max_size, submitted_pages;
   std::vector<std::pair<BufferFrame*, PID>> pagesToWrite;

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
   void ensureNotFull(Partition& partition);
   void add(BufferFrame* bf, PID pid);
   void flush(Partition& partition);
  private:
   bool full();
   bool empty();
   void handleWritten(Partition& partition);
   void submit();
   void waitAndHandle(Partition& partition);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
