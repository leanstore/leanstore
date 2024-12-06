#pragma once
#include "BufferFrame.hpp"
#include "Partition.hpp"
#include "Units.hpp"
#include "BMPlainGuard.hpp"
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
   std::function<Partition&(PID)> getPartition;
   std::function<void(BufferFrame& write_command)> pageCallback;
   std::vector<std::pair<BufferFrame*, PID>> pagesToWrite;
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
  public:
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, std::function<Partition&(PID)> getPartition,
      std::function<void(BufferFrame& write_command)> pageCallback);
   void flush();
   void add(BufferFrame& bf, PID pid);
  private:
   void ensureNotFull();
   bool full();
   bool empty();
   u64 submit();
   void waitAndHandle(u64 submitted_pages);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
