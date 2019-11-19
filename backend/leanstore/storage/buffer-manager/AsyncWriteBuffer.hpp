#pragma once
#include "Units.hpp"
#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <functional>
#include <libaio.h>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer {
private:
   struct WriteCommand {
      BufferFrame *bf;
      PID pid;
      ReadGuard guard;
   };
   io_context_t aio_context;
   u64 page_size, n_buffer_slots;
   int fd;
   std::vector<u64> batch;
   u8 insistence_counter = 0;
   u64 pending_requests = 0;
   struct timespec timeout;
public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   // -------------------------------------------------------------------------------------
   std::vector<u64> write_buffer_free_slots;
   AsyncWriteBuffer(int fd, u64 page_size, u64 n_buffer_slots);
   // Caller takes care of sync
   void add(BufferFrame &bf);
   void submitIfNecessary(std::function<void(BufferFrame &, u64)>, u64 batch_max_size);
};
// -------------------------------------------------------------------------------------
}
}
// -------------------------------------------------------------------------------------
