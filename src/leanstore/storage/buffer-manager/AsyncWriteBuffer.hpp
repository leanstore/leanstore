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
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer {
private:
   struct WriteCommand {
      BufferFrame *bf;
      PID pid;
   };
   io_context_t aio_context;
   u64 page_size, n_buffer_slots;
   int fd;
   std::vector<u64> batch;
   std::unordered_map<u64, struct WriteCommand> ht;
public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::vector<u64> write_buffer_free_slots;
   AsyncWriteBuffer(int fd, u64 page_size, u64 n_buffer_slots);
   // Caller takes care of sync
   void add(BufferFrame &bf);
   void submitIfNecessary(std::function<void(BufferFrame &, u64)>, u64 batch_size);
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
