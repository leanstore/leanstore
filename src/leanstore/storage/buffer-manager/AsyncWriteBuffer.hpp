#pragma once
#include "Units.hpp"
#include "BufferFrame.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <list>
#include <functional>
#include <libaio.h>
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer {
private:
   io_context_t aio_context;
   u64 page_size, n_buffer_slots;
   int fd;
public:
   std::unique_ptr<u8[]> write_buffer;
   std::list<u64> buffer_free_slots;
   AsyncWriteBuffer(int fd, u64 page_size, u64 n_buffer_slots);
   // Caller takes care of sync
   void add(BufferFrame &bf);
   void submitIfNecessary(std::function<void(BufferFrame &, u64)>, u64 batch_size);
};
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
