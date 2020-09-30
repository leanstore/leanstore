#include "WAL.hpp"

#include "WALWriter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
WAL::WAL(u64 partition_id) : partition_id(partition_id)
{
  std::thread ww_thread([&]() {
    ww_thread_running = true;
    while (ww_thread_keep_running) {
      if (ww_cursor != wt_cursor) {
        chunks[ww_cursor].disk_image.partition_id = partition_id;
        chunks[ww_cursor].disk_image.chunk_size = WALChunk::CHUNK_SIZE - chunks[ww_cursor].free_space;
        chunks[ww_cursor].disk_image.partition_next_chunk_offset = 0;  // TODO:
        WALWriter::write(reinterpret_cast<u8*>(&chunks[ww_cursor].disk_image), WALChunk::CHUNK_SIZE);
        chunks[ww_cursor].reset();
        u64 next_chunk = (ww_cursor + 1) % CHUNKS_PER_WAL;
        ww_cursor.store(next_chunk, std::memory_order_release);
      }
    }
    ww_thread_running = false;
  });
  ww_thread.detach();
}
// -------------------------------------------------------------------------------------
WAL::~WAL()
{
  ww_thread_keep_running = false;
  while (ww_thread_running) {
  }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
