#include "WAL.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"

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
  std::thread ww_thread([&, partition_id]() {
    pthread_setname_np(pthread_self(), "ww");
    CPUCounters::registerThread("ww_" + partition_id);
    ww_thread_running = true;
    while (ww_thread_keep_running) {
      std::unique_lock<std::mutex> guard(mutex);
      cv.wait(guard, [&] { return (ww_thread_keep_running == false) || (ww_cursor != wt_cursor); });
      guard.unlock();
      chunks[ww_cursor].disk_image.partition_id = partition_id;
      chunks[ww_cursor].disk_image.chunk_size = WALChunk::CHUNK_SIZE - chunks[ww_cursor].free_space;
      chunks[ww_cursor].disk_image.partition_next_chunk_offset = 0;  // TODO:
      WALWriter::write(reinterpret_cast<u8*>(&chunks[ww_cursor].disk_image), WALChunk::CHUNK_SIZE);
      chunks[ww_cursor].reset();
      guard.lock();
      const u64 next_chunk = (ww_cursor + 1) % CHUNKS_PER_WAL;
      ww_cursor = next_chunk;
      guard.unlock();
      cv.notify_one();
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
    cv.notify_all();
  }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
