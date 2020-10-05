#include "WAL.hpp"

#include "WALWriter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
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
  if (FLAGS_wal) {
    std::thread ww_thread([&, partition_id]() {
      pthread_setname_np(pthread_self(), "ww");
      // const u64 t_i = CPUCounters::registerThread("ww_" + std::to_string(partition_id));
      ww_thread_running = true;
      while (ww_thread_keep_running) {
        std::unique_lock<std::mutex> guard(mutex);
        cv.wait(guard, [&] { return (ww_thread_keep_running == false) || (ww_cursor != wt_cursor); });
        guard.unlock();
        chunks[ww_cursor].disk_image.partition_id = partition_id;
        chunks[ww_cursor].disk_image.chunk_size = WALChunk::CHUNK_SIZE - chunks[ww_cursor].free_space;
        chunks[ww_cursor].disk_image.partition_next_chunk_offset = 0;  // TODO:
        WALWriter::write(reinterpret_cast<u8*>(&chunks[ww_cursor].disk_image), WALChunk::CHUNK_SIZE);
        // -------------------------------------------------------------------------------------
        CRCounters::myCounters().written_log_bytes += WALChunk::CHUNK_SIZE;
        // -------------------------------------------------------------------------------------
        chunks[ww_cursor].reset();
        guard.lock();
        const u64 next_chunk = (ww_cursor + 1) % CHUNKS_PER_WAL;
        ww_cursor = next_chunk;
        guard.unlock();
        cv.notify_one();
      }
      ww_thread_running = false;
      // CPUCounters::removeThread(t_i);
    });
    ww_thread.detach();
  }
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
