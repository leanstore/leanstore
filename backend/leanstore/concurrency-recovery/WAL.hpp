#pragma once
#include "Exceptions.hpp"
#include "Units.hpp"
#include "WALWriter.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct WALEntry {
  enum class TYPE : u8 { USER_TX_START, SYSTEM_TX_START, DT_SPECIFIC, TX_COMMIT, TX_ABORT };
  // -------------------------------------------------------------------------------------
  u16 size;
  TYPE type;
  DTID dt_id;
  LID lsn;
  LID gsn;
  PID pid;
  u8 payload[];
};
// -------------------------------------------------------------------------------------
struct WALChunk {
  static constexpr u64 CHUNK_SIZE = (1024ull * 1024 * 100);  // 100 MiB
  static constexpr u64 CHUNK_SIZE_PURE = CHUNK_SIZE - (8 + 8 + 8);
  struct alignas(512) DiskImage {
    u64 partition_id;
    u64 partition_next_chunk_offset;
    u64 chunk_size;
    u8 buffer[CHUNK_SIZE_PURE];
  };
  static_assert(sizeof(DiskImage) == CHUNK_SIZE, "chunk size problem");
  DiskImage disk_image;
  u64 free_space = CHUNK_SIZE_PURE;
  u8* write_ptr = disk_image.buffer;
  void reset()
  {
    free_space = CHUNK_SIZE_PURE;
    write_ptr = disk_image.buffer;
  }
};
// -------------------------------------------------------------------------------------
struct WAL {
 private:
  atomic<bool> ww_thread_keep_running = true;
  atomic<bool> ww_thread_running = true;
  u64 partition_id;

 public:
  static constexpr u64 CHUNKS_PER_WAL = 5;
  WALChunk chunks[CHUNKS_PER_WAL];
  WALChunk* current_chunk = chunks + 0;
  // -------------------------------------------------------------------------------------
  std::mutex mutex;
  std::condition_variable cv;
  u64 wt_cursor = 0;  // Worker thread cursor
  u64 ww_cursor = 0;  // WAL Writer cursor
  // -------------------------------------------------------------------------------------
  LID lsn_counter = 0;
  LID max_gsn = 0;
  // -------------------------------------------------------------------------------------
  void nextChunk()
  {
    std::unique_lock<std::mutex> guard(mutex);
    u64 next_chunk = (wt_cursor + 1) % CHUNKS_PER_WAL;
    cv.wait(guard, [&] { return next_chunk != ww_cursor; });
    wt_cursor = next_chunk;
    current_chunk = chunks + next_chunk;
    guard.unlock();
    cv.notify_one();
  }
  // -------------------------------------------------------------------------------------
  inline u8* reserve(u64 requested_size)
  {
    if (current_chunk->free_space >= requested_size) {
      return current_chunk->write_ptr;
    } else {
      nextChunk();
      assert(current_chunk->free_space >= requested_size);
      return current_chunk->write_ptr;
    }
  }
  // -------------------------------------------------------------------------------------
  inline void submit(u64 reserved_size)
  {
    assert(current_chunk->free_space >= reserved_size);
    current_chunk->write_ptr += reserved_size;
    current_chunk->free_space -= reserved_size;
  };
  // -------------------------------------------------------------------------------------
  WAL(u64 partition_id);
  ~WAL();
};  // namespace cr
}  // namespace cr
}  // namespace leanstore
