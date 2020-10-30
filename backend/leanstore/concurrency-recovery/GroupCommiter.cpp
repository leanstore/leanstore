#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>
// -------------------------------------------------------------------------------------
using namespace std::chrono_literals;
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
struct alignas(512) SSDMeta {
   u64 last_written_chunk;
};
// -------------------------------------------------------------------------------------
struct alignas(512) WALChunk {
   static constexpr u16 STATIC_MAX_WORKERS = 256;
   struct Slot {
      u64 offset;
      u64 length;
   };
   u8 workers_count;
   u32 total_size;
   Slot slot[STATIC_MAX_WORKERS];
   u8 data[];
};
// -------------------------------------------------------------------------------------
inline u64 upAlign(u64 x)
{
   return (x + 511) & ~511ul;
}
// -------------------------------------------------------------------------------------
inline u64 downAlign(u64 x)
{
   return x - (x & 511);
}
// -------------------------------------------------------------------------------------
void CRManager::groupCommiter()
{
   using Time = decltype(std::chrono::high_resolution_clock::now());
   [[maybe_unused]] Time phase_1_begin, phase_1_end, phase_2_begin, phase_2_end, write_begin, write_end;
   // -------------------------------------------------------------------------------------
   running_threads++;
   std::string thread_name("group_committer");
   pthread_setname_np(pthread_self(), thread_name.c_str());
   // -------------------------------------------------------------------------------------
   auto tid = syscall(__NR_gettid);
   cout << "GCT: " << tid << endl;
   // -------------------------------------------------------------------------------------
   CPUCounters::registerThread(thread_name, false);
   // -------------------------------------------------------------------------------------
   WALChunk chunk;
   SSDMeta meta;
   [[maybe_unused]] u64 round_i = 0;  // For debugging
   const u64 meta_offset = end_of_block_device - sizeof(SSDMeta);
   u64* index = reinterpret_cast<u64*>(chunk.data);
   u64 ssd_offset = end_of_block_device - sizeof(SSDMeta);
   // -------------------------------------------------------------------------------------
   LID max_safe_gsn;
   // -------------------------------------------------------------------------------------
   while (keep_running) {
      round_i = 0;
      CRCounters::myCounters().gct_rounds++;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      max_safe_gsn = std::numeric_limits<LID>::max();
      // -------------------------------------------------------------------------------------
      // Phase 1
      for (u32 w_i = 0; w_i < workers_count; w_i++) {
         Worker& worker = *workers[w_i];
         {
            std::unique_lock<std::mutex> g(worker.worker_group_commiter_mutex);
            worker.group_commit_data.ready_to_commit_cut = worker.ready_to_commit_queue.size();
            worker.group_commit_data.gsn_to_flush = worker.wal_max_gsn;
            worker.group_commit_data.wt_cursor_to_flush = worker.wal_wt_cursor;
         }
         {
            if (worker.group_commit_data.wt_cursor_to_flush > worker.wal_ww_cursor) {
               const u64 size = worker.group_commit_data.wt_cursor_to_flush - worker.wal_ww_cursor;
               const u64 size_aligned = upAlign(size);
               ssd_offset -= size_aligned;
               if (!FLAGS_wal_io_hack) {
                  const u64 ret = pwrite(ssd_fd, worker.wal_buffer + worker.wal_ww_cursor, size_aligned, ssd_offset);
                  posix_check(ret == size_aligned);
               }
               COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               chunk.total_size += size_aligned;
               {
                  chunk.slot[w_i].offset = ssd_offset + worker.group_commit_data.bytes_to_ignore_in_the_next_round;
                  chunk.slot[w_i].length = size - worker.group_commit_data.bytes_to_ignore_in_the_next_round;
                  const u64 down_aligned = downAlign(worker.group_commit_data.wt_cursor_to_flush);
                  worker.group_commit_data.bytes_to_ignore_in_the_next_round = worker.group_commit_data.wt_cursor_to_flush - down_aligned;
                  worker.group_commit_data.wt_cursor_to_flush = down_aligned;
               }
            } else if (worker.group_commit_data.wt_cursor_to_flush < worker.wal_ww_cursor) {
               u64 total_size = 0;
               {
                  ensure((Worker::WORKER_WAL_SIZE - worker.wal_ww_cursor) % 512 == 0);
                  const u64 size = Worker::WORKER_WAL_SIZE - worker.wal_ww_cursor;
                  total_size += size;
                  ssd_offset -= size;
                  if (!FLAGS_wal_io_hack) {
                     const u64 ret = pwrite(ssd_fd, worker.wal_buffer + worker.wal_ww_cursor, size, ssd_offset);
                     posix_check(ret == size);
                  }
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size; }
                  chunk.total_size += size;
               }
               {
                  // copy the rest
                  const u64 size = worker.group_commit_data.wt_cursor_to_flush;
                  const u64 size_aligned = upAlign(size);
                  total_size += size;
                  ssd_offset -= size_aligned;
                  if (!FLAGS_wal_io_hack) {
                     const u64 ret = pwrite(ssd_fd, worker.wal_buffer, size_aligned, ssd_offset);
                     posix_check(ret == size_aligned);
                  }
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                  chunk.total_size += size_aligned;
               }
               {
                  chunk.slot[w_i].offset = ssd_offset + worker.group_commit_data.bytes_to_ignore_in_the_next_round;
                  chunk.slot[w_i].length = total_size - worker.group_commit_data.bytes_to_ignore_in_the_next_round;
                  const u64 down_aligned = downAlign(worker.group_commit_data.wt_cursor_to_flush);
                  worker.group_commit_data.bytes_to_ignore_in_the_next_round = worker.group_commit_data.wt_cursor_to_flush - down_aligned;
                  worker.group_commit_data.wt_cursor_to_flush = down_aligned;
               }
            }
         }
         // -------------------------------------------------------------------------------------
         index[w_i] = ssd_offset;
      }
      // -------------------------------------------------------------------------------------
      if (workers[0]->wal_max_gsn > workers[0]->group_commit_data.max_safe_gsn_to_commit) {
         max_safe_gsn = std::min<LID>(workers[0]->group_commit_data.gsn_to_flush, max_safe_gsn);
      }
      for (u32 w_i = 1; w_i < workers_count; w_i++) {
         Worker& worker = *workers[w_i];
         worker.group_commit_data.max_safe_gsn_to_commit = std::min<LID>(worker.group_commit_data.max_safe_gsn_to_commit, max_safe_gsn);
         if (worker.wal_max_gsn > worker.group_commit_data.gsn_to_flush) {
            max_safe_gsn = std::min<LID>(max_safe_gsn, worker.group_commit_data.gsn_to_flush);
         }
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         write_begin = phase_1_end;
      }
      if (chunk.total_size) {
         ensure(ssd_offset % 512 == 0);
         ssd_offset -= sizeof(WALChunk);
         for (u64 w_i = 0; w_i < workers_count; w_i++) {
            chunk.slot[w_i].offset -= ssd_offset;  // Make it relative to the beginning of SSD
         }
         if (!FLAGS_wal_io_hack) {
            const u64 ret = pwrite(ssd_fd, &chunk, sizeof(WALChunk), ssd_offset);
            posix_check(ret == sizeof(WALChunk));
            fdatasync(ssd_fd);
         }
         // -------------------------------------------------------------------------------------
         meta.last_written_chunk = ssd_offset;
         if (!FLAGS_wal_io_hack) {
            const u64 ret = pwrite(ssd_fd, &meta, sizeof(SSDMeta), meta_offset);
            ensure(ret == sizeof(SSDMeta));
         }
         if (!FLAGS_wal_io_hack) {
            fdatasync(ssd_fd);
         }
      }
      COUNTERS_BLOCK()
      {
         write_end = std::chrono::high_resolution_clock::now();
         phase_2_begin = write_end;
      }
      // Phase 2, commit
      u64 committed_tx = 0;
      for (s32 w_i = 0; w_i < s32(workers_count); w_i++) {
         Worker& worker = *workers[w_i];
         {
            u64 high_water_mark = 0;
            {
               u64 tx_i = 0;
               std::unique_lock<std::mutex> g(worker.worker_group_commiter_mutex);
               worker.wal_ww_cursor.store(worker.group_commit_data.wt_cursor_to_flush, std::memory_order_relaxed);
               while (tx_i < worker.group_commit_data.ready_to_commit_cut) {
                  if (worker.ready_to_commit_queue[tx_i].max_gsn < worker.group_commit_data.max_safe_gsn_to_commit) {
                     worker.ready_to_commit_queue[tx_i].state = Transaction::STATE::COMMITED;
                     committed_tx++;
                     tx_i++;
                  } else {
                     break;
                  }
               }
               if (tx_i)
                  high_water_mark = worker.ready_to_commit_queue[tx_i - 1].tx_id;
               worker.ready_to_commit_queue.erase(worker.ready_to_commit_queue.begin(), worker.ready_to_commit_queue.begin() + tx_i);
               worker.group_commit_data.max_safe_gsn_to_commit = std::numeric_limits<u64>::max();
            }
            worker.high_water_mark.store(high_water_mark, std::memory_order_release);
         }
      }
      CRCounters::myCounters().gct_committed_tx += committed_tx;
      COUNTERS_BLOCK()
      {
         phase_2_end = std::chrono::high_resolution_clock::now();
         CRCounters::myCounters().gct_phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
         CRCounters::myCounters().gct_phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
         CRCounters::myCounters().gct_write_ms += (std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_begin).count());
      }
   }
   running_threads--;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
