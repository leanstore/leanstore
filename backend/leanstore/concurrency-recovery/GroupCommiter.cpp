#include "CRMG.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
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
      round_i++;
      CRCounters::myCounters().gct_rounds++;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      max_safe_gsn = std::numeric_limits<LID>::max();
      chunk.total_size = sizeof(WALChunk);
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
            auto& wal_entry = *reinterpret_cast<WALEntry*>(worker.wal_buffer + worker.wal_ww_cursor);
            worker.group_commit_data.first_lsn_in_chunk = wal_entry.lsn;
         }
         {
            if (worker.group_commit_data.wt_cursor_to_flush > worker.wal_ww_cursor) {
               const u64 lower_offset = utils::downAlign(worker.wal_ww_cursor);
               const u64 upper_offset = utils::upAlign(worker.group_commit_data.wt_cursor_to_flush);
               const u64 size = worker.group_commit_data.wt_cursor_to_flush - worker.wal_ww_cursor;
               const u64 size_aligned = upper_offset - lower_offset;
               // -------------------------------------------------------------------------------------
               ssd_offset -= size_aligned;
               if (!FLAGS_wal_io_hack) {
                  const u64 ret = pwrite(ssd_fd, worker.wal_buffer + lower_offset, size_aligned, ssd_offset);
                  posix_check(ret == size_aligned);
               }
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               chunk.slot[w_i].offset = ssd_offset + (worker.wal_ww_cursor - lower_offset);
               chunk.slot[w_i].length = size;
               assert(chunk.slot[w_i].offset < end_of_block_device);
               chunk.total_size += size_aligned;
               // DEBUG_BLOCK()
               // {
               //    auto& slot = chunk.slot[w_i];
               //    const u64 offset = chunk.slot[w_i].offset;
               //    const u64 offset_aligned = utils::downAlign(chunk.slot[w_i].offset);
               //    const u64 size_aligned = utils::upAlign(slot.length + offset - offset_aligned);
               //    auto buffer = static_cast<u8*>(std::aligned_alloc(512, size_aligned));
               //    const u64 ret = pread(ssd_fd, buffer, size_aligned, offset_aligned);
               //    posix_check(ret == size_aligned);
               //    u8* ptr = buffer + offset - offset_aligned;
               //    u64 traverse_offset = 0;
               //    auto entry = reinterpret_cast<WALEntry*>(ptr);
               //    while (true) {
               //       if (entry->magic_debugging_number != 99) {
               //          auto tmp = reinterpret_cast<WALEntry*>(worker.wal_buffer + lower_offset + traverse_offset);
               //          ensure(entry->magic_debugging_number == 99);
               //       }
               //       if ((traverse_offset + entry->size) < slot.length) {
               //          traverse_offset += entry->size;
               //          entry = reinterpret_cast<WALEntry*>(ptr + traverse_offset);
               //       } else {
               //          break;
               //       }
               //    }
               //    std::free(buffer);
               // }
               ensure(chunk.slot[w_i].offset >= ssd_offset);
            } else if (worker.group_commit_data.wt_cursor_to_flush < worker.wal_ww_cursor) {
               {
                  // XXXXXX---------------
                  const u64 lower_offset = 0;
                  const u64 upper_offset = utils::upAlign(worker.group_commit_data.wt_cursor_to_flush);
                  const u64 size = worker.group_commit_data.wt_cursor_to_flush;
                  const u64 size_aligned = upper_offset - lower_offset;
                  // -------------------------------------------------------------------------------------
                  ssd_offset -= size_aligned;
                  if (!FLAGS_wal_io_hack) {
                     const u64 ret = pwrite(ssd_fd, worker.wal_buffer, size_aligned, ssd_offset);
                     posix_check(ret == size_aligned);
                  }
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                  chunk.slot[w_i].length = size;
               }
               {
                  // ------------XXXXXXXXX
                  const u64 lower_offset = utils::downAlign(worker.wal_ww_cursor);
                  const u64 upper_offset = Worker::WORKER_WAL_SIZE;
                  const u64 size = Worker::WORKER_WAL_SIZE - worker.wal_ww_cursor;
                  const u64 size_aligned = upper_offset - lower_offset;
                  // -------------------------------------------------------------------------------------
                  ssd_offset -= size_aligned;
                  if (!FLAGS_wal_io_hack) {
                     const u64 ret = pwrite(ssd_fd, worker.wal_buffer + lower_offset, size_aligned, ssd_offset);
                     posix_check(ret == size_aligned);
                  }
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                  chunk.slot[w_i].offset = ssd_offset + (worker.wal_ww_cursor - lower_offset);
                  chunk.slot[w_i].length += size;
               }
               ensure(chunk.slot[w_i].offset >= ssd_offset);
            } else {
               chunk.slot[w_i].offset = 0;
               chunk.slot[w_i].length = 0;
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
      // -------------------------------------------------------------------------------------
      // Flush
      if (0 && chunk.total_size > sizeof(WALChunk)) {
         ensure(ssd_offset % 512 == 0);
         ssd_offset -= sizeof(WALChunk);
         if (!FLAGS_wal_io_hack) {
            const u64 ret = pwrite(ssd_fd, &chunk, sizeof(WALChunk), ssd_offset);
            posix_check(ret == sizeof(WALChunk));
            if (FLAGS_wal_fsync) {
               fdatasync(ssd_fd);
            }
         }
         // -------------------------------------------------------------------------------------
         meta.last_written_chunk = ssd_offset;
         if (!FLAGS_wal_io_hack) {
            const u64 ret = pwrite(ssd_fd, &meta, sizeof(SSDMeta), meta_offset);
            ensure(ret == sizeof(SSDMeta));
         }
         if (!FLAGS_wal_io_hack && FLAGS_wal_fsync) {
            fdatasync(ssd_fd);
         }
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK()
      {
         write_end = std::chrono::high_resolution_clock::now();
         phase_2_begin = write_end;
      }
      // -------------------------------------------------------------------------------------
      // Phase 2, commit
      u64 committed_tx = 0;
      for (s32 w_i = 0; w_i < s32(workers_count); w_i++) {
         Worker& worker = *workers[w_i];
         {
            u64 tx_i = 0;
            std::unique_lock<std::mutex> g(worker.worker_group_commiter_mutex);
            if (chunk.slot[w_i].offset) {
               worker.wal_finder.insertJumpPoint(worker.group_commit_data.first_lsn_in_chunk, chunk.slot[w_i]);
            }
            // -------------------------------------------------------------------------------------
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
            if (tx_i > 0) {
               const u64 high_water_mark = worker.ready_to_commit_queue[tx_i - 1].tts + 1;
               worker.high_water_mark.store(high_water_mark, std::memory_order_release);
               // cout << "HighWaterMark[" << w_i << "] = " << worker.high_water_mark << endl;
            }
            worker.ready_to_commit_queue.erase(worker.ready_to_commit_queue.begin(), worker.ready_to_commit_queue.begin() + tx_i);
            worker.group_commit_data.max_safe_gsn_to_commit = std::numeric_limits<u64>::max();
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
