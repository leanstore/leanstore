#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
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
struct WALChunk {
   u32 size;
   u8 workers_count;
   u8 data[];
};
// -------------------------------------------------------------------------------------
void CRManager::groupCommiter()
{
   running_threads++;
   std::string thread_name("group_committer");
   pthread_setname_np(pthread_self(), thread_name.c_str());
   // -------------------------------------------------------------------------------------
   CPUCounters::registerThread(thread_name, false);
   // -------------------------------------------------------------------------------------
   unique_ptr<u8[]> buffer = make_unique<u8[]>((Worker::WORKER_WAL_SIZE * workers_count) + sizeof(WALChunk));
   auto& chunk = *reinterpret_cast<WALChunk* const>(buffer.get());
   auto index = reinterpret_cast<u64* const>(chunk.data);
   u64 ssd_offset = end_of_block_device;
   const auto log_begin = reinterpret_cast<u8*>(index + workers_count);
   while (keep_running) {
      auto log_ptr = log_begin;
      // -------------------------------------------------------------------------------------
      // Phase 1
      for (s32 w_i = 0; w_i < s32(workers_count); w_i++) {
         Worker& worker = *workers[w_i];
         {
            std::unique_lock<std::mutex> g(worker.worker_group_commiter_mutex);
            worker.group_commit_data.ready_to_commit_cut = worker.ready_to_commit_queue.size();
            worker.group_commit_data.gsn_to_flush = worker.wal_max_gsn;
            worker.group_commit_data.wt_cursor_to_flush = worker.wal_wt_cursor;
         }
         {
            index[w_i] = log_ptr - buffer.get();
            {
               const u32 size = worker.group_commit_data.wt_cursor_to_flush - worker.wal_ww_cursor;
               std::memcpy(log_ptr, worker.wal_buffer + worker.wal_ww_cursor, size);
               log_ptr += size;
            }
            if (worker.group_commit_data.wt_cursor_to_flush < worker.wal_ww_cursor) {
               // copy the rest
               const u32 size = worker.group_commit_data.wt_cursor_to_flush;
               std::memcpy(log_ptr, worker.wal_buffer, size);
               log_ptr += size;
            }
         }
         for (s32 p_w_i = w_i - 1; p_w_i >= 0; p_w_i--) {
            Worker& p_worker = *workers[p_w_i];
            if (p_worker.wal_max_gsn > p_worker.group_commit_data.gsn_to_flush) {
               worker.group_commit_data.max_safe_gsn_to_commit =
                   std::min(p_worker.group_commit_data.gsn_to_flush, worker.group_commit_data.max_safe_gsn_to_commit);
            }
         }
      }
      if (log_ptr != log_begin) {
         const u32 size = log_ptr - buffer.get();
         ssd_offset -= size;
         pwrite(ssd_fd, buffer.get(), size, ssd_offset);
         fdatasync(ssd_fd);
      }
      // Phase 2, commit
      for (s32 w_i = 0; w_i < s32(workers_count); w_i++) {
         Worker& worker = *workers[w_i];
         {
            std::unique_lock<std::mutex> g(worker.worker_group_commiter_mutex);
            worker.wal_ww_cursor = worker.group_commit_data.wt_cursor_to_flush;
            u64 tx_i = 0;
            while (tx_i < worker.group_commit_data.ready_to_commit_cut) {
               if (worker.ready_to_commit_queue[tx_i].max_gsn < worker.group_commit_data.max_safe_gsn_to_commit) {
                  worker.ready_to_commit_queue[tx_i].state = Transaction::STATE::COMMITED;
                  cout << "Committing: " << worker.ready_to_commit_queue[tx_i].tx_id << endl;
                  // TODO: commit for real
                  tx_i++;
               } else {
                  break;
               }
            }
            worker.ready_to_commit_queue.erase(worker.ready_to_commit_queue.begin(), worker.ready_to_commit_queue.begin() + tx_i);
         }
      }
      //   std::this_thread::sleep_for(1s);
   }
   running_threads--;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
