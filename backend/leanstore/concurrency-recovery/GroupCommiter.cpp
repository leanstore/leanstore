#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
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
void CRManager::groupCommiter()
{
   using Time = decltype(std::chrono::high_resolution_clock::now());
   [[maybe_unused]] Time phase_1_begin, phase_1_end, phase_2_begin, phase_2_end, write_begin, write_end;
   // -------------------------------------------------------------------------------------
   running_threads++;
   std::string thread_name("group_committer");
   pthread_setname_np(pthread_self(), thread_name.c_str());
   CPUCounters::registerThread(thread_name, false);
   // -------------------------------------------------------------------------------------
   [[maybe_unused]] u64 round_i = 0;  // For debugging
   u64 ssd_offset = end_of_block_device;
   // -------------------------------------------------------------------------------------
   // Async IO
   const u64 batch_max_size = (workers_count * 2) + 2;  // 2x because of potential wrapping around
   s32 io_slot = 0;
   std::unique_ptr<struct iocb[]> iocbs = make_unique<struct iocb[]>(batch_max_size);
   std::unique_ptr<struct iocb*[]> iocbs_ptr = make_unique<struct iocb*[]>(batch_max_size);
   std::unique_ptr<struct io_event[]> events = make_unique<struct io_event[]>(batch_max_size);
   io_context_t aio_context;
   {
      memset(&aio_context, 0, sizeof(aio_context));
      const int ret = io_setup(batch_max_size, &aio_context);
      if (ret != 0) {
         throw ex::GenericException("io_setup failed, ret code = " + std::to_string(ret));
      }
   }
   auto add_pwrite = [&](u8* src, u64 size, u64 offset) {
      ensure(offset % 512 == 0);
      ensure(u64(src) % 512 == 0);
      ensure(size % 512 == 0);
      io_prep_pwrite(&iocbs[io_slot], ssd_fd, src, size, offset);
      iocbs[io_slot].data = src;
      iocbs_ptr[io_slot] = &iocbs[io_slot];
      io_slot++;
   };
   // -------------------------------------------------------------------------------------
   LID min_all_workers_gsn;  // For Remote Flush Avoidance
   LID max_all_workers_gsn;  // Sync all workers to this point
   TXID min_all_workers_hardened_commit_ts;
   std::vector<u64> ready_to_commit_rfa_cut;  // Exclusive ) ==
   std::vector<Worker::Logging::WorkerToLW> wt_to_lw_copy;
   ready_to_commit_rfa_cut.resize(workers_count, 0);
   wt_to_lw_copy.resize(workers_count);
   // -------------------------------------------------------------------------------------
   while (keep_running) {
      io_slot = 0;
      round_i++;
      CRCounters::myCounters().gct_rounds++;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      min_all_workers_gsn = std::numeric_limits<LID>::max();
      max_all_workers_gsn = 0;
      min_all_workers_hardened_commit_ts = std::numeric_limits<TXID>::max();
      // -------------------------------------------------------------------------------------
      // Phase 1
      for (u32 w_i = 0; w_i < workers_count; w_i++) {
         Worker& worker = *workers[w_i];
         {
            {
               std::unique_lock<std::mutex> g(worker.logging.precommitted_queue_mutex);
               ready_to_commit_rfa_cut[w_i] = worker.logging.precommitted_queue_rfa.size();
            }
            wt_to_lw_copy[w_i] = worker.logging.wt_to_lw.getSync();
            // -------------------------------------------------------------------------------------
            max_all_workers_gsn = std::max<LID>(max_all_workers_gsn, wt_to_lw_copy[w_i].last_gsn);
            min_all_workers_gsn = std::min<LID>(min_all_workers_gsn, wt_to_lw_copy[w_i].last_gsn);
            min_all_workers_hardened_commit_ts = std::min<TXID>(min_all_workers_hardened_commit_ts, wt_to_lw_copy[w_i].precommitted_tx_commit_ts);
         }
         if (wt_to_lw_copy[w_i].wal_written_offset > worker.logging.wal_gct_cursor) {
            const u64 lower_offset = utils::downAlign(worker.logging.wal_gct_cursor);
            const u64 upper_offset = utils::upAlign(wt_to_lw_copy[w_i].wal_written_offset);
            const u64 size_aligned = upper_offset - lower_offset;
            // -------------------------------------------------------------------------------------
            if (FLAGS_wal_pwrite) {
               // TODO: add the concept of chunks
               ssd_offset -= size_aligned;
               add_pwrite(worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
            }
         } else if (wt_to_lw_copy[w_i].wal_written_offset < worker.logging.wal_gct_cursor) {
            {
               // ------------XXXXXXXXX
               const u64 lower_offset = utils::downAlign(worker.logging.wal_gct_cursor);
               const u64 upper_offset = Worker::WORKER_WAL_SIZE;
               const u64 size_aligned = upper_offset - lower_offset;
               // -------------------------------------------------------------------------------------
               if (FLAGS_wal_pwrite) {
                  ssd_offset -= size_aligned;
                  add_pwrite(worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               }
            }
            {
               // XXXXXX---------------
               const u64 lower_offset = 0;
               const u64 upper_offset = utils::upAlign(wt_to_lw_copy[w_i].wal_written_offset);
               const u64 size_aligned = upper_offset - lower_offset;
               // -------------------------------------------------------------------------------------
               if (FLAGS_wal_pwrite) {
                  ssd_offset -= size_aligned;
                  add_pwrite(worker.logging.wal_buffer, size_aligned, ssd_offset);
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
               }
            }
         }
      }
      // -------------------------------------------------------------------------------------
      // Phase 2
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         write_begin = phase_1_end;
      }
      // -------------------------------------------------------------------------------------
      // Flush
      if (FLAGS_wal_pwrite) {
         ensure(ssd_offset % 512 == 0);
         if (FLAGS_wal_pwrite) {
            u32 submitted = 0;
            u32 left = io_slot;
            while (left) {
               s32 ret_code = io_submit(aio_context, left, iocbs_ptr.get() + submitted);
               if (ret_code != s32(io_slot)) {
                  cout << ret_code << "," << io_slot << "," << ssd_offset << endl;
                  ensure(false);
               }
               posix_check(ret_code >= 0);
               submitted += ret_code;
               left -= ret_code;
            }
            {
               if (io_slot > 0) {
                  const s32 done_requests = io_getevents(aio_context, submitted, submitted, events.get(), NULL);
                  posix_check(done_requests >= 0);
               }
            }
            if (FLAGS_wal_fsync) {
               fdatasync(ssd_fd);
            }
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
      for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
         Worker& worker = *workers[w_i];
         worker.logging.hardened_commit_ts.store(wt_to_lw_copy[w_i].precommitted_tx_commit_ts, std::memory_order_release);
         TXID signaled_up_to = std::numeric_limits<TXID>::max();
         // TODO: prevent contention on mutex
         {
            worker.logging.wal_gct_cursor.store(wt_to_lw_copy[w_i].wal_written_offset, std::memory_order_release);
            std::unique_lock<std::mutex> g(worker.logging.precommitted_queue_mutex);
            // -------------------------------------------------------------------------------------
            u64 tx_i = 0;
            for (tx_i = 0;
                 tx_i < worker.logging.precommitted_queue.size() && worker.logging.precommitted_queue[tx_i].max_observed_gsn <= min_all_workers_gsn &&
                 worker.logging.precommitted_queue[tx_i].start_ts <= min_all_workers_hardened_commit_ts;
                 tx_i++) {
               worker.logging.precommitted_queue[tx_i].state = Transaction::STATE::COMMITTED;
            }
            if (tx_i > 0) {
               signaled_up_to = std::min<TXID>(signaled_up_to, worker.logging.precommitted_queue[tx_i - 1].commitTS());
               worker.logging.precommitted_queue.erase(worker.logging.precommitted_queue.begin(), worker.logging.precommitted_queue.begin() + tx_i);
               committed_tx += tx_i;
            }
            // -------------------------------------------------------------------------------------
            for (tx_i = 0; tx_i < ready_to_commit_rfa_cut[w_i]; tx_i++) {
               worker.logging.precommitted_queue_rfa[tx_i].state = Transaction::STATE::COMMITTED;
            }
            if (tx_i > 0) {
               signaled_up_to = std::min<TXID>(signaled_up_to, worker.logging.precommitted_queue_rfa[tx_i - 1].commitTS());
               worker.logging.precommitted_queue_rfa.erase(worker.logging.precommitted_queue_rfa.begin(),
                                                           worker.logging.precommitted_queue_rfa.begin() + tx_i);
               committed_tx += tx_i;
            }
         }
         if (signaled_up_to < std::numeric_limits<TXID>::max() && signaled_up_to > 0) {
            worker.logging.signaled_commit_ts.store(signaled_up_to, std::memory_order_release);
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
      // -------------------------------------------------------------------------------------
      assert(Worker::Logging::global_min_gsn_flushed.load() <= min_all_workers_gsn);
      Worker::Logging::global_min_gsn_flushed.store(min_all_workers_gsn, std::memory_order_release);
      Worker::Logging::global_sync_to_this_gsn.store(max_all_workers_gsn, std::memory_order_release);
   }
   running_threads--;
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
