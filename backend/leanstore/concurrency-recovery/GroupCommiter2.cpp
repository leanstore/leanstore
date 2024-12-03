#include "CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
// -------------------------------------------------------------------------------------
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
void CRManager::groupCommiter2()
{
   std::thread log_committer([&]() {
      running_threads++;
      std::string thread_name("log_committer");
      pthread_setname_np(pthread_self(), thread_name.c_str());
      CPUCounters::registerThread(thread_name, false);
      // -------------------------------------------------------------------------------------
      while (keep_running) {
         u64 committed_tx = 0;
         for (WORKERID w_i = 0; w_i < workers_count; w_i++) {
            Worker& worker = *workers[w_i];
            const auto time_now = std::chrono::high_resolution_clock::now();
            std::unique_lock<std::mutex> g(worker.logging.precommitted_queue_mutex);
            // -------------------------------------------------------------------------------------
            u64 tx_i = 0;
            TXID signaled_up_to = std::numeric_limits<TXID>::max();
            for (tx_i = 0; tx_i < worker.logging.precommitted_queue.size(); tx_i++) {
               auto& tx = worker.logging.precommitted_queue[tx_i];
               if (tx.max_observed_gsn > Worker::Logging::global_min_gsn_flushed || tx.start_ts > Worker::Logging::global_min_commit_ts_flushed) {
                  tx.stats.flushes_counter++;
                  break;
               }
               tx.state = Transaction::STATE::COMMITTED;
               tx.stats.commit = time_now;
               if (1) {
                  const u64 cursor = CRCounters::myCounters().cc_latency_cursor++ % CRCounters::latency_tx_capacity;
                  CRCounters::myCounters().cc_ms_precommit_latency[cursor] =
                      std::chrono::duration_cast<std::chrono::microseconds>(tx.stats.precommit - tx.stats.start).count();
                  CRCounters::myCounters().cc_ms_commit_latency[cursor] =
                      std::chrono::duration_cast<std::chrono::microseconds>(tx.stats.commit - tx.stats.start).count();
                  CRCounters::myCounters().cc_flushes_counter[cursor] += tx.stats.flushes_counter;
               }
            }
            if (tx_i > 0) {
               signaled_up_to = std::min<TXID>(signaled_up_to, worker.logging.precommitted_queue[tx_i - 1].commitTS());
               worker.logging.precommitted_queue.erase(worker.logging.precommitted_queue.begin(), worker.logging.precommitted_queue.begin() + tx_i);
               committed_tx += tx_i;
            }
         }
         // -------------------------------------------------------------------------------------
         // CRCounters::myCounters().gct_rounds += 1;
      }
      running_threads--;
   });
   log_committer.detach();
   // -------------------------------------------------------------------------------------
   std::vector<std::thread> writer_threads;
   for (u64 t_i = 0; t_i < workers_count; t_i++) {
      writer_threads.emplace_back([&, t_i]() {
         // u64 ssd_offset = end_of_block_device - (t_i * 1024 * 1024 * 1024);
         const WORKERID workers_range_size = 1;
         const WORKERID w_i = t_i;
         running_threads++;
         std::string thread_name("log_writer_" + std::to_string(t_i));
         pthread_setname_np(pthread_self(), thread_name.c_str());
         CPUCounters::registerThread(thread_name, false);
         // -------------------------------------------------------------------------------------
         [[maybe_unused]] u64 round_i = 0;  // For debugging
         // -------------------------------------------------------------------------------------
         // Async IO
         std::vector<u64> ready_to_commit_rfa_cut;  // Exclusive ) ==
         std::vector<Worker::Logging::WorkerToLW> wt_to_lw_copy;
         ready_to_commit_rfa_cut.resize(workers_range_size, 0);
         wt_to_lw_copy.resize(workers_range_size);
         // -------------------------------------------------------------------------------------
         while (keep_running) {
            round_i++;
            // -------------------------------------------------------------------------------------
            // Phase 1
            {
               Worker& worker = *workers[w_i];
               {
                  {
                     std::unique_lock<std::mutex> g(worker.logging.precommitted_queue_mutex);
                     ready_to_commit_rfa_cut[0] = worker.logging.precommitted_queue_rfa.size();
                  }
                  wt_to_lw_copy[0] = worker.logging.wt_to_lw.getSync();
               }
               if (wt_to_lw_copy[0].wal_written_offset > worker.logging.wal_gct_cursor) {
                  const u64 lower_offset = utils::downAlign(worker.logging.wal_gct_cursor);
                  const u64 upper_offset = utils::upAlign(wt_to_lw_copy[0].wal_written_offset);
                  const u64 size_aligned = upper_offset - lower_offset;
                  // -------------------------------------------------------------------------------------
                  if (FLAGS_wal_pwrite) {
                     // TODO: add the concept of chunks
                     const u64 ssd_offset = g_ssd_offset.fetch_add(-size_aligned) - size_aligned;
                     pwrite(ssd_fd, worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
                     // add_pwrite(worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
                     // -------------------------------------------------------------------------------------
                     COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                  }
               } else if (wt_to_lw_copy[0].wal_written_offset < worker.logging.wal_gct_cursor) {
                  {
                     // ------------XXXXXXXXX
                     const u64 lower_offset = utils::downAlign(worker.logging.wal_gct_cursor);
                     const u64 upper_offset = FLAGS_wal_buffer_size;
                     const u64 size_aligned = upper_offset - lower_offset;
                     // -------------------------------------------------------------------------------------
                     if (FLAGS_wal_pwrite) {
                        const u64 ssd_offset = g_ssd_offset.fetch_add(-size_aligned) - size_aligned;
                        pwrite(ssd_fd, worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
                        // add_pwrite(worker.logging.wal_buffer + lower_offset, size_aligned, ssd_offset);
                        // -------------------------------------------------------------------------------------
                        COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                     }
                  }
                  {
                     // XXXXXX---------------
                     const u64 lower_offset = 0;
                     const u64 upper_offset = utils::upAlign(wt_to_lw_copy[0].wal_written_offset);
                     const u64 size_aligned = upper_offset - lower_offset;
                     // -------------------------------------------------------------------------------------
                     if (FLAGS_wal_pwrite) {
                        const u64 ssd_offset = g_ssd_offset.fetch_add(-size_aligned) - size_aligned;
                        pwrite(ssd_fd, worker.logging.wal_buffer, size_aligned, ssd_offset);
                        // add_pwrite(worker.logging.wal_buffer, size_aligned, ssd_offset);
                        // -------------------------------------------------------------------------------------
                        COUNTERS_BLOCK() { CRCounters::myCounters().gct_write_bytes += size_aligned; }
                     }
                  }
               } else if (wt_to_lw_copy[0].wal_written_offset == worker.logging.wal_gct_cursor) {
                  if (FLAGS_tmp7) {
                     worker.logging.wt_to_lw.wait(wt_to_lw_copy[0]);
                  }
               }
            }
            // -------------------------------------------------------------------------------------
            // Flush
            if (FLAGS_wal_fsync) {
               ensure(g_ssd_offset % 512 == 0);
               const u64 fsync_current_value = fsync_counter.load();
               fsync_counter.wait(fsync_current_value);
               while ((fsync_current_value + 2) >= fsync_counter.load() && keep_running) {
               }
            }
            // -------------------------------------------------------------------------------------
            // Phase 2, commit
            u64 committed_tx = 0;
            {
               Worker& worker = *workers[w_i];
               worker.logging.hardened_commit_ts.store(wt_to_lw_copy[0].precommitted_tx_commit_ts, std::memory_order_release);
               worker.logging.hardened_gsn.store(wt_to_lw_copy[0].last_gsn, std::memory_order_release);
               TXID signaled_up_to = std::numeric_limits<TXID>::max();
               // TODO: prevent contention on mutex
               {
                  worker.logging.wal_gct_cursor.store(wt_to_lw_copy[0].wal_written_offset, std::memory_order_release);
                  const auto time_now = std::chrono::high_resolution_clock::now();
                  std::unique_lock<std::mutex> g(worker.logging.precommitted_queue_mutex);
                  // -------------------------------------------------------------------------------------
                  // RFA
                  u64 tx_i = 0;
                  for (tx_i = 0; tx_i < ready_to_commit_rfa_cut[0]; tx_i++) {
                     auto& tx = worker.logging.precommitted_queue_rfa[tx_i];
                     tx.state = Transaction::STATE::COMMITTED;
                     tx.stats.commit = time_now;
                     if (1) {
                        const u64 cursor = CRCounters::myCounters().cc_rfa_latency_cursor++ % CRCounters::latency_tx_capacity;
                        CRCounters::myCounters().cc_rfa_ms_precommit_latency[cursor] =
                            std::chrono::duration_cast<std::chrono::microseconds>(tx.stats.precommit - tx.stats.start).count();
                        CRCounters::myCounters().cc_rfa_ms_commit_latency[cursor] =
                            std::chrono::duration_cast<std::chrono::microseconds>(tx.stats.commit - tx.stats.start).count();
                     }
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
            // CRCounters::myCounters().gct_rounds += 1;
            // CRCounters::myCounters().gct_rounds += t_i == 0;
         }
         running_threads--;
      });
   }
   for (auto& thread : writer_threads) {
      thread.detach();
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
