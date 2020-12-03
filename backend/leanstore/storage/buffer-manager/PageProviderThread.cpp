#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderThread(u64 p_begin, u64 p_end)  // [p_begin, p_end)
{
   pthread_setname_np(pthread_self(), "page_provider");
   using Time = decltype(std::chrono::high_resolution_clock::now());
   // -------------------------------------------------------------------------------------
   // TODO: register as special worker
   cr::Worker::tls_ptr = new cr::Worker(0, nullptr, 0, ssd_fd);
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
   // -------------------------------------------------------------------------------------
   auto phase_1_condition = [&](Partition& p) { return (p.dram_free_list.counter + p.cooling_bfs_counter) < p.cooling_bfs_limit; };  //
   auto phase_2_3_condition = [&](Partition& p) { return (p.dram_free_list.counter < p.free_bfs_limit); };
   // -------------------------------------------------------------------------------------
   while (bg_threads_keep_running) {
      /*
       * Phase 1: unswizzle pages (put in the cooling stage)
       */
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      BufferFrame* volatile r_buffer = &randomBufferFrame();  // Attention: we may set the r_buffer to a child of a bf instead of random
      volatile u64 failed_attempts =
          0;  // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
#define repickIf(cond)                 \
   if (cond) {                         \
      r_buffer = &randomBufferFrame(); \
      failed_attempts++;               \
      continue;                        \
   }
      while (true) {
         jumpmuTry()
         {
            while (phase_1_condition(randomPartition()) && failed_attempts < 10) {
               COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
               OptimisticGuard r_guard(r_buffer->header.latch, true);
               // -------------------------------------------------------------------------------------
               // Performance crticial: we should cross cool (unswizzle), otherwise write performance will drop
               [[maybe_unused]] const u64 partition_i = getPartitionID(r_buffer->header.pid);
               const bool is_cooling_candidate = (!r_buffer->header.keep_in_memory && !r_buffer->header.isWB &&
                                                  !(r_buffer->header.latch.isExclusivelyLatched())
                                                  // && (partition_i) >= p_begin && (partition_i) <= p_end
                                                  && r_buffer->header.state == BufferFrame::STATE::HOT);
               repickIf(!is_cooling_candidate);
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
               // -------------------------------------------------------------------------------------
               bool picked_a_child_instead = false, all_children_evicted = true;
               [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
               COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
               getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
                  all_children_evicted &= swip.isEVICTED();  // ignore when it has a child in the cooling stage
                  if (swip.isHOT()) {
                     r_buffer = &swip.bfRef();
                     r_guard.recheck();
                     picked_a_child_instead = true;
                     return false;
                  }
                  r_guard.recheck();
                  return true;
               });
               COUNTERS_BLOCK()
               {
                  iterate_children_begin = std::chrono::high_resolution_clock::now();
                  PPCounters::myCounters().iterate_children_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
               }
               if (picked_a_child_instead) {
                  continue;  // restart the inner loop
               }
               repickIf(!all_children_evicted);

               // -------------------------------------------------------------------------------------
               [[maybe_unused]] Time find_parent_begin, find_parent_end;
               COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
               DTID dt_id = r_buffer->page.dt_id;
               r_guard.recheck();
               ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, *r_buffer);
               assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
               assert(parent_handler.parent_guard.latch != reinterpret_cast<HybridLatch*>(0x99));
               COUNTERS_BLOCK()
               {
                  find_parent_end = std::chrono::high_resolution_clock::now();
                  PPCounters::myCounters().find_parent_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(find_parent_end - find_parent_begin).count());
               }
               // -------------------------------------------------------------------------------------
               r_guard.recheck();
               if (getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer, r_guard, parent_handler)) {
                  r_buffer = &randomBufferFrame();
                  continue;
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               // Suitable page founds, lets cool
               {
                  const PID pid = r_buffer->header.pid;
                  Partition& partition = getPartition(pid);
                  // r_x_guard can only be acquired and release while the partition mutex is locked
                  {
                     JMUW<std::unique_lock<std::mutex>> g_guard(partition.cooling_mutex);
                     ExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
                     ExclusiveGuard r_x_guard(r_guard);
                     // -------------------------------------------------------------------------------------
                     assert(r_buffer->header.pid == pid);
                     assert(r_buffer->header.state == BufferFrame::STATE::HOT);
                     assert(r_buffer->header.isWB == false);
                     assert(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
                     assert(parent_handler.swip.bf == r_buffer);
                     partition.cooling_queue.emplace_back(reinterpret_cast<BufferFrame*>(r_buffer));
                     r_buffer->header.state = BufferFrame::STATE::COOL;
                     parent_handler.swip.cool();
                     partition.cooling_bfs_counter++;
                  }
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
                  // -------------------------------------------------------------------------------------
                  if (!phase_1_condition(partition)) {
                     r_buffer = &randomBufferFrame();
                     break;
                  }
               }
               r_buffer = &randomBufferFrame();
               // -------------------------------------------------------------------------------------
            }
            failed_attempts = 0;
            jumpmu_break;
         }
         jumpmuCatch() { r_buffer = &randomBufferFrame(); }
      }
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
      }
      // -------------------------------------------------------------------------------------
      struct FreedBfsBatch {
         BufferFrame *freed_bfs_batch_head = nullptr, *freed_bfs_batch_tail = nullptr;
         u64 freed_bfs_counter = 0;
         // -------------------------------------------------------------------------------------
         void reset()
         {
            freed_bfs_batch_head = nullptr;
            freed_bfs_batch_tail = nullptr;
            freed_bfs_counter = 0;
         }
         // -------------------------------------------------------------------------------------
         void push(Partition& partition)
         {
            partition.dram_free_list.batchPush(freed_bfs_batch_head, freed_bfs_batch_tail, freed_bfs_counter);
            reset();
         }
         // -------------------------------------------------------------------------------------
         u64 size() { return freed_bfs_counter; }
         // -------------------------------------------------------------------------------------
         void add(BufferFrame& bf)
         {
            bf.header.next_free_bf = freed_bfs_batch_head;
            if (freed_bfs_batch_head == nullptr) {
               freed_bfs_batch_tail = &bf;
            }
            freed_bfs_batch_head = &bf;
            freed_bfs_counter++;
            // -------------------------------------------------------------------------------------
         }
      };
      // -------------------------------------------------------------------------------------
      // phase_2_3:
      for (volatile u64 p_i = p_begin; p_i < p_end; p_i++) {
         Partition& partition = partitions[p_i];
         // -------------------------------------------------------------------------------------
         FreedBfsBatch freed_bfs_batch;
         // -------------------------------------------------------------------------------------
         auto evict_bf = [&](BufferFrame& bf, OptimisticGuard& guard, std::list<BufferFrame*>::iterator& bf_itr) {
            DTID dt_id = bf.page.dt_id;
            guard.recheck();
            ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
            assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
            ExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
            guard.guard.toExclusive();
            // -------------------------------------------------------------------------------------
            partition.cooling_queue.erase(bf_itr);
            partition.cooling_bfs_counter--;
            // -------------------------------------------------------------------------------------
            assert(!bf.header.isWB);
            // Reclaim buffer frame
            assert(bf.header.state == BufferFrame::STATE::COOL);
            parent_handler.swip.evict(bf.header.pid);
            // -------------------------------------------------------------------------------------
            // Reclaim buffer frame
            bf.reset();
            bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
            bf.header.latch.mutex.unlock();
            // -------------------------------------------------------------------------------------
            freed_bfs_batch.add(bf);
            if (freed_bfs_batch.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
               freed_bfs_batch.push(partition);
            }
            // -------------------------------------------------------------------------------------
            COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
         };
         if (phase_2_3_condition(partition)) {
            const s64 pages_to_iterate_partition = partition.free_bfs_limit - partition.dram_free_list.counter;
            // -------------------------------------------------------------------------------------
            /*
             * Phase 2:
             * Iterate over all partitions, in each partition:
             * iterate over the end of FIFO queue.
             */
            [[maybe_unused]] Time phase_2_begin, phase_2_end;
            COUNTERS_BLOCK() { phase_2_begin = std::chrono::high_resolution_clock::now(); }
            if (pages_to_iterate_partition > 0) {
               PPCounters::myCounters().phase_2_counter++;
               volatile u64 pages_left_to_iterate_partition = pages_to_iterate_partition;
               JMUW<std::unique_lock<std::mutex>> g_guard(partition.cooling_mutex);
               auto bf_itr = partition.cooling_queue.begin();
               while (pages_left_to_iterate_partition && bf_itr != partition.cooling_queue.end()) {
                  BufferFrame& bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  // -------------------------------------------------------------------------------------
                  jumpmuTry()
                  {
                     OptimisticGuard o_guard(bf.header.latch, true);
                     // Check if the BF got swizzled in or unswizzle another time in another partition
                     if (bf.header.state != BufferFrame::STATE::COOL || getPartitionID(bf.header.pid) != p_i) {
                        partition.cooling_queue.erase(bf_itr);
                        partition.cooling_bfs_counter--;
                        jumpmu::jump();
                     }
                     if (!bf.header.isWB) {
                        // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
                        {
                           JMUW<std::unique_lock<std::mutex>> io_guard(partition.io_mutex);
                           if (partition.io_ht.lookup(bf.header.pid)) {
                              partition.cooling_queue.erase(bf_itr);
                              partition.cooling_bfs_counter--;
                              jumpmu::jump();
                           }
                        }
                        pages_left_to_iterate_partition--;
                        if (bf.isDirty()) {
                           if (!async_write_buffer.full()) {
                              {
                                 ExclusiveGuard ex_guard(o_guard);
                                 assert(!bf.header.isWB);
                                 bf.header.isWB = true;
                              }
                              {
                                 SharedGuard s_guard(o_guard);
                                 PID wb_pid = bf.header.pid;
                                 if (FLAGS_out_of_place) {
                                    wb_pid = partitions[p_i].nextPID();
                                    assert(getPartitionID(bf.header.pid) == p_i);
                                    assert(getPartitionID(wb_pid) == p_i);
                                 }
                                 async_write_buffer.add(bf, wb_pid);
                              }
                           } else {
                              jumpmu_break;
                           }
                        } else {
                           evict_bf(bf, o_guard, bf_itr);
                        }
                     }
                     bf_itr = next_bf_tr;
                  }
                  jumpmuCatch() { bf_itr = next_bf_tr; }
               }
            };
            COUNTERS_BLOCK() { phase_2_end = std::chrono::high_resolution_clock::now(); }
            /*
             * Phase 3:
             */
            [[maybe_unused]] Time phase_3_begin, phase_3_end;
            COUNTERS_BLOCK() { phase_3_begin = std::chrono::high_resolution_clock::now(); }
            if (pages_to_iterate_partition > 0) {
               [[maybe_unused]] Time submit_begin, submit_end;
               COUNTERS_BLOCK()
               {
                  PPCounters::myCounters().phase_3_counter++;
                  submit_begin = std::chrono::high_resolution_clock::now();
               }
               async_write_buffer.submit();
               COUNTERS_BLOCK() { submit_end = std::chrono::high_resolution_clock::now(); }
               // -------------------------------------------------------------------------------------
               [[maybe_unused]] Time poll_begin, poll_end;
               COUNTERS_BLOCK() { poll_begin = std::chrono::high_resolution_clock::now(); }
               const u32 polled_events = async_write_buffer.pollEventsSync();
               COUNTERS_BLOCK() { poll_end = std::chrono::high_resolution_clock::now(); }
               // -------------------------------------------------------------------------------------
               [[maybe_unused]] Time async_wb_begin, async_wb_end;
               COUNTERS_BLOCK() { async_wb_begin = std::chrono::high_resolution_clock::now(); }
               async_write_buffer.getWrittenBfs(
                   [&](BufferFrame& written_bf, u64 written_lsn, PID out_of_place_pid) {
                      while (true) {
                         jumpmuTry()
                         {
                            Guard guard(written_bf.header.latch);
                            guard.toExclusive();
                            assert(written_bf.header.isWB);
                            assert(written_bf.header.lastWrittenGSN < written_lsn);
                            // -------------------------------------------------------------------------------------
                            if (FLAGS_out_of_place) {
                               partition.freePage(written_bf.header.pid);
                               written_bf.header.pid = out_of_place_pid;
                            }
                            written_bf.header.lastWrittenGSN = written_lsn;
                            written_bf.header.isWB = false;
                            PPCounters::myCounters().flushed_pages_counter++;
                            // -------------------------------------------------------------------------------------
                            guard.unlock();
                            jumpmu_break;
                         }
                         jumpmuCatch() {}
                      }
                   },
                   polled_events);
               COUNTERS_BLOCK() { async_wb_end = std::chrono::high_resolution_clock::now(); }
               // -------------------------------------------------------------------------------------
               volatile u64 pages_left_to_iterate_partition = polled_events;
               JMUW<std::unique_lock<std::mutex>> g_guard(partition.cooling_mutex);
               // -------------------------------------------------------------------------------------
               auto bf_itr = partition.cooling_queue.begin();
               while (pages_left_to_iterate_partition-- && bf_itr != partition.cooling_queue.end()) {
                  BufferFrame& bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  // -------------------------------------------------------------------------------------
                  jumpmuTry()
                  {
                     OptimisticGuard o_guard(bf.header.latch, true);
                     if (bf.header.state != BufferFrame::STATE::COOL || getPartitionID(bf.header.pid) != p_i) {
                        partition.cooling_queue.erase(bf_itr);
                        partition.cooling_bfs_counter--;
                        jumpmu::jump();
                     }
                     if (!bf.header.isWB && !bf.isDirty()) {
                        evict_bf(bf, o_guard, bf_itr);
                     }
                     // -------------------------------------------------------------------------------------
                     bf_itr = next_bf_tr;
                  }
                  jumpmuCatch() { bf_itr = next_bf_tr; }
               }
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK()
               {
                  PPCounters::myCounters().poll_ms += (std::chrono::duration_cast<std::chrono::microseconds>(poll_end - poll_begin).count());
                  PPCounters::myCounters().async_wb_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(async_wb_end - async_wb_begin).count());
                  PPCounters::myCounters().submit_ms += (std::chrono::duration_cast<std::chrono::microseconds>(submit_end - submit_begin).count());
                  phase_3_end = std::chrono::high_resolution_clock::now();
               }
            }
            // -------------------------------------------------------------------------------------
            COUNTERS_BLOCK()
            {
               PPCounters::myCounters().phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
               PPCounters::myCounters().phase_3_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_3_end - phase_3_begin).count());
            }
         }
         // -------------------------------------------------------------------------------------
         // Attach the freed bfs batch to the partition free list
         if (freed_bfs_batch.size()) {
            freed_bfs_batch.push(partition);
         }
      }  // end of partitions for
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bg_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
