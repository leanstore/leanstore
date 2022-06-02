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
   // TODO: register as special worker [hackaround atm]
   while (!cr::Worker::init_done);
   cr::Worker::tls_ptr = new cr::Worker(0, nullptr, 0, ssd_fd);
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   while (bg_threads_keep_running) {
      /*
       * Phase 1: unswizzle pages (put in the cooling stage)
       */
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      cool_pages();
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
      }
      // -------------------------------------------------------------------------------------
      // phase_2_3:
      for (volatile u64 p_i = p_begin; p_i < p_end; p_i++) {
         Partition& partition = getPartition(p_i);
         FreedBfsBatch freed_bfs_batch;

         if (partition.partition_size > partition.max_partition_size) {
            const s64 pages_to_iterate_partition = partition.partition_size - partition.max_partition_size;
            // -------------------------------------------------------------------------------------
            /*
             * Phase 2:
             * Iterate over all partitions, in each partition:
             * iterate over the end of FIFO queue.
             */
            [[maybe_unused]] Time phase_2_begin, phase_2_end;
            COUNTERS_BLOCK() { phase_2_begin = std::chrono::high_resolution_clock::now(); }
            second_phase(async_write_buffer, p_i, pages_to_iterate_partition, partition, freed_bfs_batch);
            COUNTERS_BLOCK() { phase_2_end = std::chrono::high_resolution_clock::now(); }
            /*
             * Phase 3:
             */
            [[maybe_unused]] Time phase_3_begin, phase_3_end;
            COUNTERS_BLOCK() { phase_3_begin = std::chrono::high_resolution_clock::now(); }
            third_phase(async_write_buffer, p_i, partition, pages_to_iterate_partition, freed_bfs_batch, phase_3_end);
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
            freed_bfs_batch.push();
         }
      }  // end of partitions for
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bg_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}

void BufferManager::third_phase(AsyncWriteBuffer& async_write_buffer,
                                volatile u64 p_i,
                                Partition& partition,
                                const s64 pages_to_iterate_partition,
                                BufferManager::FreedBfsBatch& freed_bfs_batch,
                                Time& phase_3_end)
{
   if (pages_to_iterate_partition > 0) {

      [[maybe_unused]] Time submit_begin, submit_end;
      COUNTERS_BLOCK()
      {
         PPCounters::myCounters().phase_3_counter++;
         submit_begin = std::chrono::high_resolution_clock::now();
      }
      u32 polled_events = async_write_buffer.flush(partition);

      COUNTERS_BLOCK() { submit_end = std::chrono::high_resolution_clock::now(); }
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
            BMOptimisticGuard o_guard(bf.header.latch);
            if (bf.header.state != BufferFrame::STATE::COOL || getPartitionID(bf.header.pid) != p_i) {
               partition.cooling_queue.erase(bf_itr);
               partition.cooling_bfs_counter--;
               bf_itr = next_bf_tr;
               jumpmu_continue;
            }
            if (!bf.header.isInWriteBuffer && !bf.isDirty()) {
               nonDirtyEvict(partition, bf_itr, bf, o_guard, freed_bfs_batch);
            }
            // -------------------------------------------------------------------------------------
            bf_itr = next_bf_tr;
         }
         jumpmuCatch() { bf_itr = next_bf_tr; }
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK()
      {
         PPCounters::myCounters().submit_ms += (std::chrono::duration_cast<std::chrono::microseconds>(submit_end - submit_begin).count());
         phase_3_end = std::chrono::high_resolution_clock::now();
      }
   }
}
void BufferManager::second_phase(AsyncWriteBuffer& async_write_buffer,
                                 volatile u64 p_i,
                                 const s64 pages_to_iterate_partition,
                                 Partition& partition,
                                 BufferManager::FreedBfsBatch& freed_bfs_batch)
{
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
            BMOptimisticGuard o_guard(bf.header.latch);
            // Check if the BF got swizzled in or unswizzle another time in another partition
            if (bf.header.state != BufferFrame::STATE::COOL || getPartitionID(bf.header.pid) != p_i) {
               partition.cooling_queue.erase(bf_itr);
               partition.cooling_bfs_counter--;
               bf_itr = next_bf_tr;
               jumpmu_continue;
            }
            if (!bf.header.isInWriteBuffer) {
               // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
               {
                  JMUW<std::unique_lock<std::mutex>> io_guard(partition.io_mutex);
                  if (partition.io_ht.lookup(bf.header.pid)) {
                     partition.cooling_queue.erase(bf_itr);
                     partition.cooling_bfs_counter--;
                     bf_itr = next_bf_tr;
                     jumpmu_continue;
                  }
               }
               pages_left_to_iterate_partition--;
               if (bf.isDirty()) {
                  if (!async_write_buffer.full()) {
                     {
                        BMExclusiveGuard ex_guard(o_guard);
                        assert(!bf.header.isInWriteBuffer);
                        bf.header.isInWriteBuffer = true;
                     }
                     {
                        BMSharedGuard s_guard(o_guard);
                        PID wb_pid = bf.header.pid;
                        if (FLAGS_out_of_place) {
                           wb_pid = getPartition(p_i).nextPID();
                           assert(getPartitionID(bf.header.pid) == p_i);
                           assert(getPartitionID(wb_pid) == p_i);
                        }
                        async_write_buffer.add(bf, wb_pid);
                     }
                  } else {
                     jumpmu_break;
                  }
               } else {
                  nonDirtyEvict(partition, bf_itr, bf, o_guard, freed_bfs_batch);
               }
            }
            bf_itr = next_bf_tr;
         }
         jumpmuCatch() { bf_itr = next_bf_tr; }
      }
   };
}
void BufferManager::cool_pages()
{
   auto phase_1_condition = [&](Partition& p) { return p.cooling_bfs_counter < p.cooling_bfs_limit; };
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
            BMOptimisticGuard r_guard(r_buffer->header.latch);
            // -------------------------------------------------------------------------------------
            // Performance crticial: we should cross cool (unswizzle), otherwise write performance will drop
            [[maybe_unused]] const u64 partition_i = getPartitionID(r_buffer->header.pid);
            const bool is_cooling_candidate = (!r_buffer->header.keep_in_memory && !r_buffer->header.isInWriteBuffer &&
                                               !(r_buffer->header.latch.isExclusivelyLatched())
                                               // && (partition_i) >= p_begin && (partition_i) <= p_end
                                               && r_buffer->header.state == BufferFrame::STATE::HOT);
            repickIf(!is_cooling_candidate);
            r_guard.recheck();
            // -------------------------------------------------------------------------------------
            bool picked_a_child_instead = false, all_children_evicted = true;
            [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
            COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
            getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
               all_children_evicted &= swip.isEVICTED();  // ignore when it has a child in the cooling stage
               if (swip.isHOT()) {
                  r_buffer = &swip.asBufferFrame();
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
               // r_x_guard can only be acquired and released while the partition mutex is locked
               {
                  JMUW<std::unique_lock<std::mutex>> g_guard(partition.cooling_mutex);
                  BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
                  BMExclusiveGuard r_x_guard(r_guard);
                  // -------------------------------------------------------------------------------------
                  assert(r_buffer->header.pid == pid);
                  assert(r_buffer->header.state == BufferFrame::STATE::HOT);
                  assert(r_buffer->header.isInWriteBuffer == false);
                  assert(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
                  assert(parent_handler.swip.bf == r_buffer);
                  // -------------------------------------------------------------------------------------
                  partition.cooling_queue.emplace_back(reinterpret_cast<BufferFrame*>(r_buffer));
                  r_buffer->header.state = BufferFrame::STATE::COOL;
                  parent_handler.swip.cool();  // Cool the pointing swip before unlocking the current bf
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
}

void BufferManager::nonDirtyEvict(Partition& partition,
                                  std::_List_iterator<BufferFrame*>& bf_itr,
                                  BufferFrame& bf,
                                  BMOptimisticGuard& guard,
                                  FreedBfsBatch& evictedOnes)
{
   DTID dt_id = bf.page.dt_id;
   guard.recheck();
   ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
   assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
   BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
   guard.guard.toExclusive();
   // -------------------------------------------------------------------------------------
   partition.cooling_queue.erase(bf_itr);
   partition.cooling_bfs_counter--;
   partition.partition_size--;
   // -------------------------------------------------------------------------------------
   assert(!bf.header.isInWriteBuffer);
   // Reclaim buffer frame
   assert(bf.header.state == BufferFrame::STATE::COOL);
   parent_handler.swip.evict(bf.header.pid);
   // -------------------------------------------------------------------------------------
   // Reclaim buffer frame
   bf.reset();
   bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
   bf.header.latch.mutex.unlock();
   // -------------------------------------------------------------------------------------
   evictedOnes.add(bf);
   if (evictedOnes.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
      evictedOnes.push();
   }
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; PPCounters::myCounters().total_evictions++; }
}

// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
