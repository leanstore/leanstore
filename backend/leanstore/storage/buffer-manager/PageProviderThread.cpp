#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
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
   std::string thread_name("page_provider_" + std::to_string(p_begin) + "_" + std::to_string(p_end));
   pthread_setname_np(pthread_self(), thread_name.c_str());
   using Time = decltype(std::chrono::high_resolution_clock::now());
   // -------------------------------------------------------------------------------------
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size);
   std::vector<BufferFrame*> evict_candidate_bfs;
   // -------------------------------------------------------------------------------------
   auto phase_1_condition = [&](Partition& p) { return (p.dram_free_list.counter < p.free_bfs_limit); };
   auto next_bf_range = [&]() {
      restart : {
         u64 current_cursor = clock_cursor.load();
         u64 next_value;
         if ((current_cursor + 64) < dram_pool_size) {
            next_value = current_cursor + 64;
         } else {
            next_value = 0;
         }
         if (!clock_cursor.compare_exchange_strong(current_cursor, next_value)) {
            goto restart;
         }
         return std::pair<u64, u64>{current_cursor, next_value == 0 ? dram_pool_size : next_value};
      }
   };
   // -------------------------------------------------------------------------------------
   while (bg_threads_keep_running) {
      // Phase 1: unswizzle pages (put in the cooling stage)
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      BufferFrame* volatile r_buffer = nullptr;  // Attention: we may set the r_buffer to a child of a bf instead of random
      volatile u64 failed_attempts =
          0;  // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
#define repickIf(cond)                       \
   if (cond) {                               \
      failed_attempts = failed_attempts + 1; \
      jumpmu_continue;                              \
   }
      auto& current_partition = randomPartition();
      if (phase_1_condition(current_partition) && failed_attempts < 10) {
         bool picked_a_child_instead = false;
         auto bf_range = next_bf_range();
         u64 bf_i = bf_range.first;
         while (bf_i < bf_range.second || picked_a_child_instead) {
            jumpmuTry()
            {
               if (picked_a_child_instead) {
                  picked_a_child_instead = false;
               } else {
                  r_buffer = &bfs[bf_i++];
               }
               COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
               BMOptimisticGuard r_guard(r_buffer->header.latch);
               // -------------------------------------------------------------------------------------
               repickIf(r_buffer->header.keep_in_memory || r_buffer->header.is_being_written_back || r_buffer->header.latch.isExclusivelyLatched());
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               if (r_buffer->header.state == BufferFrame::STATE::COOL) {
                  evict_candidate_bfs.push_back(reinterpret_cast<BufferFrame*>(r_buffer));
                  repickIf(true);  // TODO: maybe without failed_attempts
               }
               repickIf(r_buffer->header.state != BufferFrame::STATE::HOT);
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
               // -------------------------------------------------------------------------------------
               bool all_children_evicted = true;
               [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
               COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
               getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
                  all_children_evicted &= swip.isEVICTED();  // Ignore when it has a child in the cooling stage
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
                  jumpmu_continue;  // Restart the inner loop without re-picking
               }
               repickIf(!all_children_evicted);
               // -------------------------------------------------------------------------------------
               [[maybe_unused]] Time find_parent_begin, find_parent_end;
               COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
               DTID dt_id = r_buffer->page.dt_id;
               r_guard.recheck();
               ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, *r_buffer);
               // -------------------------------------------------------------------------------------
               if (FLAGS_optimistic_parent_pointer) {
                  if (parent_handler.is_bf_updated) {
                     r_guard.guard.version += 2;
                  }
               }
               // -------------------------------------------------------------------------------------
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
               const SpaceCheckResult space_check_res = getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer);
               if (space_check_res == SpaceCheckResult::RESTART_SAME_BF || space_check_res == SpaceCheckResult::PICK_ANOTHER_BF) {
                  jumpmu_continue;
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               // Suitable page founds, lets cool
               {
                  const PID pid = r_buffer->header.pid;
                  Partition& partition = getPartition(pid);
                  // r_x_guard can only be acquired and released while the partition mutex is locked
                  {
                     BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
                     BMExclusiveGuard r_x_guard(r_guard);
                     // -------------------------------------------------------------------------------------
                     assert(r_buffer->header.pid == pid);
                     assert(r_buffer->header.state == BufferFrame::STATE::HOT);
                     assert(r_buffer->header.is_being_written_back == false);
                     assert(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
                     assert(parent_handler.swip.bf == r_buffer);
                     // -------------------------------------------------------------------------------------
                     r_buffer->header.state = BufferFrame::STATE::COOL;
                     parent_handler.swip.cool();  // Cool the pointing swip before unlocking the current bf
                  }
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
                  // -------------------------------------------------------------------------------------
                  if (!phase_1_condition(partition)) {
                     jumpmu_break;
                  }
               }
               failed_attempts = 0;
            }
            jumpmuCatch() {}
         }
      }
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
      }
      // -------------------------------------------------------------------------------------
      // Phase 2:
      FreedBfsBatch freed_bfs_batch;
      auto evict_bf = [&](BufferFrame& bf, BMOptimisticGuard& c_guard) {
         DTID dt_id = bf.page.dt_id;
         c_guard.recheck();
         ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
         // -------------------------------------------------------------------------------------
         if (FLAGS_optimistic_parent_pointer) {
            if (parent_handler.is_bf_updated) {
               c_guard.guard.version += 2;
            }
         }
         // -------------------------------------------------------------------------------------
         assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
         BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
         c_guard.guard.toExclusive();
         // -------------------------------------------------------------------------------------
         assert(!bf.header.is_being_written_back);
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
            freed_bfs_batch.push(current_partition);
         }
         // -------------------------------------------------------------------------------------
         COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
      };
      // -------------------------------------------------------------------------------------
      for (volatile const auto& cooled_bf : evict_candidate_bfs) {
         jumpmuTry()
         {
            BMOptimisticGuard o_guard(cooled_bf->header.latch);
            // Check if the BF got swizzled in or unswizzle another time in another partition
            if (cooled_bf->header.state != BufferFrame::STATE::COOL ||
                cooled_bf->header.is_being_written_back) {  //  || getPartitionID(bf.header.pid) != p_i
               continue;
            }
            const PID cooled_bf_pid = cooled_bf->header.pid;
            const u64 p_i = getPartitionID(cooled_bf_pid);
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               Partition& partition = getPartition(p_i);
               JMUW<std::unique_lock<std::mutex>> io_guard(partition.ht_mutex);
               if (partition.io_ht.lookup(cooled_bf_pid)) {
                  continue;
               }
            }
            if (cooled_bf->isDirty()) {
               if (!async_write_buffer.full()) {
                  {
                     BMExclusiveGuard ex_guard(o_guard);
                     assert(!cooled_bf->header.is_being_written_back);
                     cooled_bf->header.is_being_written_back = true;
                     // TODO: preEviction callback according to DTID
                  }
                  {
                     BMSharedGuard s_guard(o_guard);
                     PID wb_pid = cooled_bf_pid;
                     if (FLAGS_out_of_place) {
                        wb_pid = getPartition(cooled_bf_pid).nextPID();
                        assert(getPartitionID(cooled_bf->header.pid) == p_i);
                        assert(getPartitionID(wb_pid) == p_i);
                     }
                     async_write_buffer.add(*cooled_bf, wb_pid);
                  }
               } else {
                  jumpmu_break;
               }
            } else {
               evict_bf(*cooled_bf, o_guard);
            }
         }
         jumpmuCatch() {}
      }
      evict_candidate_bfs.clear();
      // -------------------------------------------------------------------------------------
      // Phase 3:
      if (async_write_buffer.submit()) {
         const u32 polled_events = async_write_buffer.pollEventsSync();
         async_write_buffer.getWrittenBfs(
             [&](BufferFrame& written_bf, u64 written_lsn, PID out_of_place_pid) {
                while (true) {
                   jumpmuTry()
                   {
                      Guard guard(written_bf.header.latch);
                      guard.toExclusive();
                      assert(written_bf.header.is_being_written_back);
                      assert(written_bf.header.last_written_plsn < written_lsn);
                      // -------------------------------------------------------------------------------------
                      if (FLAGS_out_of_place) {
                         getPartition(getPartitionID(written_bf.header.pid)).freePage(written_bf.header.pid);
                         written_bf.header.pid = out_of_place_pid;
                      }
                      written_bf.header.last_written_plsn = written_lsn;
                      written_bf.header.is_being_written_back = false;
                      PPCounters::myCounters().flushed_pages_counter++;
                      // -------------------------------------------------------------------------------------
                      guard.unlock();
                      jumpmu_break;
                   }
                   jumpmuCatch() {}
                }
                // -------------------------------------------------------------------------------------
                jumpmuTry()
                {
                   BMOptimisticGuard o_guard(written_bf.header.latch);
                   if (written_bf.header.state == BufferFrame::STATE::COOL && !written_bf.header.is_being_written_back && !written_bf.isDirty()) {
                      evict_bf(written_bf, o_guard);
                   }
                }
                jumpmuCatch() {}
             },
             polled_events);
      }
      if (freed_bfs_batch.size()) {
         freed_bfs_batch.push(current_partition);
      }
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bg_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
