#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "Tracing.hpp"
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
   std::string thread_name("pp_" + std::to_string(p_begin) + "_" + std::to_string(p_end));
   pthread_setname_np(pthread_self(), thread_name.c_str());
   using Time = decltype(std::chrono::high_resolution_clock::now());
   // -------------------------------------------------------------------------------------
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size);
   std::vector<BufferFrame*> cool_candidate_bfs, evict_candidate_bfs;
   // -------------------------------------------------------------------------------------
   auto next_bf_range = [&]() {
      const u64 BATCH_SIZE = FLAGS_replacement_chunk_size;
      cool_candidate_bfs.clear();
      for (u64 i = 0; i < BATCH_SIZE; i++) {
         BufferFrame* r_bf = &randomBufferFrame();
         DO_NOT_OPTIMIZE(r_bf->header.state);
         cool_candidate_bfs.push_back(r_bf);
      }
      return;
   };
   // -------------------------------------------------------------------------------------
   while (bg_threads_keep_running) {
      // Phase 1: unswizzle pages (put in the cooling stage)
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      volatile u64 failed_attempts =
          0;  // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
#define repickIf(cond)                       \
   if (cond) {                               \
      failed_attempts = failed_attempts + 1; \
      jumpmu_continue;                       \
   }
      auto& current_partition = randomPartition();
      if ((current_partition.dram_free_list.counter < current_partition.free_bfs_limit) && failed_attempts < 10) {
         next_bf_range();
         while (cool_candidate_bfs.size()) {
            jumpmuTry()
            {
               BufferFrame* r_buffer = cool_candidate_bfs.back();
               cool_candidate_bfs.pop_back();
               COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
               // -------------------------------------------------------------------------------------
               BMOptimisticGuard r_guard(r_buffer->header.latch);
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
               bool picked_a_child_instead = false;
               [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
               COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
               getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
                  all_children_evicted &= swip.isEVICTED();  // Ignore when it has a child in the cooling stage
                  if (swip.isHOT()) {
                     BufferFrame* picked_child_bf = &swip.asBufferFrame();
                     r_guard.recheck();
                     picked_a_child_instead = true;
                     cool_candidate_bfs.push_back(picked_child_bf);
                     return false;
                  }
                  r_guard.recheck();
                  return true;
               });
               COUNTERS_BLOCK()
               {
                  iterate_children_end = std::chrono::high_resolution_clock::now();
                  PPCounters::myCounters().iterate_children_ms +=
                      (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
               }
               repickIf(!all_children_evicted || picked_a_child_instead);
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
               paranoid(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
               paranoid(parent_handler.parent_guard.latch != reinterpret_cast<HybridLatch*>(0x99));
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
                  // r_x_guard can only be acquired and released while the partition mutex is locked
                  {
                     BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
                     BMExclusiveGuard r_x_guard(r_guard);
                     // -------------------------------------------------------------------------------------
                     paranoid(r_buffer->header.pid == pid);
                     paranoid(r_buffer->header.state == BufferFrame::STATE::HOT);
                     paranoid(r_buffer->header.is_being_written_back == false);
                     paranoid(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
                     paranoid(parent_handler.swip.bf == r_buffer);
                     // -------------------------------------------------------------------------------------
                     r_buffer->header.state = BufferFrame::STATE::COOL;
                     parent_handler.swip.cool();  // Cool the pointing swip before unlocking the current bf
                  }
                  // -------------------------------------------------------------------------------------
                  COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
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
         paranoid(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
         BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
         c_guard.guard.toExclusive();
         // -------------------------------------------------------------------------------------
         if (FLAGS_crc_check && bf.header.crc) {
            ensure(utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE) == bf.header.crc);
         }
         // -------------------------------------------------------------------------------------
         ensure(!bf.isDirty());
         paranoid(!bf.header.is_being_written_back);
         paranoid(bf.header.state == BufferFrame::STATE::COOL);
         paranoid(parent_handler.swip.isCOOL());
         // -------------------------------------------------------------------------------------
         const PID evicted_pid = bf.header.pid;
         parent_handler.swip.evict(evicted_pid);
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
         if (FLAGS_pid_tracing) {
            Tracing::mutex.lock();
            if (Tracing::ht.contains(evicted_pid)) {
               std::get<1>(Tracing::ht[evicted_pid])++;
            } else {
               Tracing::ht[evicted_pid] = {dt_id, 1};
            }
            Tracing::mutex.unlock();
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
               jumpmu_continue;
            }
            const PID cooled_bf_pid = cooled_bf->header.pid;
            const u64 p_i = getPartitionID(cooled_bf_pid);
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               Partition& partition = getPartition(p_i);
               JMUW<std::unique_lock<std::mutex>> io_guard(partition.ht_mutex);
               if (partition.io_ht.lookup(cooled_bf_pid)) {
                  jumpmu_continue;
               }
            }
            if (cooled_bf->isDirty()) {
               if (!async_write_buffer.full()) {
                  {
                     BMExclusiveGuard ex_guard(o_guard);
                     paranoid(!cooled_bf->header.is_being_written_back);
                     cooled_bf->header.is_being_written_back.store(true, std::memory_order_release);
                     if (FLAGS_crc_check) {
                        cooled_bf->header.crc = utils::CRC(cooled_bf->page.dt, EFFECTIVE_PAGE_SIZE);
                     }
                     // TODO: preEviction callback according to DTID
                     PID wb_pid = cooled_bf_pid;
                     if (FLAGS_out_of_place) {
                        wb_pid = getPartition(cooled_bf_pid).nextPID();
                        paranoid(getPartitionID(cooled_bf->header.pid) == p_i);
                        paranoid(getPartitionID(wb_pid) == p_i);
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
                jumpmuTry()
                {
                   // When the written back page is being exclusively locked, we should rather waste the write and move on to another page
                   // Instead of waiting on its latch because of the likelihood that a data structure implementation keeps holding a parent latch
                   // while trying to acquire a new page
                   {
                      BMOptimisticGuard o_guard(written_bf.header.latch);
                      BMExclusiveGuard ex_guard(o_guard);
                      ensure(written_bf.header.is_being_written_back);
                      ensure(written_bf.header.last_written_plsn < written_lsn);
                      // -------------------------------------------------------------------------------------
                      if (FLAGS_out_of_place) {  // For recovery, so much has to be done here...
                         getPartition(getPartitionID(written_bf.header.pid)).freePage(written_bf.header.pid);
                         written_bf.header.pid = out_of_place_pid;
                      }
                      written_bf.header.last_written_plsn = written_lsn;
                      written_bf.header.is_being_written_back = false;
                      PPCounters::myCounters().flushed_pages_counter++;
                   }
                }
                jumpmuCatch()
                {
                   written_bf.header.crc = 0;
                   written_bf.header.is_being_written_back.store(false, std::memory_order_release);
                }
                // -------------------------------------------------------------------------------------
                {
                   jumpmuTry()
                   {
                      BMOptimisticGuard o_guard(written_bf.header.latch);
                      if (written_bf.header.state == BufferFrame::STATE::COOL && !written_bf.header.is_being_written_back && !written_bf.isDirty()) {
                         evict_bf(written_bf, o_guard);
                      }
                   }
                   jumpmuCatch() {}
                }
             },
             polled_events);
      }
      if (freed_bfs_batch.size()) {
         freed_bfs_batch.push(current_partition);
      }
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   cr::CRManager::global->deleteSpecialWorker();
   bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
