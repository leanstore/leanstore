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
using Time = decltype(std::chrono::high_resolution_clock::now());

BufferManager::PageProviderThread::PageProviderThread(u64 t_i, BufferManager* bf_mgr): id(t_i), bf_mgr(*bf_mgr), 
         async_write_buffer(bf_mgr->ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size,
            [&](PID pid) -> Partition& {return bf_mgr->getPartition(pid);},
            [&](BufferFrame& bf){
               jumpmuTry()
               {
                  BMOptimisticGuard o_guard(bf.header.latch);
                  if (bf.header.state == BufferFrame::STATE::COOL && !bf.header.is_being_written_back && !bf.isDirty()) {
                     bf_mgr->evict_bf(freed_bfs_batch, bf, o_guard);
                  }
               }
               jumpmuCatch() {}
            }){
         };
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::PageProviderThread::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, bf_mgr.dram_pool_size);
   COUNTERS_BLOCK() {
      PPCounters::myCounters().touched_bfs_counter++;
      PPCounters::myCounters().total_touches++;}
   return bf_mgr.bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::PageProviderThread::set_thread_config(){
   if (FLAGS_pin_threads) {
      utils::pinThisThread(FLAGS_worker_threads + FLAGS_wal + id);
   } else {
      utils::pinThisThread(FLAGS_wal + id);
   }
   std::string thread_name("pp_" + std::to_string(id));
   CPUCounters::registerThread(thread_name);
   // https://linux.die.net/man/2/setpriority
   if (FLAGS_root) {
      posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
   }
   pthread_setname_np(pthread_self(), thread_name.c_str());
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
}
// -------------------------------------------------------------------------------------
void BufferManager::PageProviderThread::select_bf_range() {
      const u64 BATCH_SIZE = FLAGS_replacement_chunk_size;
      cool_candidate_bfs.clear();
      for (u64 i = 0; i < BATCH_SIZE; i++) {
         BufferFrame* r_bf = &randomBufferFrame();
         DO_NOT_OPTIMIZE(r_bf->header.state);
         cool_candidate_bfs.push_back(r_bf);
      }
      return;
}

bool BufferManager::PageProviderThread::childInRam(BufferFrame* r_buffer, BMOptimisticGuard& r_guard){
      // Check if child in RAM; Add first hot child to cooling queue
      bool all_children_evicted = true;
      bool picked_a_child_instead = false;
      [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
      COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
      bf_mgr.getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
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
         iterate_children_begin = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().iterate_children_ms +=
             (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
      }
      return !all_children_evicted || picked_a_child_instead;
}

ParentSwipHandler BufferManager::PageProviderThread::findParent(BufferFrame* r_buffer, BMOptimisticGuard& r_guard){
      [[maybe_unused]] Time find_parent_begin, find_parent_end;
      COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
      DTID dt_id = r_buffer->page.dt_id;
      r_guard.recheck();
      // get Parent
      ParentSwipHandler parent_handler = bf_mgr.getDTRegistry().findParent(dt_id, *r_buffer);
      // -------------------------------------------------------------------------------------
      paranoid(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
      paranoid(parent_handler.parent_guard.latch != reinterpret_cast<HybridLatch*>(0x99));
      COUNTERS_BLOCK()
      {
         find_parent_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().find_parent_ms +=
             (std::chrono::duration_cast<std::chrono::microseconds>(find_parent_end - find_parent_begin).count());
      }
      r_guard.recheck();
      return parent_handler;

}

bool BufferManager::PageProviderThread::checkXMerge(BufferFrame* r_buffer, BMOptimisticGuard& r_guard){
      const SpaceCheckResult space_check_res = bf_mgr.getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer);
      return space_check_res == SpaceCheckResult::RESTART_SAME_BF || space_check_res == SpaceCheckResult::PICK_ANOTHER_BF;
}

void BufferManager::PageProviderThread::setCool(BufferFrame* r_buffer, BMOptimisticGuard& r_guard, ParentSwipHandler& parent_handler){
      {
         [[maybe_unused]] const PID pid = r_buffer->header.pid;
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

}
void BufferManager::PageProviderThread::firstChance(){
   [[maybe_unused]] Time phase_1_begin, phase_1_end;
      COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
      while (cool_candidate_bfs.size()) {
         jumpmuTry()
         {
            COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
            BufferFrame* r_buffer = cool_candidate_bfs.back();
            cool_candidate_bfs.pop_back();
            // -------------------------------------------------------------------------------------
            BMOptimisticGuard r_guard(r_buffer->header.latch);
            if(bf_mgr.pageIsNotEvictable(r_buffer, r_guard)){jumpmu_continue;}
            r_guard.recheck();
            // Second Chance was given and missed. Send to phase 2;
            if (r_buffer->header.state == BufferFrame::STATE::COOL) {
               evict_candidate_bfs.push_back(reinterpret_cast<BufferFrame*>(r_buffer));
               jumpmu_continue;
            }
            // Check if not hot (therefore loaded / free)
            if(r_buffer->header.state != BufferFrame::STATE::HOT){
               jumpmu_continue;
            }
            COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
            if(childInRam(r_buffer, r_guard)){jumpmu_continue;}
            r_guard.recheck();
            ParentSwipHandler parent_handler = findParent(r_buffer, r_guard);
            if(checkXMerge(r_buffer, r_guard)){jumpmu_continue;}
            r_guard.recheck();
            setCool(r_buffer, r_guard, parent_handler);
         }
         jumpmuCatch() {}
      }
      COUNTERS_BLOCK()
      {
         phase_1_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
      }
}

void BufferManager::PageProviderThread::secondChance(){
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
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               HashTable& io_table = bf_mgr.getIOTable(cooled_bf_pid);
               JMUW<std::unique_lock<std::mutex>> io_guard(io_table.ht_mutex);
               if (io_table.lookup(cooled_bf_pid)) {
                  jumpmu_continue;
               }
            }
            if (cooled_bf->isDirty()) {
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
                     [[maybe_unused]] u64 p_i = bf_mgr.getPartitionID(cooled_bf_pid);
                     wb_pid = bf_mgr.getPartition(cooled_bf_pid).nextPID();
                     paranoid(bf_mgr.getPartitionID(cooled_bf->header.pid) == p_i);
                     paranoid(bf_mgr.getPartitionID(wb_pid) == p_i);
                  }
                  async_write_buffer.add(*cooled_bf, wb_pid);
               }
            } else {
               bf_mgr.evict_bf(freed_bfs_batch, *cooled_bf, o_guard);
            }
         }
         jumpmuCatch() {}
      }
      evict_candidate_bfs.clear();
}
void BufferManager::PageProviderThread::handleWritten(){
   async_write_buffer.flush();
}
void BufferManager::PageProviderThread::run()
{
   set_thread_config();

   while (bf_mgr.bg_threads_keep_running) {
      auto& current_free_list = bf_mgr.randomFreeList();
      if (current_free_list.counter >= current_free_list.free_bfs_limit) {
            continue;
      }
      freed_bfs_batch.set_free_list(&current_free_list);
      select_bf_range();
      firstChance();
      secondChance();
      handleWritten();
      freed_bfs_batch.push();
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bf_mgr.bg_threads_counter--;
   //   delete cr::Worker::tls_ptr;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
