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
            [&]([[maybe_unused]] BufferFrame& bf){}),
         evictions_per_epoch(std::max((u64) 1, (u64) bf_mgr->dram_pool_size / FLAGS_epoch_size)){
         };
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::PageProviderThread::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, bf_mgr.dram_pool_size);
   COUNTERS_BLOCK() {PPCounters::myCounters().touched_bfs_counter++;}
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
void BufferManager::PageProviderThread::prefetch_bf(u32 BATCH_SIZE) {
      prefetched_bfs.clear();
      for (u64 i = 0; i < BATCH_SIZE; i++) {
         BufferFrame* r_bf = &randomBufferFrame();
         __builtin_prefetch(r_bf);
         prefetched_bfs.push_back(r_bf);
      }
      return;
}

bool BufferManager::PageProviderThread::childInRam(BufferFrame* r_buffer, BMOptimisticGuard& r_guard, bool pick_child){
      // Check if child in RAM; Add first hot child to cooling queue
      bool all_children_evicted = true;
      bool picked_a_child_instead = false;
      [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
      COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
      bf_mgr.getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
         all_children_evicted &= swip.isEVICTED();  // Ignore when it has a child in the cooling stage
         if(!pick_child && !all_children_evicted){
            return false;
         }
         if (swip.isHOT()) {
            BufferFrame* picked_child_bf = &swip.asBufferFrame();
            r_guard.recheck();
            picked_a_child_instead = true;
            prefetched_bfs.push_back(picked_child_bf);
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

bool BufferManager::PageProviderThread::checkXMerge(BufferFrame* r_buffer){
      const SpaceCheckResult space_check_res = bf_mgr.getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer);
      return space_check_res == SpaceCheckResult::RESTART_SAME_BF || space_check_res == SpaceCheckResult::PICK_ANOTHER_BF;
}

double BufferManager::PageProviderThread::findThresholds(){
   prefetch_bf(FLAGS_watt_samples*2);
   volatile double min = 50000, second_min = 50000;
   [[maybe_unused]] Time threshold_tests_begin, threshold_tests_end;
   COUNTERS_BLOCK() { threshold_tests_begin = std::chrono::high_resolution_clock::now(); }
   u32 valid_tests = 0;
   while(valid_tests < FLAGS_watt_samples){
      BufferFrame * r_buffer = prefetched_bfs.back();
      prefetched_bfs.pop_back();
      if(prefetched_bfs.empty()){
         prefetch_bf((FLAGS_watt_samples - valid_tests)*2);
      }
      jumpmuTry()
      {
         COUNTERS_BLOCK() { PPCounters::myCounters().threshold_tests++; }
         // -------------------------------------------------------------------------------------
         BMOptimisticGuard r_guard(r_buffer->header.latch);
         if(bf_mgr.pageIsNotEvictable(r_buffer)){jumpmu_continue;}
         r_guard.recheck();
         if(childInRam(r_buffer, r_guard, false)){jumpmu_continue;}
         r_guard.recheck();
         double value = r_buffer->header.tracker.getValue();
         r_guard.recheck();
         if(value < min){
            second_min = min;
            min = value;
         }
         else if (value < second_min){
            second_min = value;
         }
         valid_tests++;
      }
      jumpmuCatch() {}
   }
   COUNTERS_BLOCK()
   {
      threshold_tests_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().threshold_tests_ms += (std::chrono::duration_cast<std::chrono::microseconds>(threshold_tests_end - threshold_tests_begin).count());
   }
   // double calculated_min = min;
   double second_value = min*1.3;
   if(valid_tests>1){
      //calculated_min = (min + second_min) / 2.0;
      second_value = second_min;
   }
   // last_min = calculated_min;
   return second_value;
   // return calculated_min;
}

void BufferManager::PageProviderThread::evictPages(double min){
   prefetch_bf(FLAGS_replacement_chunk_size);
   prefetched_bfs.insert(prefetched_bfs.end(), second_chance_bfs.begin(), second_chance_bfs.end());
   second_chance_bfs.clear();
   [[maybe_unused]] Time eviction_begin, eviction_end;
      COUNTERS_BLOCK() { eviction_begin = std::chrono::high_resolution_clock::now(); }
      while(prefetched_bfs.size() > 0){
         BufferFrame * r_buffer = prefetched_bfs.back();
         prefetched_bfs.pop_back();

         jumpmuTry()
         {
            COUNTERS_BLOCK() { PPCounters::myCounters().eviction_tests++; }
            // -------------------------------------------------------------------------------------
            BMOptimisticGuard r_guard(r_buffer->header.latch);
            if(bf_mgr.pageIsNotEvictable(r_buffer)){jumpmu_continue;}
            r_guard.recheck();
            COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
            if(childInRam(r_buffer, r_guard, false)){jumpmu_continue;}
            r_guard.recheck();
            if(checkXMerge(r_buffer)){jumpmu_continue;}
            r_guard.recheck();
            double value = r_buffer->header.tracker.getValue();
            r_guard.recheck();
            if (value > min){ // Page Value is high enough.
               jumpmu_continue;
            }
            if (r_buffer->header.is_being_written_back) {  //  || getPartitionID(bf.header.pid) != p_i
               jumpmu_continue;
            }
            const PID r_buffer_pid = r_buffer->header.pid;
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               HashTable& io_table = bf_mgr.getIOTable(r_buffer_pid);
               JMUW<std::unique_lock<std::mutex>> io_guard(io_table.ht_mutex);
               if (io_table.lookup(r_buffer_pid)) {
                  jumpmu_continue;
               }
            }
            if (r_buffer->isDirty()) {
               handleDirty(r_guard, r_buffer, r_buffer_pid);
            } else {
               bf_mgr.evict_bf(freed_bfs_batch, *r_buffer, r_guard);
               pages_evicted ++;
            }

         }
         jumpmuCatch() {}
      }
      COUNTERS_BLOCK()
      {
         eviction_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().eviction_ms += (std::chrono::duration_cast<std::chrono::microseconds>(eviction_end - eviction_begin).count());
      }
}
void BufferManager::PageProviderThread::handleDirty(leanstore::storage::BMOptimisticGuard& o_guard,
                                                    leanstore::storage::BufferFrame* const volatile& cooled_bf,
                                                    const PID cooled_bf_pid)
{
   // Set is_beeing_written_back, update crc, get out_of_place pid, add to write_buffer
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
      // for eviction
      prefetch_bf(FLAGS_replacement_chunk_size);
      double threshold = findThresholds();
      evictPages(threshold);
         while(pages_evicted >= evictions_per_epoch){
            pages_evicted -= evictions_per_epoch;
            leanstore::storage::BufferFrame::Header::Tracker::globalTrackerTime++;
         }
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
