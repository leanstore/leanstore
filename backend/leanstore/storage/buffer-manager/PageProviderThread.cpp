#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"
#include "leanstore/storage/buffer-manager/Partition.hpp"
#include "leanstore/storage/buffer-manager/Swip.hpp"
#include "leanstore/sync-primitives/JumpMU.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency/Mean.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <linux/perf_event.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <stdexcept>
#include <string>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------

s64 BufferManager::phase_1_condition(CoolingPartition& p) {
   return  (s64)p.cooling_bfs_limit - (p.dram_free_list.counter + p.cooling_bfs_counter);
}
s64 BufferManager::phase_2_3_condition(CoolingPartition& p) {
   return (s64)p.free_bfs_limit - p.dram_free_list.counter;
};
void BufferManager::pageProviderThread(u64 p_begin, u64 p_end)  // [p_begin, p_end)
{
   // FIXME MERGE put in comment
   //cr::Worker::tls_ptr = new cr::Worker(0, nullptr, 0, ssd_fd); 
   //   delete cr::Worker::tls_ptr;
}

void BufferManager::pageProviderCycle(int partition_id) {
   using Time = decltype(std::chrono::high_resolution_clock::now());

   /*
    * Phase 1:
    */
   ensure(partition_id < (int)cooling_partitions_count);
   CoolingPartition& partition = cooling_partitions[partition_id];
   if (phase_1_condition(partition) > 64) {
      u64 picked = pageProviderPhase1(partition, 128, partition_id);
      //u64 picked = pageProviderPhase1Vec(partition, 128);
      COUNTERS_BLOCK() {ThreadCounters::myCounters().pp_p1_picked += picked; }
   }
   // -------------------------------------------------------------------------------------
   /*
    * Phase 2:
    * Iterate over all partitions, in each partition:
    * iterate over the end of FIFO queue.
    */
   s64 pages_to_iterate_partition = partition.free_bfs_limit - partition.dram_free_list.counter;// - (s64)partition.outstanding;
   [[maybe_unused]] Time phase_2_begin, phase_2_end;
   [[maybe_unused]] Time submit_begin, submit_end;
   COUNTERS_BLOCK() { phase_2_begin = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().pp_qlen += partition.cooling_queue.size();
      PPCounters::myCounters().pp_pages_iterate += pages_to_iterate_partition > 0 ? pages_to_iterate_partition : 0;
      PPCounters::myCounters().pp_qlen_cnt++;
   }
   // -------------------------------------------------------------------------------------
   if (pages_to_iterate_partition > 0) {
      pages_to_iterate_partition = std::max(pages_to_iterate_partition , (s64)1);
      int added = pageProviderPhase2(partition, pages_to_iterate_partition, partition.state.freed_bfs_batch);
      COUNTERS_BLOCK() { PPCounters::myCounters().phase_2_added += added; }
   }
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { 
      phase_2_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_2_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end - phase_2_begin).count());
      submit_begin = phase_2_end;
      partition.state.phase_3_begin = submit_begin; 
      PPCounters::myCounters().phase_3_counter++;
   }
   /*
    * Phase 3:
    */
   COUNTERS_BLOCK() { 
      //PPCounters::myCounters().submitted += submitted;
      //PPCounters::myCounters().submit_cnt++;
      submit_end = std::chrono::high_resolution_clock::now(); 
      PPCounters::myCounters().submit_ms += (std::chrono::duration_cast<std::chrono::microseconds>(submit_end - submit_begin).count());
      partition.state.poll_begin = submit_end; 
   }
   COUNTERS_BLOCK() { partition.state.poll_end = std::chrono::high_resolution_clock::now(); }
   [[maybe_unused]] Time async_wb_begin, async_wb_end;
   // -------------------------------------------------------------------------------------
   pageProviderPhase3evict(partition, partition.state.freed_bfs_batch);
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK()
   {
      PPCounters::myCounters().poll_ms += (std::chrono::duration_cast<std::chrono::microseconds>(partition.state.poll_end - partition.state.poll_begin).count());
      PPCounters::myCounters().async_wb_ms += 0;
      partition.state.phase_3_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_3_ms += (std::chrono::duration_cast<std::chrono::microseconds>(partition.state.phase_3_end - partition.state.phase_3_begin).count());
   }
   // -------------------------------------------------------------------------------------
   // Attach the freed bfs batch to the partition free list
   if (partition.state.freed_bfs_batch.size()) {
      partition.pushFreeList();
   }
   COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
}

void BufferManager::evict_bf(CoolingPartition& partition, FreedBfsBatch& freed_bfs_batch, BufferFrame& bf, OptimisticGuard& guard, bool& p1, bool& p2) {
   DTID dt_id = bf.page.dt_id;
   guard.recheck();
   ParentSwipHandler parent_handler;
   getDTRegistry().findParent(dt_id, bf, parent_handler);
   // -------------------------------------------------------------------------------------
   if (FLAGS_optimistic_parent_pointer) {
      if (parent_handler.is_bf_updated) {
         guard.guard.version += 2;
      }
   }
   // -------------------------------------------------------------------------------------
   assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
   ExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
   guard.guard.toExclusive();
   // -------------------------------------------------------------------------------------
   assert(!bf.header.isWB);
   // Reclaim buffer frame
   assert(bf.header.state == BufferFrame::STATE::IOCOLDDONE || bf.header.state == BufferFrame::STATE::IOPOPPED || bf.header.state == BufferFrame::STATE::COOL);
   p1 = true;
   parent_handler.swip->evict(bf.header.pid);
   parent_handler.parent_bf->page.GSN++;
   // -------------------------------------------------------------------------------------
   // Reclaim buffer frame
   bf.reset();
   ensure(bf.header.state == BufferFrame::STATE::FREE);
   bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
   bf.header.latch.mutex.unlock();
   // -------------------------------------------------------------------------------------
   freed_bfs_batch.add(bf);
   if (freed_bfs_batch.size() <= std::min<u64>(FLAGS_worker_threads, 128)) {
      partition.pushFreeList();
   }
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; }
   COUNTERS_BLOCK() { ThreadCounters::myCounters().pp_p23_evicted++; }
   p2 = true;
}
// -------------------------------------------------------------------------------------
u64 BufferManager::pageProviderPhase1Vec(CoolingPartition& partition, const u64 required, int partition_id)  {
   using Time = decltype(std::chrono::high_resolution_clock::now());
   /*
    * Phase 1: unswizzle pages (put in the cooling stage)
    */
   // -------------------------------------------------------------------------------------
   [[maybe_unused]] Time phase_1_begin, phase_1_end;
   COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
   // -------------------------------------------------------------------------------------
   const int max_partitions = FLAGS_pp_threads;
   ensure(partition_id < max_partitions);
   const int MAX_VEC_SIZE = 16;
   struct vec_struct {
      BufferFrame* bf;
      u64 version;
      ParentSwipHandler parent_handler;
      bool versionCheck() const {
         return bf->header.latch.version == version;
      }
   };
   vec_struct vec_structs[MAX_VEC_SIZE];
   vec_struct* vecs[MAX_VEC_SIZE];
   // -------------------------------------------------------------------------------------
   u64 failed_cause_not_hot = 0;
   u64 found = 0;
   u64 attempts = 0;
   u64 failed_attempts = 0; // attempts
   // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
   while (found < required && failed_attempts < 10) {
      COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter += MAX_VEC_SIZE; }
      attempts += MAX_VEC_SIZE;
      int vec_size = MAX_VEC_SIZE;
      // -------------------------------------------------------------------------------------
      for (int i = 0; i < MAX_VEC_SIZE; i++ ) {
         vecs[i] = &vec_structs[i];
         //std::cout << std::hex << "bf_pos: " << bf_pos << std::endl;
         u64 bf_pos = partitionRandomBufferFramePos(partition_id, max_partitions);  
         vecs[i]->bf = &bfs[bf_pos];
      }
      for (int i = 0; i < MAX_VEC_SIZE; i++ ) {
         vecs[i]->version = vecs[i]->bf->header.latch.version;
      }
      // -------------------------------------------------------------------------------------
      // check conditions AND NOT EXCLUSIVE latched
      // TODO isExcLatched on version
#define is_cooling_candidate(bf) !bf->header.keep_in_memory && !bf->header.isWB \
      && !(bf->header.latch.isExclusivelyLatched()) \
      && bf->header.state == BufferFrame::STATE::HOT 
#define skip_this_if(check) if (check) {vec_size--; vecs[i] = vecs[vec_size]; continue; }
      for (int i = 0; i < vec_size; /*_*/ ) {
         const auto& v = *vecs[i];
         bool ok = is_cooling_candidate(v.bf) && v.versionCheck();
         skip_this_if(!ok);
         i++;
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
      [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
      COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      for (int i = 0; i < vec_size; /*_*/ ) {
         const auto& vec = *vecs[i];
         bool all_children_evicted = true;
         getDTRegistry().iterateChildrenSwips(vec.bf->page.dt_id, *vec.bf, [&all_children_evicted, &vec](Swip<BufferFrame>& swip) {
            ///*
            bool childEvicted = swip.isEVICTED();
            all_children_evicted &= childEvicted;
            return childEvicted && vec.versionCheck(); // only continue checking if childs are evicted
            //*/
            /*
            all_children_evicted = false;
            return false;
            */
            // recheck
         });
         skip_this_if(!all_children_evicted);
         i++;
      }
      COUNTERS_BLOCK()
      {
         iterate_children_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().iterate_children_ms +=
            (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
      }
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Time find_parent_begin, find_parent_end;
      COUNTERS_BLOCK() { find_parent_begin = std::chrono::high_resolution_clock::now(); }
      // -------------------------------------------------------------------------------------
      for (int i = 0; i < vec_size; /*_*/ ) {
         __builtin_prefetch(vecs[i]->bf->header.optimistic_parent_pointer.child.parent_bf);
         i++;
      }
      for (int i = 0; i < vec_size; /*_*/ ) {
         auto& vec = *vecs[i];
         DTID dt_id = vec.bf->page.dt_id;
         skip_this_if(!vec.versionCheck());
         // recheck
         vec.parent_handler.reset();
         bool parentfound = getDTRegistry().findParentNoJump(dt_id, *vec.bf, vec.parent_handler);
         //ensure(bfs_parent_handler_vec[i].swip->isHOT());
         //bfs_parent_handler_vec[i].parent_guard.recheck();
         //assert(bfs_parent_handler_vec[i].parent_guard.latch && bfptr == bf);
         //skip_this_if(!vec.parent_handler.swip->isHOT()); // TODO hack, not sure why this even happens
         skip_this_if(!parentfound);
         i++;
      }
      // -------------------------------------------------------------------------------------
      if (FLAGS_optimistic_parent_pointer) {
         for (int i = 0; i < vec_size; i++) {
            auto& vec = *vecs[i];
            if (vec.parent_handler.is_bf_updated) {
               vec.bf->header.latch.version += 2; // TODO WHY?
               auto& parent_handler = vec.parent_handler;
               assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
               assert(parent_handler.parent_guard.latch != reinterpret_cast<HybridLatch*>(0x99));
            }
         }
      }
      // -------------------------------------------------------------------------------------
      COUNTERS_BLOCK()
      {
         find_parent_end = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().find_parent_ms +=
            (std::chrono::duration_cast<std::chrono::microseconds>(find_parent_end - find_parent_begin).count());
      }
      // -------------------------------------------------------------------------------------
      /*
      r_guard.recheck();
      if (getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer, r_guard, parent_handler)) {
         r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions);
         continue;
      }
      */
      // -------------------------------------------------------------------------------------
      // Suitable page founds, lets cool
      for (int i = 0; i < vec_size; /*_*/ ) {
         auto& vec = *vecs[i];
         auto& bf = vec.bf; 
         auto& parent_handler = vec.parent_handler;
         skip_this_if(!vec.versionCheck());
         ///*
         Guard p_guard = std::move(parent_handler.parent_guard);
         //std::cout << " p_guard.v: " << p_guard.version;
         Guard to_r_x_guard(bf->header.latch);
         //std::cout << " i: " << i << std::hex << " parent_handler.p_bf: " << parent_handler.parent_bf << " pg.v: " << parent_handler.parent_guard.version;
         if (p_guard.tryToExclusive()) {
            //std::cout << " after p_x_guard.v: " << p_guard.version;
            to_r_x_guard.state = GUARD_STATE::OPTIMISTIC;
            to_r_x_guard.version = vec.version;
            if (to_r_x_guard.tryToExclusive()) {
               // -------------------------------------------------------------------------------------
               assert(bf->header.state == BufferFrame::STATE::HOT);
               assert(bf->header.isWB == false);
               assert(p_guard.version == p_guard.latch->ref().load());
               ensure(parent_handler.swip->isHOT());
               ensure(parent_handler.swip->bf == bf);
               // -------------------------------------------------------------------------------------
               partition.cooling_queue.push_back(reinterpret_cast<BufferFrame*>(bf));
               partition.cooling_bfs_counter++;
               ensure(partition.cooling_queue.size() == partition.cooling_bfs_counter);
               bf->header.state = BufferFrame::STATE::COOL;
               parent_handler.swip->cool();
               //std::cout << std:: hex << "bf: " << bf <<  " pid: " << bf->header.pid << " state: " << (int)bf->header.state << " swip.bf: " << parent_handler.swip->bf << " isCool: " << parent_handler.swip->isCOOL() << std::endl;
               found++;
               to_r_x_guard.unlock();
            }
            p_guard.unlock();
         }
         //*/
         /*
         jumpmuTry() {
            //JMUW<std::unique_lock<mean::SpinLock>> g_guard(partition.cooling_mutex);
            ExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
            Guard to_r_x_guard(bf->header.latch);
            to_r_x_guard.state = GUARD_STATE::OPTIMISTIC;
            to_r_x_guard.version = vec.version;
            OptimisticGuard r_guard(to_r_x_guard);
            ExclusiveGuard r_x_guard(r_guard);
            // -------------------------------------------------------------------------------------
            assert(bf->header.state == BufferFrame::STATE::HOT);
            assert(bf->header.isWB == false);
            assert(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
            assert(parent_handler.swip->bf == bf);
            // -------------------------------------------------------------------------------------
            partition.cooling_queue.push_back(reinterpret_cast<BufferFrame*>(bf));
            bf->header.state = BufferFrame::STATE::COOL;
            parent_handler.swip->cool();
            partition.cooling_bfs_counter++;
            found++;
         } jumpmuCatch() {}
         //*/
         //std::cout  << std::endl;
         i++;
      }
      failed_attempts++;
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
   } // while inner
   failed_attempts = 0;
   COUNTERS_BLOCK()
   {
      phase_1_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
   }
   COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter += found; }
   COUNTERS_BLOCK() { PPCounters::myCounters().failed_bf_pick_attempts += (attempts - found); }
   COUNTERS_BLOCK() { PPCounters::myCounters().failed_bf_pick_attempts_cause_dbg += failed_cause_not_hot; }
   return found;
}
// -------------------------------------------------------------------------------------
u64 BufferManager::pageProviderPhase1(CoolingPartition& partition, const u64 required, int partition_id)  {
   using Time = decltype(std::chrono::high_resolution_clock::now());
   /*
    * Phase 1: unswizzle pages (put in the cooling stage)
    */
   // -------------------------------------------------------------------------------------
   [[maybe_unused]] Time phase_1_begin, phase_1_end;
   COUNTERS_BLOCK() { phase_1_begin = std::chrono::high_resolution_clock::now(); }
   // -------------------------------------------------------------------------------------
   const int max_partitions = FLAGS_pp_threads;
   ensure(partition_id < max_partitions);
   BufferFrame* volatile r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions);  // Attention: we may set the r_buffer to a child of a bf instead of random
   // -------------------------------------------------------------------------------------
   volatile u64 failed_cause_not_hot = 0;
   volatile u64 failed_cause_io = 0;
   volatile u64 failed_cause_cool = 0;
   volatile u64 failed_cause_iocoldone = 0;
   volatile u64 found = 0;
   volatile u64 attempts = 0;
   volatile u64 failed_attempts =
      0;  // [corner cases]: prevent starving when free list is empty and cooling to the required level can not be achieved
#define repickIf(cond)                 \
   if (cond) {                         \
      r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions); \
      failed_attempts++;               \
      continue;                        \
   }
   while (failed_attempts < 10) {
      jumpmuTry()
      {
         while (found < required && failed_attempts < 10) {
            COUNTERS_BLOCK() { PPCounters::myCounters().phase_1_counter++; }
            attempts++;
            OptimisticGuard r_guard(r_buffer->header.latch, true);
            // -------------------------------------------------------------------------------------
            // Performance crticial: we should cross cool (unswizzle), otherwise write performance will drop
            bool is_cooling_candidate = (!r_buffer->header.keep_in_memory && !r_buffer->header.isWB &&
                                               !(r_buffer->header.latch.isExclusivelyLatched())
                                               // && (partition_i) >= p_begin && (partition_i) <= p_end
                                               && (r_buffer->header.state == BufferFrame::STATE::HOT ));
            failed_cause_not_hot += r_buffer->header.state == BufferFrame::STATE::HOT ? 0 : 1;
            failed_cause_io += r_buffer->header.state == BufferFrame::STATE::IOCOLD ? 1 : 0;
            failed_cause_cool += r_buffer->header.state == BufferFrame::STATE::COOL ? 1 : 0;
            failed_cause_iocoldone += r_buffer->header.state == BufferFrame::STATE::IOCOLDDONE ? 1 : 0;
            // test: do not evict: warehouse, district, new order, iitem
            //is_cooling_candidate = is_cooling_candidate && r_buffer->page.dt_id != 9 && r_buffer->page.dt_id != 0 && r_buffer->page.dt_id != 1 && r_buffer->page.dt_id != 5;
            repickIf(!is_cooling_candidate);
            r_guard.recheck();
            //
            // -------------------------------------------------------------------------------------
            COUNTERS_BLOCK() { PPCounters::myCounters().touched_bfs_counter++; }
            // -------------------------------------------------------------------------------------
            bool picked_a_child_instead = false, all_children_evicted = true;
            [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
            COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
            getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer,
               [&](Swip<BufferFrame>& swip) {
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
               iterate_children_end = std::chrono::high_resolution_clock::now();
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
            // -------------------------------------------------------------------------------------
            // -------------------------------------------------------------------------------------
            ParentSwipHandler parent_handler;
            getDTRegistry().findParent(dt_id, *r_buffer, parent_handler);
            // -------------------------------------------------------------------------------------
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
            if (getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer, r_guard, parent_handler)) {
               r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions);
               continue;
            }
            r_guard.recheck();
            // -------------------------------------------------------------------------------------
            // Suitable page founds, lets cool
            {
               //JMUW<std::unique_lock<mean::SpinLock>> g_guard(partition.cooling_mutex);
               ExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
               ExclusiveGuard r_x_guard(r_guard);
               /*
               if (!parent_handler.swip->isHOT()) {
                  failed_attempts++;
                  break;
               }
               */
               // -------------------------------------------------------------------------------------
               assert(r_buffer->header.state == BufferFrame::STATE::HOT );
               assert(r_buffer->header.isWB == false);
               assert(parent_handler.parent_guard.version == parent_handler.parent_guard.latch->ref().load());
               assert(parent_handler.swip->bf == r_buffer);
               // -------------------------------------------------------------------------------------
               partition.cooling_queue.push_back(reinterpret_cast<BufferFrame*>(r_buffer));
               partition.cooling_bfs_counter++;
               ensure(partition.cooling_queue.size() == partition.cooling_bfs_counter);
               r_buffer->header.state = BufferFrame::STATE::COOL;
               parent_handler.swip->cool();
            }
            found++;
            // -------------------------------------------------------------------------------------
            COUNTERS_BLOCK() { PPCounters::myCounters().unswizzled_pages_counter++; }
            // -------------------------------------------------------------------------------------
            r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions);
            // -------------------------------------------------------------------------------------
         } // while inner
         failed_attempts = 0;
         jumpmu_break;
      }
      jumpmuCatch() { r_buffer = &partitionRandomBufferFrame(partition_id, max_partitions); }
   } // while outer
   COUNTERS_BLOCK()
   {
      phase_1_end = std::chrono::high_resolution_clock::now();
      PPCounters::myCounters().phase_1_ms += (std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end - phase_1_begin).count());
   }
   if (attempts > found) {
      COUNTERS_BLOCK() { PPCounters::myCounters().failed_bf_pick_attempts += (attempts - found); }
      COUNTERS_BLOCK() { PPCounters::myCounters().failed_bf_pick_attempts_cause_dbg += failed_cause_not_hot; }
   }
   return found;
}
// -------------------------------------------------------------------------------------
int BufferManager::pageProviderPhase2(CoolingPartition& partition, const u64 pages_to_iterate_partition, FreedBfsBatch&  freed_bfs_batch) {
   if (partition.state.debug_thread == -1) {
      partition.state.debug_thread = mean::exec::getId();
   }
   ensure(partition.state.debug_thread == mean::exec::getId());
   COUNTERS_BLOCK() { PPCounters::myCounters().phase_2_counter++; }
   volatile s64 pages_left_to_iterate_partition = pages_to_iterate_partition;
   //std::cout << "pl:" << p_i << " pages_left_to_iterate_partition: "  << pages_left_to_iterate_partition  << " q: " << partition.cooling_queue.size() << std::endl;
   volatile u64 added = 0;
   volatile bool evictedCalledd = false;
   volatile bool fromTheBeginning = false;
   //volatile s64 left_in_q = partition.cooling_queue.size();
   //const int submitMultiple = mean::exec::ioChannel().submitMin();
   BufferFrame* bf_arr;
   pages_left_to_iterate_partition = std::min(pages_to_iterate_partition, partition.cooling_queue.size());
   while (pages_left_to_iterate_partition > 0  
         && !mean::exec::ioChannel().writeStackFull() && partition.outstanding < (s64)partition.io_queue.max_size) {
      if (!partition.cooling_queue.try_pop(bf_arr)) {
         break;
      }
      pages_left_to_iterate_partition--;
      partition.cooling_bfs_counter--;
      ensure(partition.cooling_queue.size() == partition.cooling_bfs_counter);
      // TODO think about RAID
      // || (submitMultiple > 0 && added % submitMultiple != 0)) 
      // // try that submitted is always multiple of raid submit count, even if it means we have to submit more than necessary
      // && (submitMultiple <= 0 || added % submitMultiple != 0 || (left_in_q >= submitMultiple*10))
      // if it looks like we aren't gonna have enough enqued to have enough submissions, do not submit anything at all 
      BufferFrame& bf = *bf_arr;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         OptimisticGuard o_guard(bf.header.latch, true);
         // Check if the BF got swizzled in or unswizzle another time in another partition
         if (bf.header.state != BufferFrame::STATE::COOL) {
            fromTheBeginning = true;
            jumpmu::jump();
         }
         if (!bf.header.isWB) {
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               IoPartition& io_partition = getIoPartition(bf.header.pid);
               if (io_partition.io_mutex.try_lock()) {
                  if (io_partition.io_ht.lookup(bf.header.pid)) {
                     io_partition.io_mutex.unlock();
                     jumpmu::jump();
                  }
                  io_partition.io_mutex.unlock();
               } else {
                  //std::cout << "io_mutex" << std::endl;
                  jumpmu::jump();
               }
            }
            pages_left_to_iterate_partition--;
            if (bf.isDirty()) {
               ensure(partition.outstanding >= 0);
               if (!mean::exec::ioChannel().writeStackFull() && partition.outstanding < (s64)partition.io_queue.max_size) {
                  {
                     ExclusiveGuard ex_guard(o_guard);
                     assert(!bf.header.isWB);
                     bf.header.isWB = true;
                     //pbf.header.state = BufferFrame::STATE::IOCOLD;
                  }
                  {
                     SharedGuard s_guard(o_guard);
                     PID wb_pid = bf.header.pid;
                     if (FLAGS_out_of_place) {
                        wb_pid = partition.nextPID();
                     }
                     mean::UserIoCallback cb;
                     cb.user_data.val.ptr = &bf;
                     cb.user_data2.val.u = bf.page.GSN;
                     cb.user_data3.val.ptr = &partition; 
                     cb.callback = [](mean::IoBaseRequest* req) {
                        auto& written_bf = *req->user.user_data.as<BufferFrame*>();
                        auto written_lsn = req->user.user_data2.val.u;
                        auto& partition = *req->user.user_data3.as<CoolingPartition*>();
                        while (true) {
                           jumpmuTry()
                           {
                              Guard guard(written_bf.header.latch);
                              guard.toExclusive();
                              //written_bf.header.state = BufferFrame::STATE::IOCOLDDONE;
                              assert(written_bf.header.isWB);
                              assert(written_bf.header.lastWrittenGSN < written_lsn);
                              // -------------------------------------------------------------------------------------
                              if (FLAGS_out_of_place) {
                                 PID old_pid = written_bf.header.pid;
                                 auto out_of_place_pid = req->out_of_place_addr;
                                 written_bf.header.pid = out_of_place_pid;
                                 partition.freePage(old_pid);
                              }
                              written_bf.header.lastWrittenGSN = written_lsn;
                              written_bf.header.isWB = false;
                              PPCounters::myCounters().flushed_pages_counter++;
                              // -------------------------------------------------------------------------------------
                              guard.unlock();
                              jumpmu_break;
                           }
                           jumpmuCatch() {
                              std::cout << "pp2 io callback catch: " << (int)written_bf.header.state << std::endl;
                           }
                        }
                        partition.state.done++;
                        //std::cout << "i: " << &written_bf << std::endl;
                        partition.io_queue.push_back(&written_bf);
                        partition.io_queue2.push_back(&written_bf);
                     };
                     bf.page.magic_debugging_number = wb_pid;
                     bf.page.magic_debugging_number_end = wb_pid;
                     mean::exec::ioChannel().pushWrite(
                           reinterpret_cast<char*>(&bf.page), PAGE_SIZE * wb_pid, PAGE_SIZE, /*reinterpret_cast<uintptr_t>(&bf), pid,*/ cb, true);
                     partition.state.submitted++;
                     partition.outstanding++; // 1
                     ensure(partition.outstanding <= (s64)partition.io_queue.max_size);
                     added++;
                     COUNTERS_BLOCK() { ThreadCounters::myCounters().pp_p2_iopushed++; }
                  }
               } else {
                  partition.cooling_bfs_counter++;
                  partition.cooling_queue.push_back(&bf);
                  ensure(partition.cooling_queue.size() == partition.cooling_bfs_counter);
                  //std::cout << "might be the issue" << std::endl;
                  jumpmu_break;
               }
            } else {
               __builtin_prefetch(bf.header.optimistic_parent_pointer.child.parent_bf,0,1);
               evictedCalledd = true;
               bool p1 = false;
               bool p2 = false;
               evict_bf(partition, freed_bfs_batch, bf, o_guard, p1, p2);
               ensure(p1 && p2);
            }
         } else {
            // is already write back, do nothing skip
            //std::cout << "is allready write back" << std::endl;
         }
      }
      jumpmuCatch() {
         if (bf.header.state == BufferFrame::STATE::COOL) {
            partition.cooling_bfs_counter++;
            partition.cooling_queue.push_back(&bf);
            ensure(partition.cooling_queue.size() == partition.cooling_bfs_counter);
            //bf.header.state = BufferFrame::STATE::IOLOST;
         } else if (bf.header.state != BufferFrame::STATE::HOT 
               && bf.header.state != BufferFrame::STATE::FREE 
               && bf.header.state != BufferFrame::STATE::IOCOLD
               && bf.header.state != BufferFrame::STATE::IOCOLDDONE
               && bf.header.state != BufferFrame::STATE::LOADED ) {
            std::cout << "state: " << (int)bf.header.state << " evicted: " << evictedCalledd << " fromthe: " << fromTheBeginning << std::endl; 
         }
      }
   }
   ensure(mean::exec::ioChannel().submitMin() == 0 || added % mean::exec::ioChannel().submitMin() == 0);
   return added;
}
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderPhase3evict(CoolingPartition& partition, FreedBfsBatch& freed_bfs_batch) {
   static std::atomic<u64> bla = 0;
   // no lock required as only this thread is accessing the io_queue
   // -------------------------------------------------------------------------------------
   BufferFrame* bf_ptr;
   int in = partition.io_queue2.size();
   volatile int cntDone = 0;
   volatile int cntCatch = 0;
   while (!partition.io_queue2.empty() && in-- > 0) {
      ensure(partition.io_queue.try_pop(bf_ptr));
      //std::cout << "e: " << bf_ptr << std::endl;
      //ensure(bf_ptr == partition.io_queue2.front());
      bf_ptr = partition.io_queue2.front();
      partition.io_queue2.pop_front();
      ensure(partition.outstanding >= 0);
      ensure(partition.outstanding <= (s64)partition.io_queue.max_size);
      partition.outstanding--; // 2

      BufferFrame& bf = *bf_ptr;
      volatile bool jumpCool = false;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         OptimisticGuard o_guard(bf.header.latch, true);
         if (bf.header.state != BufferFrame::STATE::COOL && bf.header.state != BufferFrame::STATE::IOCOLDDONE && bf.header.state != BufferFrame::STATE::IOLOST2) {
            jumpCool = true;
            jumpmu::jump();
         }
         if (!bf.header.isWB && !bf.isDirty()) {
            bool p1 = false;
            bool p2 = false;
            evict_bf(partition, freed_bfs_batch, bf, o_guard, p1, p2);
            ensure(p1 && p2);
            cntDone++;
         } else {
            //bla++;
            //std::cout << "else " << &bf << std::dec << " isWb: " << bf.header.isWB << " isDirty: " << bf.isDirty() <<  " page lost " << bla << " state " << (int)bf.header.state << std::endl;
            //partition.outstanding++;
            //partition.io_queue.push_back(bf_ptr);
            //partition.io_queue2.push_back(bf_ptr);
            //bf.header.state = BufferFrame::STATE::IOLOST;
         }
         // -------------------------------------------------------------------------------------
      }
      jumpmuCatch() {
         //std::cout << "catch jumpCool " << jumpCool << " "<< &bf << std::dec << " isWb: " << bf.header.isWB << " isDirty: " << bf.isDirty() <<  " page lost " << bla << " state " << (int)bf.header.state << std::endl;
         ///*
         cntCatch++;
         if (!jumpCool) {
            partition.outstanding++; // 3
            partition.io_queue.push_back(bf_ptr);
            partition.io_queue2.push_back(bf_ptr);
            //bf.header.state = BufferFrame::STATE::IOLOST2;
            ensure(partition.outstanding <= (s64)partition.io_queue.max_size);
         } else if (bf.header.state == BufferFrame::STATE::IOCOLDDONE) {
            partition.outstanding++; // 4
            partition.io_queue.push_back(bf_ptr);
            partition.io_queue2.push_back(bf_ptr);
         }
         //*/
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
