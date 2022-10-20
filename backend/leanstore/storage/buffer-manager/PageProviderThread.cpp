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
void BufferManager::pageProviderThread()  // [p_begin, p_end)
{
   // Init Worker itself
   pthread_setname_np(pthread_self(), "page_provider");
   // TODO: register as special worker [hackaround atm]
   while (!cr::Worker::init_done);
   cr::Worker::tls_ptr = new cr::Worker(0, nullptr, 0, ssd_fd);

   // Init Datastructures
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
   std::vector<std::vector<BufferFrame*>> nextBufferFrames;
   std::vector<Partition*> partitions;
   std::vector<u8> sampleSizes;
   nextBufferFrames.resize(partitions_count);
   partitions.resize(partitions_count);
   sampleSizes.resize(partitions_count);
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      nextBufferFrames[p_i].reserve(getPartition(p_i).next_bfs_limit + 1);
      partitions[p_i] = &getPartition(p_i);
      sampleSizes[p_i] = FLAGS_watt_samples;
   }

   // Do Work
   if(FLAGS_epoch_size < 1){
      FLAGS_epoch_size = 1;
   }
   u64 evicted_limit = std::max((u64) 1, (u64) dram_pool_size / FLAGS_epoch_size), pages_evicted=0, total_evictions=0;
   if(evicted_limit <1){
      evicted_limit = 1;
   }
   wait_for_start(partitions);
   while (bg_threads_keep_running) {
      for (u64 p_i = 0; p_i < partitions.size(); p_i++) {
         Partition& myPart = *partitions[p_i];
         if(myPart.is_page_provided.exchange(true)){
            // alreaddy handled;
            continue;
         }
            if (myPart.partition_size < myPart.max_partition_size - 10) {
            myPart.is_page_provided = false;
            continue;
         }
         std::pair<double, double> min = findThreshold(sampleSizes[p_i]);
         evictPages(min, p_i, async_write_buffer, partitions, pages_evicted);
         while(pages_evicted >= evicted_limit){
            pages_evicted -= evicted_limit;
            total_evictions += evicted_limit;
            leanstore::storage::BufferFrame::globalTrackerTime++;
         }
         myPart.is_page_provided = false;
      }
   }

   total_evictions+= pages_evicted;
   cout << "Pages Evicted: " << +total_evictions << endl;

   // Finish
   bg_threads_counter--;
}
void BufferManager::wait_for_start(const std::vector<Partition*>& partitions) const
{
   u64 wait_till = partitions[0]->max_partition_size*0.95;
   cout << "mx_part_size: " << partitions[0]->max_partition_size <<endl;
   while(bg_threads_keep_running) {
      for(Partition* part: partitions) {
         Partition& myPart = *part;
         myPart.is_page_provided = false;
         if (myPart.partition_size > wait_till) {
            cout << "Start pageproviding" << endl;
            return;
         }
      }
   }
}

bool page_is_evictable(BufferFrame& page) {
   return page.header.state == BufferFrame::STATE::HOT      // Only HOT Pages are Evictable
          && !page.header.keep_in_memory                    // Is Fixed in Memory
          && !page.header.isInWriteBuffer                   // Check if already in WriteBuffer via "isInWriteBuffer"
          && !(page.header.latch.isExclusivelyLatched());   // Is in Use
};

std::pair<double, double> BufferManager::findThreshold(int samples)
{
   volatile double min = 500000;
   volatile double next_min = 500000;
   volatile int i=0;
   while(i< samples) {
      jumpmuTry()
      {
         while(i< samples) {
            BufferFrame& r_buffer = *randomBufferFrame();
            BMOptimisticGuard r_guard(r_buffer.header.latch);

            if(!page_is_evictable(r_buffer)){continue;}

            if(!childrenEvicted(r_guard, r_buffer)){
               continue;
            }
            i+=1;
            double value = r_buffer.header.tracker.getValue();
            if(value < min){
               next_min = min;
               min=value;
            }
            else if (value < next_min){
               next_min = value;
            }
         }
         jumpmu_break;
      }
      jumpmuCatch(){};
   }
   double calculated_min = min;
   double second_value = min*1.3;
   if(samples>1){
      calculated_min = (min + next_min) / 2.0;
      second_value = next_min;
   }
   last_min = calculated_min;
   return {calculated_min, second_value};
}
/***
 * Main Idea: check random BufferFrame (prioritize from nextBufferFrames list), if
 * NonEvictable: skip
 * Evictable and Clean: directly Evict
 * Evictable and Dirty: collect for group flush and add to nextBufferFrames list.
 * @param min
 * @param partitionID
 * @param async_write_buffer
 * @param nextBufferFrames
 */
bool BufferManager::evictPages(std::pair<double, double> min,
                               u64 partitionID,
                               AsyncWriteBuffer& async_write_buffer,
                               std::vector<Partition*>& partitions,
                               u64& pages_evicted)
{
   Partition& myPartition = *partitions[partitionID];
   FreedBfsBatch evictedOnes;
   u64 evictions = 0;
   {
      u64 tmp = myPartition.partition_size;
      if(tmp > myPartition.max_partition_size){
         evictions = tmp - myPartition.max_partition_size;
      }
      if(evictions > 1000){
         evictions = 1000;
      }
   }
   const u64 toEvict = evictions;
   volatile u64 evictedPages = 0;
   volatile u64 fails = 0;
//   cout << "going to evict pages: " << toEvict << endl;
//   cout << "NextFrameList has Entrys: " << nextBufferFrames.size() <<endl;
   WATT_TIME curr_time = BufferFrame::globalTrackerTime.load();
   checkGoodBufferFrames(myPartition, min, curr_time);
   const u32 batch = 100;
   BufferFrame* frames[batch];
   while(evictedPages < toEvict && fails < 50*toEvict){
      for (u32 j=0; j<batch; j++) {
         frames[j] = getNextBufferFrame(myPartition);
         __builtin_prefetch(frames[j]);
      }
      for (u32 j=0; j<batch; j++) {
         fails++;
         jumpmuTry()
         {
            // Select and check random BufferFrame
            BufferFrame& r_buffer = *frames[j];

            BMOptimisticGuard r_guard(r_buffer.header.latch);

            if(!page_is_evictable(r_buffer)){jumpmu_continue;}
            double value = r_buffer.header.tracker.getValue(curr_time);
            if(value > min.first){
               if(value < min.second)
                  getPartition(r_buffer.header.pid).addGoodBufferFrame(&r_buffer);
               jumpmu_continue;
            }
            // If has children: Evict children, too? (children freq usually < parent freq)
            // Right now: abort if one child is not evicted
            // Pick one or All Childs for eviction (Problem with Partitioning, thats why not direct evict)?
            if(!childrenEvicted(r_guard, r_buffer)){
               jumpmu_continue;
            }
            PID pid = r_buffer.header.pid;
            if(getPartitionID(pid) != partitionID){
               PID p_id = getPartitionID(pid);
               if(r_buffer.isDirty()){
                  getPartition(p_id).addNextBufferFrame(&r_buffer);
               }
               else if (partitions[p_id]->partition_size > partitions[p_id]->max_partition_size){
                  nonDirtyEvict(r_buffer, r_guard, evictedOnes);
                  partitions[p_id]->partition_size--;
                  evictedPages++;
                  if (evictedOnes.size() > 100) {
                     evictedOnes.push();
                  }

               }
               jumpmu_continue;
            }
            // If clean: Evict directly;
            // Else: Add to async write buffer
            if (!r_buffer.isDirty()) {
               nonDirtyEvict(r_buffer, r_guard, evictedOnes);
               myPartition.partition_size--;
               evictedPages++;
               if (evictedOnes.size() > 100) {
                  evictedOnes.push();
               }
               fails--;
               jumpmu_continue;
            }
            // If Async Write Buffer is full, flush
            if (async_write_buffer.full()) {
               async_write_buffer.flush(myPartition);
            }
            {
               BMExclusiveGuard ex_guard(r_guard);
               r_buffer.header.isInWriteBuffer = true;
            }
            {
               BMSharedGuard s_guard(r_guard);
               PID wb_pid = r_buffer.header.pid;
               if (FLAGS_out_of_place) {
                  wb_pid = myPartition.nextPID();
                  assert(getPartitionID(r_buffer.header.pid) == partitionID);
                  assert(getPartitionID(wb_pid) == partitionID);
               }
               async_write_buffer.add(r_buffer, wb_pid);
            }
         }
         jumpmuCatch(){};
      }
   }
   // Finally flush
   async_write_buffer.flush(myPartition);
   if(evictedOnes.size()){
      evictedOnes.push();
   }
   pages_evicted += evictedPages;
   return (evictedPages == toEvict);
}
void BufferManager::checkGoodBufferFrames(Partition& partition, std::pair<double, double> threshold, WATT_TIME curr_time)
{
   if(partition.good_queue.size() == 0){
      return;
   }
   if(curr_time == partition.last_good_check){
      return;
   }
   partition.last_good_check = BufferFrame::globalTrackerTime.load();
   partition.good_mutex.lock();
   std::vector<BufferFrame*> evictions, keep;
   for(BufferFrame* good : partition.good_queue){
      double value = good->header.tracker.getValue(curr_time);
      if (value < threshold.first){
         evictions.emplace_back(good);
      }else if(value < threshold.second){
         keep.emplace_back(good);
      }
   }
   // cout << "check" << evictions.size() << " " << keep.size() << endl;
   partition.good_queue.swap(keep);
   partition.good_mutex.unlock();
   partition.next_mutex.lock();
   for(BufferFrame* next: evictions){
      partition.next_queue.emplace_back(next);
   }
   partition.next_mutex.unlock();
}
BufferFrame* BufferManager::getNextBufferFrame(Partition& partition)
{
   partition.next_mutex.lock();
   if(partition.next_queue.empty()) {
      partition.next_mutex.unlock();
      return randomBufferFrame();
   }
   BufferFrame* next = partition.next_queue.back();
   partition.next_queue.pop_back();
   partition.next_mutex.unlock();
   return next;
}
bool BufferManager::childrenEvicted(BMOptimisticGuard& r_guard, BufferFrame& r_buffer)
{
   bool all_children_evicted;
   all_children_evicted= true;
   getDTRegistry().iterateChildrenSwips(r_buffer.page.dt_id, r_buffer, [&](Swip<BufferFrame>& swip) {
      all_children_evicted &= swip.isEVICTED();
      if (swip.isHOT()) {
         all_children_evicted = false;
         return false;
      }
      r_guard.recheck();
      return true;
   });
   return all_children_evicted;
}
void BufferManager::nonDirtyEvict(BufferFrame& bf, BMOptimisticGuard& guard, FreedBfsBatch& evictedOnes)
{
   DTID dt_id = bf.page.dt_id;
   guard.recheck();
   ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
   assert(parent_handler.parent_guard.state == GUARD_STATE::OPTIMISTIC);
   BMExclusiveUpgradeIfNeeded p_x_guard(parent_handler.parent_guard);
   guard.guard.toExclusive();
   // -------------------------------------------------------------------------------------
   assert(!bf.header.isInWriteBuffer);
   // Reclaim buffer frame
   parent_handler.swip.evict(bf.header.pid);
   // -------------------------------------------------------------------------------------
   // Reclaim buffer frame
   if(FLAGS_watt_history){
      bf.header.watt_backlog.store(bf.header.pid, bf.header.tracker);
   }
   bf.reset();
   bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
   bf.header.latch.mutex.unlock();
   // -------------------------------------------------------------------------------------
   evictedOnes.add(bf);
   COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; PPCounters::myCounters().total_evictions++; }
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
