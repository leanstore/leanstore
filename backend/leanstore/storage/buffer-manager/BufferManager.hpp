#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "AsyncWriteBuffer.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <sys/mman.h>

#include <cstring>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
class LeanStore;  // Forward declaration
namespace profiling
{
class BMTable;  // Forward declaration
}
namespace storage
{
// -------------------------------------------------------------------------------------
struct FreedBfsBatch {
   BufferFrame *freed_bfs_batch_head = nullptr, *freed_bfs_batch_tail = nullptr;
   u64 freed_bfs_counter = 0;
   FreeList* free_list = nullptr;
   // -------------------------------------------------------------------------------------
   void reset()
   {
      freed_bfs_batch_head = nullptr;
      freed_bfs_batch_tail = nullptr;
      freed_bfs_counter = 0;
   }
   // -------------------------------------------------------------------------------------
   void push()
   {
      paranoid(free_list != nullptr);
      if(freed_bfs_counter > 0){
         free_list->batchPush(freed_bfs_batch_head, freed_bfs_batch_tail, freed_bfs_counter);
      }
      reset();
   }
   // -------------------------------------------------------------------------------------
   u64 size() { return freed_bfs_counter; }
   // -------------------------------------------------------------------------------------
   void add(BufferFrame& bf)
   {
      if (freed_bfs_counter >= std::min<u64>(FLAGS_worker_threads, 128) && free_list != nullptr) {
         push();
      }
      bf.header.next_free_bf = freed_bfs_batch_head;
      if (freed_bfs_batch_head == nullptr) {
         freed_bfs_batch_tail = &bf;
      }
      freed_bfs_batch_head = &bf;
      freed_bfs_counter++;
      // -------------------------------------------------------------------------------------
   }
   void set_free_list(FreeList* list){
      if(free_list != nullptr && size()>0){
         push();
      }
      free_list = list;
   }
};
// -------------------------------------------------------------------------------------
// TODO: revisit the comments after switching to clock replacement strategy
// Notes on Synchronization in Buffer Manager
// Terminology: PPT: Page Provider Thread, WT: Worker Thread. P: Parent, C: Child, M: Cooling stage mutex
// Latching order for all PPT operations (unswizzle, evict): M -> P -> C
// Latching order for all WT operations: swizzle: [unlock P ->] M -> P ->C, coolPage: P -> C -> M
// coolPage conflict with this order which could lead to a deadlock which we can mitigate by jumping instead of blocking in BMPlainGuard [WIP]
// -------------------------------------------------------------------------------------
class BufferManager
{
  private:
   friend class leanstore::LeanStore;
   friend class leanstore::profiling::BMTable;
   // -------------------------------------------------------------------------------------
   BufferFrame* bfs;
   // -------------------------------------------------------------------------------------
   const int ssd_fd;
   // -------------------------------------------------------------------------------------
   // Free  Pages
   const u8 safety_pages = 10;               // we reserve these extra pages to prevent segfaults
   const u64 dram_pool_size;                 // total number of dram buffer frames
   atomic<u64> ssd_freed_pages_counter = 0;  // used to track how many pages did we really allocate
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   const u64 partitions_count;
   const u64 partitions_mask;
   std::vector<std::unique_ptr<Partition>> partitions;
   std::atomic<u64> clock_cursor = {0};
   // -------------------------------------------------------------------------------------
   // Threads managements
   void evict_bf(FreedBfsBatch& batch, BufferFrame& bf, BMOptimisticGuard& c_guard);
   bool pageIsNotEvictable(BufferFrame* r_buffer);
   struct PageProviderThread{
     private:
      const u64 id;
      BufferManager& bf_mgr;
      AsyncWriteBuffer async_write_buffer;
      const u64 evictions_per_epoch;
      u64 pages_evicted = 0;
      std::vector<BufferFrame*> prefetched_bfs, second_chance_bfs;
      FreedBfsBatch freed_bfs_batch;
      void set_thread_config();
      BufferFrame& randomBufferFrame();
      void prefetch_bf(u32 BATCH_SIZE);
      bool childInRam(BufferFrame* r_buffer, BMOptimisticGuard& r_guard, bool pickChild);
      ParentSwipHandler findParent(BufferFrame* r_buffer, BMOptimisticGuard& r_guard);
      bool checkXMerge(BufferFrame* r_buffer);
      double findThresholds();
      void evictPages(double threshold);
      void handleDirty(leanstore::storage::BMOptimisticGuard& o_guard,
                       leanstore::storage::BufferFrame* const volatile& cooled_bf,
                       const PID cooled_bf_pid);
      void handleWritten();

     public:
      PageProviderThread(u64 t_i, BufferManager* bf_mgr);
      void run();
   };

   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   FreeList& randomFreeList();
   BufferFrame& randomFreeFrame();
   PID randomFreePID();
   HashTable& getIOTable(PID);
   Partition& getPartition(PID);
   u64 getPartitionID(PID);
   // -------------------------------------------------------------------------------------
   // Temporary hack: let workers evict the last page they used
   static thread_local BufferFrame* last_read_bf;

  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame& allocatePage();
   inline BufferFrame& tryFastResolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
   {
      if (swip_value.isHOT()) {
         COUNTERS_BLOCK() {
            if(FLAGS_count_hits){
               leanstore::WorkerCounters::myCounters().hot_hit_counter++;}}
         BufferFrame& bf = swip_value.asBufferFrame();
         swip_guard.recheck();
         return bf;
      } else {
         return resolveSwip(swip_guard, swip_value);
      }
   }
   BufferFrame& resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value);
   void evictLastPage();
   void reclaimPage(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address
    * temporarily 2- if not, then posix_check if it exists in cooling stage
    * queue, yes? remove it from the queue and return the buffer frame 3- in
    * anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random
    * BufferFrame (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8* destination);
   void readPageAsync(PID pid, u8* destination, std::function<void()> callback);
   void fDataSync();
   // -------------------------------------------------------------------------------------
   void startBackgroundThreads();
   void stopBackgroundThreads();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
   // -------------------------------------------------------------------------------------
   u64 getPoolSize() { return dram_pool_size; }
   DTRegistry& getDTRegistry() { return DTRegistry::global_dt_registry; }
   u64 consumedPages();
   BufferFrame& getContainingBufferFrame(const u8*);  // get the buffer frame containing the given ptr address
};                                                    // namespace storage
// -------------------------------------------------------------------------------------
class BMC
{
  public:
   static BufferManager* global_bf;
};
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
