#pragma once
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
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
   u64 dram_pool_size;                       // total number of dram buffer frames
   atomic<u64> ssd_freed_pages_counter = 0;  // used to track how many pages did we really allocate
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   u64 partitions_count;
   u64 partitions_mask;
   std::vector<std::unique_ptr<Partition>> partitions;

  private:
   // -------------------------------------------------------------------------------------
   // Threads managements
   void pageProviderThread(u64 p_begin, u64 p_end);  // [p_begin, p_end)
   atomic<u64> bg_threads_counter = 0;
   atomic<bool> bg_threads_keep_running = true;
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   BufferFrame& randomBufferFrame();
   Partition& getPartition(PID);
   u64 getPartitionID(PID);

  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame& allocatePage();
   inline BufferFrame& tryFastResolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
   {
      if (swip_value.isHOT()) {
         BufferFrame& bf = swip_value.asBufferFrame();
         swip_guard.recheck();
         return bf;
      } else {
         return resolveSwip(swip_guard, swip_value);
      }
   }
   BufferFrame& resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value);
   void coolPage(BufferFrame& bf);
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
