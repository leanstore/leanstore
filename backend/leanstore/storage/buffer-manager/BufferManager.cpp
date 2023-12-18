#include "BufferManager.hpp"

#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "Time.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/storage/btree/core/BTreeGeneric.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/Swip.hpp"
#include "leanstore/sync-primitives/Latch.hpp"
#include "leanstore/sync-primitives/PlainGuard.hpp"
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
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <set>
// -------------------------------------------------------------------------------------
// Local GFlags
// -------------------------------------------------------------------------------------
using std::thread;
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
BufferManager::BufferManager()
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      bfs = reinterpret_cast<BufferFrame*>(mean::IoInterface::allocIoMemoryChecked(dram_total_size, 512));
      // reinterpret_cast<BufferFrame*>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));

      // madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      // madvise(bfs, dram_total_size,
      //        MADV_DONTFORK);  // O_DIRECT does not work with forking.
      // -------------------------------------------------------------------------------------
      // Initialize partitions
      cooling_partitions_count = FLAGS_pp_threads;
      io_partitions_count = (1 << FLAGS_partition_bits);
      partitions_mask = io_partitions_count - 1;
      
      const u64 free_bfs_limit = std::max(std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(cooling_partitions_count)), 0.0); //128.0);
      const u64 cooling_bfs_upper_bound = std::max(std::ceil((FLAGS_cool_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(cooling_partitions_count)), 0.0);// 128.0);

      std::cout << "free_bfs_limit: " << free_bfs_limit << " cooling_bfs_upper_bound: " << cooling_bfs_upper_bound << std::endl;
      const u64 max_outsanding_ios = (FLAGS_async_batch_size + FLAGS_worker_tasks)*2;
      io_partitions = reinterpret_cast<IoPartition*>(malloc(sizeof(IoPartition) * io_partitions_count));
      for (u64 p_i = 0; p_i < io_partitions_count; p_i++) {
         new (io_partitions + p_i) IoPartition(max_outsanding_ios);
      }
      // -------------------------------------------------------------------------------------
      cooling_partitions = reinterpret_cast<CoolingPartition*>(malloc(sizeof(CoolingPartition) * cooling_partitions_count));
      for (u64 p_i = 0; p_i < cooling_partitions_count; p_i++) {
         new (cooling_partitions + p_i) CoolingPartition(p_i, cooling_partitions_count, free_bfs_limit, cooling_bfs_upper_bound, max_outsanding_ios);
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            cooling_partitions[p_i].dram_free_list.push(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % cooling_partitions_count;
         }
      });
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   /*
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
     flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
   if (FLAGS_falloc > 0) {
     const u64 gib_size = 1024ull * 1024ull * 1024ull;
     auto dummy_data = (u8*)aligned_alloc(512, gib_size);
     for (u64 i = 0; i < FLAGS_falloc; i++) {
       const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
       posix_check(ret == gib_size);
     }
     free(dummy_data);
     fsync(ssd_fd);
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   */
   // -------------------------------------------------------------------------------------
   // Background threads
   // -------------------------------------------------------------------------------------
   // Page Provider threads
   mean::env::registerPageProvider(this, cooling_partitions_count);
      /*
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
        thread& page_provider_thread = pp_threads[t_i];
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t_i, &cpuset);
        posix_check(pthread_setaffinity_np(page_provider_thread.native_handle(), sizeof(cpu_set_t), &cpuset) == 0);
        page_provider_thread.detach();
      }
      */
}
// -------------------------------------------------------------------------------------
void BufferManager::clearSSD()
{
   // TODO
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   // TODO
   //stopBackgroundThreads();
   ensure(!FLAGS_out_of_place);
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         // s64 ret = pwrite(ssd_fd, bf.page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
         mean::task::write(reinterpret_cast<char*>(&bf.page), bf.header.pid * PAGE_SIZE, PAGE_SIZE);
         bf.header.latch.mutex.unlock();
      }
   });
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   // TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < cooling_partitions_count; p_i++) {
      total_freed_pages += cooling_partitions[p_i].freedPages();
      total_used_pages += cooling_partitions[p_i].allocatedPages();
   }
   return total_used_pages - total_freed_pages;
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::getContainingBufferFrame(const u8* ptr)
{
   u64 index = (ptr - reinterpret_cast<u8*>(bfs)) / (sizeof(BufferFrame));
   return bfs[index];
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
CoolingPartition& BufferManager::randomCoolingPartition()
{
   auto rand_partition_i = utils::RandomGenerator::getRandU64(0, cooling_partitions_count);
   return cooling_partitions[rand_partition_i];
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRandU64(0, dram_pool_size);//getRandU64STD(0, dram_pool_size);//getRand<u64>(0, dram_pool_size);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::partitionRandomBufferFrame(u64 partition, u64 max_partitions)
{
   // it's a bit more complex as pool_size might not be a multiple of partitions_count 
   u64 bfs_remaining = dram_pool_size % max_partitions;
   u64 bfs_this_partition = dram_pool_size/max_partitions + (partition < bfs_remaining ? 1 : 0); // +1 if this partition has a page more
   auto rand_buffer_i = partition + utils::RandomGenerator::getRandU64(0, bfs_this_partition) * cooling_partitions_count;
   assert(rand_buffer_i < dram_pool_size);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
u64 BufferManager::partitionRandomBufferFramePos(u64 partition, u64 max_partitions)
{
   // it's a bit more complex as pool_size might not be a multiple of partitions_count 
   u64 bfs_remaining = dram_pool_size % max_partitions;
   u64 bfs_this_partition = dram_pool_size/max_partitions + (partition < bfs_remaining ? 1 : 0); // +1 if this partition has a page more
   auto rand_buffer_i = partition + utils::RandomGenerator::getRandU64(0, bfs_this_partition) * cooling_partitions_count;
   assert(rand_buffer_i < dram_pool_size);
   return rand_buffer_i;
}
// -------------------------------------------------------------------------------------
CoolingPartition& BufferManager::getCoolingPartition(BufferFrame& bf) {
   u64 pos = &bf - bfs;
   return cooling_partitions[pos % cooling_partitions_count];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   // Pick a pratition randomly
   CoolingPartition& partition = randomCoolingPartition();
   BufferFrame& free_bf = partition.dram_free_list.pop();
   free_bf.header.newPage = true;
   PID free_pid = partition.nextPID();
   assert(free_bf.header.state == BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.latch.assertNotExclusivelyLatched();
   free_bf.header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::STATE::HOT;
   free_bf.header.lastWrittenGSN = free_bf.page.GSN = 0;
   // -------------------------------------------------------------------------------------
   if (free_pid == dram_pool_size) {
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Going out of memory !" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().allocate_operations_counter++; }
   // -------------------------------------------------------------------------------------
   return free_bf;
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
   CoolingPartition& partition = getCoolingPartition(bf);
   partition.freePage(bf.header.pid);
   // -------------------------------------------------------------------------------------
   if (bf.header.isWB) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      cout << "garbage collector, yeah" << endl;
   } else {
      CoolingPartition& partition = getCoolingPartition(bf);
      bf.reset();
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      partition.dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// returns a non-latched BufferFrame
BufferFrame& BufferManager::resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
{
   if (swip_value.isHOT()) {
      BufferFrame& bf = swip_value.bfRef();
      swip_guard.recheck();
      return bf;
   } else if (swip_value.isCOOL()) {
      BufferFrame* bf = swip_value.bfPtrAsHot();
      swip_guard.recheck();
      OptimisticGuard bf_guard(bf->header.latch, true);
      ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);  // parent
      ExclusiveGuard bf_x_guard(bf_guard);                // child
      bf->header.state = BufferFrame::STATE::HOT;
      swip_value.warm();
      assert(swip_value.bf >= bfs && swip_value.bf <= bfs + dram_pool_size + safety_pages);
      return swip_value.bfRef();
   }
   auto setOptimisticParentPointer = [this](Guard& swip_x_guard, Swip<BufferFrame>& swip_value){
      if (FLAGS_optimistic_parent_pointer) {
         jumpmuTry()
         {
            // assumes parent and child are exclusively locked 
            swip_value.bfRef().header.optimistic_parent_pointer.parent.last_swip_invalidation_version = swip_value.bfRef().header.latch.version;
            // hacky, have to find parent bf from swip, pid and  pos. 
            static_assert(sizeof(BufferFrame) == 512+PAGE_SIZE);
            u64 bf_index = ((u64)swip_x_guard.latch - (u64)this->bfs) / sizeof(BufferFrame);
            BufferFrame& parent_bf = this->bfs[bf_index];
            assert(&parent_bf.header.latch == swip_x_guard.latch); // the above calculation is correct
            assert(swip_x_guard.state == GUARD_STATE::EXCLUSIVE); // the parent is exclusively locked.
            assert((swip_value.bfRef().header.latch.version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT); // the child is exclusively locked.
            // the position is needed in xmerge and contention split. They can call lowerBound to find it, just retrun -1.
            s32 pos = -1; 
            swip_value.bfRef().header.optimistic_parent_pointer.child.update(&parent_bf, parent_bf.header.pid,
                  reinterpret_cast<BufferFrame**>(&swip_value),
                  pos, parent_bf.header.latch.version, "bm"); // swip_guard == parent_bf.header.latch.version
            assert(swip_value.isHOT());
            //parent_handler.is_bf_updated = true;
         }
         jumpmuCatch() {}
      }
   };
   // -------------------------------------------------------------------------------------
   swip_guard.unlock();  // otherwise we would get a deadlock, P->G, G->P
   const PID pid = swip_value.asPageID();
   IoPartition& partition = getIoPartition(pid);
   JMUW<std::unique_lock<mean::mutex>> g_guard(partition.io_mutex);
   swip_guard.recheck();
   assert(!swip_value.isHOT());
   // -------------------------------------------------------------------------------------
   auto frame_handler = partition.io_ht.lookup(pid);
   if (!frame_handler) {
      BufferFrame& bf = randomCoolingPartition().dram_free_list.tryPop(g_guard);  // EXP
      IOFrame& io_frame = partition.io_ht.insert(pid);
      assert(bf.header.state == BufferFrame::STATE::FREE);
      bf.header.latch.assertNotExclusivelyLatched();
      // -------------------------------------------------------------------------------------
      io_frame.state = IOFrame::STATE::READING;
      io_frame.readers_counter = 1;
      io_frame.mutex.lock(); /// HEREREER
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      auto start = mean::getTimePoint();
      readPageSync(pid, bf.page);
      /*
      //induce latnecy spikes
      if (leanstore::utils::RandomGenerator::getRand(0, 2*1000*1000) < 1) {
         auto start = mean::getTimePoint();
         int i = 0;
         for (int i = 0; i  < 10000; i++) {
            mean::task::yield();
         }
         auto end = mean::getTimePoint();
         std::cout << "wait: " << mean::timePointDifferenceUs(end, start) << std::endl;
      }
      */
      COUNTERS_BLOCK()
      {
         // WorkerCounters::myCounters().dt_misses_counter[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRandU64(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      assert(bf.page.magic_debugging_number == pid);
      assert(bf.page.magic_debugging_number_end == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      assert(!bf.header.isWB);
      bf.header.lastWrittenGSN = bf.page.GSN;
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      io_frame.bf =(BufferFrame*)0x1111;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         if (io_frame.readers_counter > 250) {
            auto done = mean::getTimePoint();
            std::cout << "high reader count read frame: " << io_frame.readers_counter << " diff " << mean::timePointDifferenceUs(done, start) << std::endl;
            //raise(SIGINT);
         }
         swip_guard.recheck();
         JMUW<std::unique_lock<mean::mutex>> g_guard(partition.io_mutex);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         io_frame.mutex.unlock();
         swip_value.warm(&bf);
         bf.header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                   // IT IS SWIZZLED IN
         { // opp
            OptimisticGuard bf_guard(bf.header.latch);
            ExclusiveGuard bf_x_guard(bf_guard);
            setOptimisticParentPointer(swip_guard, swip_value);
         }
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         }
         // -------------------------------------------------------------------------------------
         // -------------------------------------------------------------------------------------
         jumpmu_return bf;
      }
      jumpmuCatch()
      {
         // Change state to ready
         g_guard->lock();
         io_frame.bf = &bf;
         io_frame.state = IOFrame::STATE::READY;
         // -------------------------------------------------------------------------------------
         g_guard->unlock();
         io_frame.mutex.unlock();
         // -------------------------------------------------------------------------------------
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   IOFrame& io_frame = frame_handler.frame();
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READING) {
      io_frame.readers_counter++;  // incremented while holding partition lock
      g_guard->unlock();
      io_frame.mutex.lock(); //// HEREERERE
      io_frame.mutex.unlock();
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         g_guard->lock();
         if (io_frame.readers_counter == 0) {
            partition.io_ht.remove(pid);
         }
         g_guard->unlock();
      }
      // -------------------------------------------------------------------------------------
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READY) {
      // -------------------------------------------------------------------------------------
      BufferFrame* volatile bf = io_frame.bf;
      {
         // We have to exclusively lock the bf because the page provider thread will
         // try to evict them when its IO is done
         bf->header.latch.assertNotExclusivelyLatched();
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         OptimisticGuard bf_guard(bf->header.latch);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         ExclusiveGuard bf_x_guard(bf_guard);
         // -------------------------------------------------------------------------------------
         io_frame.bf = nullptr;
         assert(bf->header.pid == pid);
         swip_value.warm(bf);
         // -------------------------------------------------------------------------------------
         setOptimisticParentPointer(swip_guard, swip_value);
         // -------------------------------------------------------------------------------------
         assert(swip_value.isHOT());
         assert(swip_value.bf >= bfs && swip_value.bf <= bfs + dram_pool_size + safety_pages);
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         bf->header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                      // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter > 180) {
            std::cout << "frame: " << io_frame.readers_counter << " bf.dt_id: " << bf->page.dt_id<< std::endl;
            //raise(SIGINT);
         }
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         } else {
            io_frame.state = IOFrame::STATE::TO_DELETE;
         }
         g_guard->unlock();
         // -------------------------------------------------------------------------------------
         return *bf;
      }
   }
   if (io_frame.state == IOFrame::STATE::TO_DELETE) {
      if (io_frame.readers_counter == 0) {
         partition.io_ht.remove(pid);
      }
      g_guard->unlock();
      jumpmu::jump();
   }
   ensure(false);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreturn-type" // compiler complains about: warning: control reaches end of non-void function
}  // namespace storage
#pragma GCC diagnostic pop
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   assert(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   mean::task::read(reinterpret_cast<char*>(destination), pid * PAGE_SIZE, bytes_left);
   // const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
   // -------------------------------------------------------------------------------------
   WorkerCounters::myCounters().read_operations_counter++;
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getIoPartitionID(PID pid)
{
   return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
IoPartition& BufferManager::getIoPartition(PID pid)
{
   const u64 partition_i = getIoPartitionID(pid);
   assert(partition_i < io_partitions_count);
   return io_partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
      MYPAUSE();
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   //stopBackgroundThreads();
   free(cooling_partitions);
   free(io_partitions);
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   // close(ssd_fd);
   // ssd_fd = -1;
   // munmap(bfs, dram_total_size);
   mean::IoInterface::freeIoMemory(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
