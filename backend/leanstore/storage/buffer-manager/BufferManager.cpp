#include "BufferManager.hpp"
#include "Tracing.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
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

#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
thread_local BufferFrame* BufferManager::last_read_bf = nullptr;
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd) : ssd_fd(ssd_fd),
      dram_pool_size(FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame)),
      partitions_count(1<<FLAGS_partition_bits), partitions_mask(partitions_count -1)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      void* big_memory_chunk = mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (big_memory_chunk == MAP_FAILED) {
         perror("Failed to allocate memory for the buffer pool");
         SetupFailed("Check the buffer pool size");
      }
      bfs = reinterpret_cast<BufferFrame*>(big_memory_chunk);
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      madvise(bfs, dram_total_size,
              MADV_DONTFORK);  // O_DIRECT does not work with forking.
      // -------------------------------------------------------------------------------------
      // Initialize partitions
      const u64 free_bfs_limit = std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      for (u64 p_i = 0; p_i < partitions_count; p_i++) {
         partitions.push_back(std::make_unique<Partition>(p_i, partitions_count, free_bfs_limit));
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         FreedBfsBatch batches[partitions_count];
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            batches[p_i].add(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % partitions_count;
         }
         for (u64 i=0; i < partitions_count; i++){
            batches[i].set_free_list(&getPartition(i).dram_free_list);
            batches[i].push();
         }
      });
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::startBackgroundThreads()
{
   // Page Provider threads
   if (FLAGS_pp_threads) {  // make it optional for pure in-memory experiments
      std::vector<std::thread> pp_threads;
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         pp_threads.emplace_back(
             [&, t_i]() {
               PageProviderThread temp(t_i, this);
               temp.run();});
         bg_threads_counter++;
      }
      for (auto& thread : pp_threads) {
         thread.detach();
      }
   }
}
// -------------------------------------------------------------------------------------
std::unordered_map<std::string, std::string> BufferManager::serialize()
{
   // TODO: correctly serialize ranges of used pages
   std::unordered_map<std::string, std::string> map;
   PID max_pid = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      max_pid = std::max<PID>(getPartition(p_i).next_pid, max_pid);
   }
   map["max_pid"] = std::to_string(max_pid);
   return map;
}
// -------------------------------------------------------------------------------------
void BufferManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   PID max_pid = std::stol(map["max_pid"]);
   max_pid = (max_pid + (partitions_count - 1)) & ~(partitions_count - 1);
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      getPartition(p_i).next_pid = max_pid + p_i;
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   stopBackgroundThreads();
   ensure(!FLAGS_out_of_place);
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure(ret == PAGE_SIZE);
         }
         bf.header.latch.mutex.unlock();
      }
   });
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_freed_pages += getPartition(p_i).freedPages();
      total_used_pages += getPartition(p_i).allocatedPages();
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
Partition& BufferManager::randomPartition()
{
   auto rand_partition_i = utils::RandomGenerator::getRand<u64>(0, partitions_count);
   return getPartition(rand_partition_i);
}
FreeList& BufferManager::randomFreeList()
{
   return randomPartition().dram_free_list;
}
BufferFrame& BufferManager::randomFreeFrame()
{
   return randomFreeList().tryPop();
}
PID BufferManager::randomFreePID()
{
   return randomPartition().nextPID();
}

HashTable& BufferManager::getIOTable(PID pid){
   return getPartition(pid).io_table;
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   BufferFrame& free_bf = randomFreeFrame();
   PID free_pid = randomFreePID();
   assert(free_bf.header.state == BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.latch.assertNotExclusivelyLatched();
   free_bf.header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::STATE::HOT;
   free_bf.header.last_written_plsn = free_bf.page.PLSN = free_bf.page.GSN = 0;
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().allocate_operations_counter++; }
   // -------------------------------------------------------------------------------------
   return free_bf;
}
// -------------------------------------------------------------------------------------
void BufferManager::evictLastPage()
{
   if (!FLAGS_worker_page_eviction || !last_read_bf) {
      return;
   }
   jumpmuTry()
   {
      BMOptimisticGuard o_guard(last_read_bf->header.latch);
      if (pageIsNotEvictable(last_read_bf) || last_read_bf->isDirty()){
         jumpmu::jump();
      }
      o_guard.recheck();
      // -------------------------------------------------------------------------------------
      bool picked_a_child_instead = false;
      DTID dt_id = last_read_bf->page.dt_id;
      PID last_pid = last_read_bf->header.pid;
      o_guard.recheck();
      getDTRegistry().iterateChildrenSwips(dt_id, *last_read_bf, [&](Swip<BufferFrame>&) {
         picked_a_child_instead = true;
         return false;
      });
      if (picked_a_child_instead) {
         jumpmu::jump();
      }
      // assert(!partition.io_ht.lookup(last_read_bf->header.pid));
      // assert(!partition.io_ht.lookup(pid));
      FreedBfsBatch freed_bfs_batch = FreedBfsBatch();
      evict_bf(freed_bfs_batch, *last_read_bf, o_guard);
      freed_bfs_batch.set_free_list(&getPartition(last_pid).dram_free_list);
      freed_bfs_batch.push();
   }
   jumpmuCatch() { last_read_bf = nullptr; }
}
// -------------------------------------------------------------------------------------
void BufferManager::evict_bf(FreedBfsBatch& batch, BufferFrame& bf, BMOptimisticGuard& c_guard){
   DTID dt_id = bf.page.dt_id;
   c_guard.recheck();
   ParentSwipHandler parent_handler = getDTRegistry().findParent(dt_id, bf);
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
   // -------------------------------------------------------------------------------------
   const PID evicted_pid = bf.header.pid;
   parent_handler.swip.evict(evicted_pid);
   // -------------------------------------------------------------------------------------
   // Reclaim buffer frame
   bf.reset();
   bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
   bf.header.latch.mutex.unlock();
   // -------------------------------------------------------------------------------------
   batch.add(bf);
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
   COUNTERS_BLOCK() { PPCounters::myCounters().evicted_pages++; PPCounters::myCounters().total_evictions++; }
}
// -------------------------------------------------------------------------------------
bool BufferManager::pageIsNotEvictable(BufferFrame* r_buffer){
      // Check if not evictable
      return r_buffer->header.state != BufferFrame::STATE::HOT
         || r_buffer->header.keep_in_memory
         || r_buffer->header.is_being_written_back
         || r_buffer->header.latch.isExclusivelyLatched();
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
   Partition& partition = getPartition(bf.header.pid);
   if (FLAGS_recycle_pages) {
      partition.freePage(bf.header.pid);
   }
   // -------------------------------------------------------------------------------------
   if (bf.header.is_being_written_back) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
   } else {
      bf.reset();
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      partition.dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// Returns a non-latched BufferFrame, called by worker threads
BufferFrame& BufferManager::resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
{
   if (swip_value.isHOT()) {
      if(FLAGS_count_hits){
         COUNTERS_BLOCK() {leanstore::WorkerCounters::myCounters().hot_hit_counter++;}
      }
      BufferFrame& bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      return bf;
   }
   // -------------------------------------------------------------------------------------
   swip_guard.unlock();  // Otherwise we would get a deadlock, P->G, G->P
   const PID pid = swip_value.asPageID();
   HashTable& io_table = getIOTable(pid);
   JMUW<std::unique_lock<std::mutex>> g_guard(io_table.ht_mutex);
   swip_guard.recheck();
   paranoid(!swip_value.isHOT());
   // -------------------------------------------------------------------------------------
   auto frame_handler = io_table.lookup(pid);
   // The next section is locked by g_guard: Handling the IOFrame and readers_counter
   if (!frame_handler) {
      BufferFrame& bf = randomFreeFrame();
      IOFrame& io_frame = io_table.insert(pid);
      bf.header.latch.assertNotExclusivelyLatched();
      // -------------------------------------------------------------------------------------
      io_frame.state = IOFrame::STATE::READING;
      io_frame.readers_counter = 1;
      io_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      // -------------------------------------------------------------------------------------
      paranoid(bf.header.state == BufferFrame::STATE::FREE);
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().dt_page_reads[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      paranoid(bf.page.magic_debugging_number == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      paranoid(!bf.header.is_being_written_back);
      bf.header.last_written_plsn = bf.page.PLSN;
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      bf.header.tracker = BufferFrame::Header::Tracker();
      bf.header.tracker.trackRead();
      if (FLAGS_crc_check) {
         bf.header.crc = utils::CRC(bf.page.dt, EFFECTIVE_PAGE_SIZE);
      }
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<std::mutex>> g_guard(io_table.ht_mutex);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         io_frame.mutex.unlock();
         swip_value.setBF(&bf);
         bf.header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                     // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            io_table.remove(pid);
         }
         // -------------------------------------------------------------------------------------
         last_read_bf = &bf;
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
      io_frame.mutex.lock();
      io_frame.mutex.unlock();
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         g_guard->lock();
         if (io_frame.readers_counter == 0) {
            io_table.remove(pid);
         }
         g_guard->unlock();
      }
      // -------------------------------------------------------------------------------------
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READY) {
      // -------------------------------------------------------------------------------------
      BufferFrame* bf = io_frame.bf;
      {
         // We have to exclusively lock the bf because the page provider thread will
         // try to evict them when its IO is done
         bf->header.latch.assertNotExclusivelyLatched();
         paranoid(bf->header.state == BufferFrame::STATE::LOADED);
         BMOptimisticGuard bf_guard(bf->header.latch);
         BMExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         BMExclusiveGuard bf_x_guard(bf_guard);
         // -------------------------------------------------------------------------------------
         io_frame.bf = nullptr;
         paranoid(bf->header.pid == pid);
         swip_value.setBF(bf);
         paranoid(swip_value.isHOT());
         paranoid(bf->header.state == BufferFrame::STATE::LOADED);
         bf->header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                      // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            io_table.remove(pid);
         } else {
            io_frame.state = IOFrame::STATE::TO_DELETE;
         }
         g_guard->unlock();
         // -------------------------------------------------------------------------------------
         last_read_bf = bf;
         return *bf;
      }
   }
   if (io_frame.state == IOFrame::STATE::TO_DELETE) {
      if (io_frame.readers_counter == 0) {
         io_table.remove(pid);
      }
      g_guard->unlock();
      jumpmu::jump();
   }
   ensure(false);
}  // namespace storage
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   paranoid(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_read > 0);  // call was successfull?
      bytes_left -= bytes_read;
   } while (bytes_left > 0);
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().read_operations_counter++; }
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getPartitionID(PID pid)
{
   return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
Partition& BufferManager::getPartition(PID pid)
{
   const u64 partition_i = getPartitionID(pid);
   assert(partition_i < partitions_count);
   return *partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   munmap(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
