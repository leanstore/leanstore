#include "BufferManager.hpp"
#include "BufferFrame.hpp"
#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
#include "leanstore/utils/FVector.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/rotating_file_sink.h" // support for rotating file logging
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <unistd.h>
#include <emmintrin.h>
#include <set>
#include <iomanip>
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(Config config_snap)
        : config(config_snap)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      const u64 dram_total_size = sizeof(BufferFrame) * u64(config.dram_pages_count);
      bfs = reinterpret_cast<BufferFrame *>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      for ( u64 bf_i = 0; bf_i < config.dram_pages_count; bf_i++ ) {
         dram_free_bfs.push_back(new(bfs + bf_i) BufferFrame());
      }
      dram_free_bfs_counter = config.dram_pages_count;
   }
   // -------------------------------------------------------------------------------------
   // Init SSD pool
   const u32 ssd_total_size = config.ssd_pages_count * PAGE_SIZE;
   int flags = O_RDWR | O_DIRECT | O_CREAT;
   ssd_fd = open(config.ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
   posix_check(ftruncate(ssd_fd, ssd_total_size) == 0);
   if ( fcntl(ssd_fd, F_GETFL) == -1 ) {
      throw ex::GenericException("Can not initialize SSD storage: " + config.ssd_path);
   }
   // -------------------------------------------------------------------------------------
   for ( u64 pid = 0; pid < config.ssd_pages_count; pid++ ) {
      cooling_io_ht.emplace(std::piecewise_construct, std::forward_as_tuple(pid), std::forward_as_tuple());
      ssd_free_pages.push_back(pid);
   }
   // -------------------------------------------------------------------------------------
   // Background threads
   // -------------------------------------------------------------------------------------
   // Init time
   std::thread page_provider_thread([&]() {
      pthread_setname_np(pthread_self(), "page_provider");
      auto logger = spdlog::rotating_logger_mt("PageProviderThread", "page_provider.txt", 1024 * 1024, 1);
      // -------------------------------------------------------------------------------------
      // Init AIO Context
      // TODO: own variable for page provider write buffer size
      AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, config.write_buffer_size);
      // -------------------------------------------------------------------------------------
      BufferFrame *r_buffer = &randomBufferFrame();
      //TODO: REWRITE!!
      const u64 free_pages_limit = config.free_pct * config.dram_pages_count / 100.0;
      const u64 cooling_pages_limit = config.cool_pct * config.dram_pages_count / 100.0;
      // -------------------------------------------------------------------------------------
      auto phase_1_condition = [&]() {
         return (dram_free_bfs_counter + cooling_bfs_counter) < cooling_pages_limit;
      };
      auto phase_2_condition = [&]() {
         return (dram_free_bfs_counter < free_pages_limit);
      };
      auto phase_3_condition = [&]() {
         return true;
      };
      // -------------------------------------------------------------------------------------
      while ( bg_threads_keep_running ) {
         try {
            // Phase 1
            auto phase_1_begin = chrono::high_resolution_clock::now();
            while ( phase_1_condition()) {
               // unswizzle pages (put in the cooling stage)
               ReadGuard r_guard(r_buffer->header.lock);
               const bool is_cooling_candidate = r_buffer->header.state == BufferFrame::State::HOT; // && !rand_buffer->header.isWB
               if ( !is_cooling_candidate ) {
                  r_buffer = &randomBufferFrame();
                  continue;
               }
               r_guard.recheck();
               // -------------------------------------------------------------------------------------
               bool picked_a_child_instead = false;
               dt_registry.iterateChildrenSwips(r_buffer->page.dt_id,
                                                *r_buffer, r_guard, [&](Swip<BufferFrame> &swip) {
                          if ( swip.isSwizzled()) {
                             r_buffer = &swip.asBufferFrame();
                             r_guard.recheck();
                             picked_a_child_instead = true;
                             return false;
                          }
                          r_guard.recheck();
                          return true;
                       });
               if ( picked_a_child_instead ) {
                  continue; //restart the inner loop
               }
               // -------------------------------------------------------------------------------------
               // Suitable page founds, lets unswizzle
               {
                  ExclusiveGuard r_x_guad(r_guard);
                  ParentSwipHandler parent_handler = dt_registry.findParent(r_buffer->page.dt_id, *r_buffer, r_guard);
                  ExclusiveGuard p_x_guard(parent_handler.guard);
                  std::lock_guard g_guard(cio_mutex); // must accquire the mutex before exclusive locks

                  dt_registry.iterateChildrenSwips(r_buffer->page.dt_id,
                                                   *r_buffer, r_guard, [&](Swip<BufferFrame> &swip) {
                             if ( swip.isSwizzled()) {
                                raise(SIGTRAP);
                                return false;
                             } else {
                                return true;
                             }
                          });
                  assert(parent_handler.guard.local_version == parent_handler.guard.version_ptr->load());
                  assert(parent_handler.swip.bf == r_buffer);
                  parent_handler.swip.unswizzle(r_buffer->header.pid);
                  CIOFrame &cio_frame = cooling_io_ht[r_buffer->header.pid];
                  cio_frame.state = CIOFrame::State::COOLING;
                  cooling_fifo_queue.push_back(r_buffer);
                  cio_frame.fifo_itr = --cooling_fifo_queue.end();
                  r_buffer->header.state = BufferFrame::State::COLD;
                  cooling_bfs_counter++;
                  // -------------------------------------------------------------------------------------
                  stats.unswizzled_pages_counter++;
                  // -------------------------------------------------------------------------------------
                  if ( !phase_1_condition()) {
                     r_buffer = &randomBufferFrame();
                     goto phase_2;
                  }
               }
               r_buffer = &randomBufferFrame();
               // -------------------------------------------------------------------------------------
            }
            phase_2:
            // Phase 2: iterate over all bfs in cooling page, evicting up to free_pages_limit
            // and preparing aio for dirty pages
            auto phase_2_begin = chrono::high_resolution_clock::now();
            if ( phase_2_condition()) {
               u64 pages_left_to_evict = (dram_free_bfs_counter < free_pages_limit) ? free_pages_limit - dram_free_bfs_counter : 0;
               // AsyncWrite (for dirty) or remove (clean) the oldest (n) pages from fifo
               std::unique_lock g_guard(cio_mutex);
               //TODO: other variable than async_batch_size
               auto bf_itr = cooling_fifo_queue.begin();
               while ( bf_itr != cooling_fifo_queue.end()) {
                  BufferFrame &bf = **bf_itr;
                  auto next_bf_tr = std::next(bf_itr, 1);
                  PID pid = bf.header.pid;
                  // TODO: can we write multiple  versions sim ?
                  // TODO: current implementation assume that checkpoint thread does not touch the
                  if ( bf.header.isWB == false ) {
                     if ( !bf.isDirty()) {
                        if ( pages_left_to_evict ) {
                           std::lock_guard reservoir_guard(free_list_mutex);
                           // Reclaim buffer frame
                           CIOFrame &cio_frame = cooling_io_ht[pid];
                           assert(cio_frame.state == CIOFrame::State::COOLING);
                           cooling_fifo_queue.erase(bf_itr);
                           cio_frame.state = CIOFrame::State::NOT_LOADED;
                           dram_free_bfs.push_back(&bf);
                           // -------------------------------------------------------------------------------------
                           new(&bf.header) BufferFrame::Header();
                           assert(bf.header.lock == 0);
                           // -------------------------------------------------------------------------------------
                           dram_free_bfs_counter++;
                           cooling_bfs_counter--;
                           pages_left_to_evict--;
                           periodic_counters.evicted_pages++;
                        }
                     } else {
                        async_write_buffer.add(bf);
                        periodic_counters.awrites_submitted++;
                     }
                  }
                  bf_itr = next_bf_tr;
               }
               g_guard.unlock();
            }
            // Phase 3
            auto phase_3 = chrono::high_resolution_clock::now();
            if ( phase_3_condition()) {
               async_write_buffer.submitIfNecessary([&](BufferFrame &written_bf, u64 written_lsn) {
                  while ( true ) {
                     try {
                        ReadGuard guard(written_bf.header.lock);
                        ExclusiveGuard x_guard(guard);
                        assert(written_bf.header.isWB == true);
                        written_bf.header.lastWrittenLSN = written_lsn;
                        written_bf.header.isWB = false;
                        // -------------------------------------------------------------------------------------
                        stats.flushed_pages_counter++;
                        return;
                     } catch ( RestartException e ) {
                     }
                  }
               }, config.async_batch_size);
            }
            auto end = chrono::high_resolution_clock::now();
            // -------------------------------------------------------------------------------------
            periodic_counters.phase_1_ms += (chrono::duration_cast<chrono::microseconds>(phase_2_begin - phase_1_begin).count());
            periodic_counters.phase_2_ms += (chrono::duration_cast<chrono::microseconds>(phase_3 - phase_2_begin).count());
            periodic_counters.phase_3_ms += (chrono::duration_cast<chrono::microseconds>(end - phase_3).count());
            // -------------------------------------------------------------------------------------
         } catch ( RestartException e ) {
            r_buffer = &randomBufferFrame();
         }
      }
      bg_threads_counter--;
      logger->info("end");
   });
   bg_threads_counter++;
   page_provider_thread.detach();
   // -------------------------------------------------------------------------------------
   std::thread phase_timer_thread([&]() {
      cout << endl << "1\t2\t3\tfree_bfs\tcooling_bfs\tevicted_bfs\tawrites_submitted" << endl;
      // -------------------------------------------------------------------------------------
      s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0;
      while ( bg_threads_keep_running ) {
         local_phase_1_ms = periodic_counters.phase_1_ms.exchange(0);
         local_phase_2_ms = periodic_counters.phase_2_ms.exchange(0);
         local_phase_3_ms = periodic_counters.phase_3_ms.exchange(0);
         s64 total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
         if ( total > 0 ) {
            cout << std::fixed << std::setprecision(0);
            cout << std::round(local_phase_1_ms * 100.0 / total)
                 << "\t" << std::round(local_phase_2_ms * 100.0 / total)
                 << "\t" << std::round(local_phase_3_ms * 100.0 / total)
                 << "\t" << (dram_free_bfs_counter.load())
                 << "\t" << (cooling_bfs_counter.load())
                 << "\t" << (periodic_counters.evicted_pages.exchange(0))
                 << "\t" << (periodic_counters.awrites_submitted.exchange(0))
                 << endl;
         }
         sleep(2);
      }
      bg_threads_counter--;
   });
   bg_threads_counter++;
   phase_timer_thread.detach();
}
// -------------------------------------------------------------------------------------
void BufferManager::clearSSD()
{
   ftruncate(ssd_fd, 0);
}
// -------------------------------------------------------------------------------------
void BufferManager::persist()
{
   stopBackgroundThreads();
   flushDropAllPages();
   utils::writeBinary(config.free_pages_list_path.c_str(), ssd_free_pages);
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   utils::fillVectorFromBinaryFile(config.free_pages_list_path.c_str(), ssd_free_pages);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   return config.ssd_pages_count - ssd_free_pages.size();
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, config.dram_pages_count);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame &BufferManager::allocatePage()
{
   if ( dram_free_bfs_counter == 0 ) {
      throw RestartException();
   }
   std::lock_guard lock(free_list_mutex);
   if ( !ssd_free_pages.size()) {
      throw ex::GenericException("Ran out of SSD Pages");
   }
   if ( !dram_free_bfs.size()) {
      throw RestartException();
   }
   auto free_pid = ssd_free_pages.back();
   ssd_free_pages.pop_back();
   auto free_bf = dram_free_bfs.back();
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf->header.lock = 2; // Write lock
   free_bf->header.pid = free_pid;
   free_bf->header.state = BufferFrame::State::HOT;
   free_bf->header.lastWrittenLSN = free_bf->page.LSN = 0;
   // -------------------------------------------------------------------------------------
   dram_free_bfs.pop_back();
   dram_free_bfs_counter--;
   // -------------------------------------------------------------------------------------
   return *free_bf;
}
// -------------------------------------------------------------------------------------
void BufferManager::deletePageWithBf(BufferFrame &bf)
{
   std::lock_guard lock(free_list_mutex);
   new(&bf) BufferFrame();
   ssd_free_pages.push_back(bf.header.pid);
   dram_free_bfs.push_back(&bf);
   dram_free_bfs_counter++;
}
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::resolveSwip(ReadGuard &swip_guard, Swip<BufferFrame> &swip_value) // throws RestartException
{
   static auto logger = spdlog::rotating_logger_mt("ResolveSwip", "resolve_swip.txt", 1024 * 1024, 1);
   if ( swip_value.isSwizzled()) {
      BufferFrame &bf = swip_value.asBufferFrame();
      swip_guard.recheck();
      return bf;
   }
   // -------------------------------------------------------------------------------------
   std::unique_lock g_guard(cio_mutex);
   const PID pid = swip_value.asPageID();
   swip_guard.recheck();
   // -------------------------------------------------------------------------------------
   CIOFrame &cio_frame = cooling_io_ht[pid];
   if ( cio_frame.state == CIOFrame::State::NOT_LOADED ) {
      //TODO: something wrong here
      // First posix_check if we have enough pages
      std::unique_lock reservoir_guard(free_list_mutex);
      if ( !dram_free_bfs.size()) {
         throw RestartException();
      }
      BufferFrame &bf = *dram_free_bfs.back();
      bf.header.lock = 2; // Write lock
      dram_free_bfs.pop_back();
      dram_free_bfs_counter--;
      reservoir_guard.unlock();
      // -------------------------------------------------------------------------------------
      cio_frame.readers_counter++;
      cio_frame.state = CIOFrame::State::READING;
      cio_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard.unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      assert(bf.page.magic_debugging_number == pid);
      // ATTENTION: Fill the BF
      bf.header.lastWrittenLSN = bf.page.LSN;
      bf.header.state = BufferFrame::State::COLD;
      bf.header.isWB = false;
      assert(bf.header.pid == 9999);
      bf.header.pid = pid;
      // -------------------------------------------------------------------------------------
      // Move to cooling stage
      g_guard.lock();
      cio_frame.state = CIOFrame::State::COOLING;
      cooling_fifo_queue.push_back(&bf);
      cio_frame.fifo_itr = --cooling_fifo_queue.end();
      cooling_bfs_counter++;
      bf.header.lock = 0;
      g_guard.unlock();
      cio_frame.mutex.unlock();
      throw RestartException();
      // TODO: do we really need to clean up ?
   }
   if ( cio_frame.state == CIOFrame::State::READING ) {
      cio_frame.readers_counter++;
      g_guard.unlock();
      cio_frame.mutex.lock();
      cio_frame.readers_counter--;
      cio_frame.mutex.unlock();
      throw RestartException();
   }
   /*
    * Lessons learned here:
    * don't catch a restart exception here
    * Whenever we fail to accquire a lock or witness a version change
    * then we have to read the value ! (update SharedGuard)
    * otherwise we would stick with the wrong version the whole time
    * and nasty things would happen
    */
   if ( cio_frame.state == CIOFrame::State::COOLING ) {
      ExclusiveGuard x_lock(swip_guard);
      BufferFrame *bf = *cio_frame.fifo_itr;
      assert(bf->header.pid == pid);
      // TODO: do we really need them ?
      ReadGuard bf_guard(bf->header.lock);
      ExclusiveGuard bf_x_guard(bf_guard);

      cooling_fifo_queue.erase(cio_frame.fifo_itr);
      cooling_bfs_counter--;
      assert(bf->header.state == BufferFrame::State::COLD);
      cio_frame.state = CIOFrame::State::NOT_LOADED;
      bf->header.state = BufferFrame::State::HOT;
      // -------------------------------------------------------------------------------------
      swip_value.swizzle(bf);
      // -------------------------------------------------------------------------------------
      stats.swizzled_pages_counter++;
      return *bf;
   }
   // it is a bug signal, if the page was hot then we should never hit this path
   UNREACHABLE();
}
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8 *destination)
{
   assert(u64(destination) % 512 == 0);
   s64 bytes_left = PAGE_SIZE;
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_left > 0);
      bytes_left -= bytes_read;
   } while ( bytes_left > 0 );
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void BufferManager::registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta)
{
   dt_registry.dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
DTID BufferManager::registerDatastructureInstance(DTType type, void *root_object)
{
   DTID new_instance_id = dt_registry.dt_types_ht[type].instances_counter++;
   dt_registry.dt_instances_ht.insert({new_instance_id, {type, root_object}});
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
// Make sure all worker threads are off
void BufferManager::flushDropAllPages()
{
   stopBackgroundThreads();
   return; // TODO
   BufferFrame *bf = &randomBufferFrame();
   while ( dram_free_bfs_counter != config.dram_pages_count ) {
      try {
         if ( bf->header.state != BufferFrame::State::HOT ) {
            bf = &randomBufferFrame();
            continue;
         }
         bool picked_a_child_instead = false;
         ReadGuard guard(bf->header.lock);
         dt_registry.iterateChildrenSwips(bf->page.dt_id,
                                          *bf, guard, [&](Swip<BufferFrame> &swip) {
                    if ( swip.isSwizzled()) {
                       bf = &swip.asBufferFrame();
                       picked_a_child_instead = true;
                       return false;
                    }
                    return true;
                 });
         if ( picked_a_child_instead ) {
            continue; //restart the inner loop
         }
         ParentSwipHandler parent_handler = dt_registry.findParent(bf->page.dt_id, *bf, guard);
         parent_handler.swip.unswizzle(bf->header.pid);
         bf->page.magic_debugging_number = bf->header.pid;
         pwrite(ssd_fd, bf->page, PAGE_SIZE, PAGE_SIZE * bf->header.pid);
      } catch ( RestartException e ) {
         bf = &randomBufferFrame();
      }
   }
   fDataSync();
   cooling_bfs_counter = 0;
   cooling_fifo_queue.clear();
   for ( auto &entry: cooling_io_ht ) {
      entry.second.state = CIOFrame::State::NOT_LOADED;
      entry.second.readers_counter = 0;
   }
   for ( u64 bf_i = 0; bf_i < config.dram_pages_count; bf_i++ ) {
      new(bfs + bf_i) BufferFrame();
   }
   // -------------------------------------------------------------------------------------
   stats.print();
   stats.reset();
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while ( bg_threads_counter ) {
      _mm_pause();
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   const u64 dram_total_size = sizeof(BufferFrame) * u64(config.dram_pages_count);
   close(ssd_fd);
   ssd_fd = -1;
   munmap(bfs, dram_total_size);
   // -------------------------------------------------------------------------------------
   stats.print();
   // -------------------------------------------------------------------------------------
   spdlog::drop_all();
}
// -------------------------------------------------------------------------------------
void BufferManager::Stats::print()
{
   cout << "-------------------------------------------------------------------------------------" << endl;
   cout << "BufferManager Stats" << endl;
   cout << "swizzled counter = " << swizzled_pages_counter << endl;
   cout << "unswizzled counter = " << unswizzled_pages_counter << endl;
   cout << "flushed counter = " << flushed_pages_counter << endl;
   cout << "-------------------------------------------------------------------------------------" << endl;
}
// -------------------------------------------------------------------------------------
void BufferManager::Stats::reset()
{
   swizzled_pages_counter = 0;
   unswizzled_pages_counter = 0;
   flushed_pages_counter = 0;
}
// -------------------------------------------------------------------------------------
BufferManager *BMC::global_bf(nullptr);
}
}
// -------------------------------------------------------------------------------------