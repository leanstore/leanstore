#include "BufferManager.hpp"
#include "BufferFrame.hpp"
#include "AsyncWriteBuffer.hpp"
#include "Exceptions.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/btree/BTreeOptimistic.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/Config.hpp"
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
BufferManager::BufferManager()
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      const u64 dram_total_size = sizeof(BufferFrame) * u64(FLAGS_dram);
      bfs = reinterpret_cast<BufferFrame *>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      // -------------------------------------------------------------------------------------
      for ( u64 bf_i = 0; bf_i < FLAGS_dram; bf_i++ ) {
         dram_free_list.push(*new(bfs + bf_i) BufferFrame());
      }
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   // Init SSD pool
   const u32 ssd_total_size = FLAGS_ssd * PAGE_SIZE;
   int flags = O_RDWR | O_DIRECT | O_CREAT;
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
   posix_check(ftruncate(ssd_fd, ssd_total_size) == 0);
   if ( fcntl(ssd_fd, F_GETFL) == -1 ) {
      throw ex::GenericException("Can not initialize SSD storage: " + FLAGS_ssd_path);
   }
   // -------------------------------------------------------------------------------------
   for ( u64 pid = 0; pid < FLAGS_ssd; pid++ ) {
      cooling_io_ht.emplace(std::piecewise_construct, std::forward_as_tuple(pid), std::forward_as_tuple());
   }
   // -------------------------------------------------------------------------------------
   // Background threads
   // -------------------------------------------------------------------------------------
   std::thread page_provider_thread([&]() { pageProviderThread(); });
   bg_threads_counter++;
   page_provider_thread.detach();
   // -------------------------------------------------------------------------------------
   std::thread phase_timer_thread([&]() { debuggingThread(); });
   bg_threads_counter++;
   phase_timer_thread.detach();
}
// -------------------------------------------------------------------------------------
void BufferManager::FreeList::push(leanstore::buffermanager::BufferFrame &bf)
{
   assert(bf.header.state == BufferFrame::State::FREE);
   bf.header.next_free_bf = first.load();
   while ( !first.compare_exchange_strong(bf.header.next_free_bf, &bf));
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame &BufferManager::FreeList::pop()
{
   BufferFrame *c_header = first;
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   while ( true ) {
      while ( c_header == nullptr ) { //spin bf_s_lock
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         c_header = first;
      }
      BufferFrame *next = c_header->header.next_free_bf;
      if ( first.compare_exchange_strong(c_header, next)) {
         BufferFrame &bf = *c_header;
         bf.header.next_free_bf = nullptr;
         counter--;
         assert(bf.header.state == BufferFrame::State::FREE);
         return bf;
      }
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::pageProviderThread()
{
   pthread_setname_np(pthread_self(), "page_provider");
   auto logger = spdlog::rotating_logger_mt("PageProviderThread", "page_provider.txt", 1024 * 1024, 1);
   // -------------------------------------------------------------------------------------
   // Init AIO Context
   // TODO: own variable for page provider write buffer size
   AsyncWriteBuffer async_write_buffer(ssd_fd, PAGE_SIZE, FLAGS_async_batch_size);
   // -------------------------------------------------------------------------------------
   BufferFrame *r_buffer = &randomBufferFrame();
   //TODO: REWRITE!!
   const u64 free_pages_limit = FLAGS_free * FLAGS_dram / 100.0;
   const u64 cooling_pages_limit = FLAGS_cool * FLAGS_dram / 100.0;
   // -------------------------------------------------------------------------------------
   auto phase_1_condition = [&]() {
      return (dram_free_list.counter + cooling_bfs_counter) < cooling_pages_limit;
   };
   auto phase_2_condition = [&]() {
      return (dram_free_list.counter < free_pages_limit);
   };
   auto phase_3_condition = [&]() {
      return (cooling_bfs_counter > 0);
   };
   // -------------------------------------------------------------------------------------
   while ( bg_threads_keep_running ) {
      // Phase 1
      auto phase_1_begin = chrono::high_resolution_clock::now();
      try {
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
                                             *r_buffer, [&](Swip<BufferFrame> &swip) {
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
               assert(r_buffer->header.state == BufferFrame::State::HOT);
               ParentSwipHandler parent_handler = dt_registry.findParent(r_buffer->page.dt_id, *r_buffer);
               ExclusiveGuard p_x_guard(parent_handler.guard);
               std::lock_guard g_guard(cio_mutex);
               assert(parent_handler.guard.local_version == parent_handler.guard.version_ptr->load());
               assert(parent_handler.swip.bf == r_buffer);
               CIOFrame &cio_frame = cooling_io_ht[r_buffer->header.pid];
               cio_frame.state = CIOFrame::State::COOLING;
               cooling_fifo_queue.push_back(r_buffer);
               cio_frame.fifo_itr = --cooling_fifo_queue.end();
               r_buffer->header.state = BufferFrame::State::COLD;
               parent_handler.swip.unswizzle(r_buffer->header.pid);
               // -------------------------------------------------------------------------------------
               {
                  // check if any child is swizzle
                  {
                     dt_registry.iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame> &swip) {
                        if ( swip.isSwizzled()) {
                           ensure(false);
                        }
                        return true;
                     });
                  }
               }
               // -------------------------------------------------------------------------------------
               cooling_bfs_counter++;
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
      } catch ( RestartException e ) {
         r_buffer = &randomBufferFrame();
      }
      phase_2:
      // Phase 2: iterate over all bfs in cooling page, evicting up to free_pages_limit
      // and preparing aio for dirty pages
      auto phase_2_begin = chrono::high_resolution_clock::now();
      if ( phase_2_condition()) {
         // AsyncWrite (for dirty) or remove (clean) the oldest (n) pages from fifo
         std::unique_lock g_guard(cio_mutex);
         u64 pages_left_to_process = (dram_free_list.counter < free_pages_limit) ? free_pages_limit - dram_free_list.counter : 0;
         auto bf_itr = cooling_fifo_queue.begin();
         while ( pages_left_to_process-- && bf_itr != cooling_fifo_queue.end()) {
            BufferFrame &bf = **bf_itr;
            auto next_bf_tr = std::next(bf_itr, 1);
            PID pid = bf.header.pid;
            if ( !bf.header.isWB ) {
               if ( !bf.isDirty()) {
                  // Reclaim buffer frame
                  CIOFrame &cio_frame = cooling_io_ht[pid];
                  assert(cio_frame.state == CIOFrame::State::COOLING);
                  assert(bf.header.state == BufferFrame::State::COLD);
                  cooling_fifo_queue.erase(bf_itr);
                  cio_frame.state = CIOFrame::State::NOT_LOADED;
                  new(&bf.header) BufferFrame::Header();
                  dram_free_list.push(bf);
                  // -------------------------------------------------------------------------------------
                  // -------------------------------------------------------------------------------------
                  cooling_bfs_counter--;
                  debugging_counters.evicted_pages++;
               } else {
                  if ( async_write_buffer.add(bf)) {
                     debugging_counters.awrites_submitted++;
                  } else {
                     debugging_counters.awrites_submit_failed++;
                  }
               }
            }
            bf_itr = next_bf_tr;
         }
         g_guard.unlock();
      }
      // Phase 3
      auto phase_3_begin = chrono::high_resolution_clock::now();
      if ( phase_3_condition()) {
         async_write_buffer.submitIfNecessary();
         const u32 polled_events = async_write_buffer.pollEventsSync();
         std::lock_guard g_guard(cio_mutex);
         async_write_buffer.getWrittenBfs([&](BufferFrame &written_bf, u64 written_lsn) {
            while ( true ) {
               try {
                  assert(written_bf.header.isWB);
                  written_bf.header.lastWrittenLSN = written_lsn;
                  written_bf.header.isWB = false;
                  // -------------------------------------------------------------------------------------
                  stats.flushed_pages_counter++;
                  // -------------------------------------------------------------------------------------
                  // Evict
                  if ( written_bf.header.state == BufferFrame::State::COLD ) {
                     // Reclaim buffer frame
                     CIOFrame &cio_frame = cooling_io_ht[written_bf.header.pid];
                     assert(cio_frame.state == CIOFrame::State::COOLING);
                     assert(written_bf.header.state == BufferFrame::State::COLD);
                     cooling_fifo_queue.erase(cio_frame.fifo_itr);
                     cio_frame.state = CIOFrame::State::NOT_LOADED;
                     new(&written_bf) BufferFrame;
                     dram_free_list.push(written_bf);
                     // -------------------------------------------------------------------------------------
                     cooling_bfs_counter--;
                     debugging_counters.evicted_pages++;
                  }
                  return;
               } catch ( RestartException e ) {
               }
            }
         }, polled_events);
      }
      auto end = chrono::high_resolution_clock::now();
      // -------------------------------------------------------------------------------------
      debugging_counters.phase_1_ms += (chrono::duration_cast<chrono::microseconds>(phase_2_begin - phase_1_begin).count());
      debugging_counters.phase_2_ms += (chrono::duration_cast<chrono::microseconds>(phase_3_begin - phase_2_begin).count());
      debugging_counters.phase_3_ms += (chrono::duration_cast<chrono::microseconds>(end - phase_3_begin).count());
      debugging_counters.pp_thread_rounds++;
      // -------------------------------------------------------------------------------------
   }
   bg_threads_counter--;
   logger->info("end");
}
// -------------------------------------------------------------------------------------
void BufferManager::debuggingThread()
{
   cout << endl << "1\t2\t3\tfree_bfs\tcooling_bfs\tevicted_bfs\tawrites_submitted\twrites_submit_failed\tpp_rounds" << endl;
   // -------------------------------------------------------------------------------------
   s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0;
   while ( bg_threads_keep_running ) {
      local_phase_1_ms = debugging_counters.phase_1_ms.exchange(0);
      local_phase_2_ms = debugging_counters.phase_2_ms.exchange(0);
      local_phase_3_ms = debugging_counters.phase_3_ms.exchange(0);
      s64 total = local_phase_1_ms + local_phase_2_ms + local_phase_3_ms;
      if ( total > 0 ) {
         cout << u32(local_phase_1_ms * 100.0 / total)
              << "\t2:" << u32(local_phase_2_ms * 100.0 / total)
              << "\t3:" << u32(local_phase_3_ms * 100.0 / total)
              << "\tf:" << (dram_free_list.counter)
              << "\tc:" << (cooling_bfs_counter.load())
              << "\te:" << (debugging_counters.evicted_pages.exchange(0))
              << "\tas:" << (debugging_counters.awrites_submitted.exchange(0))
              << "\taf:" << (debugging_counters.awrites_submit_failed.exchange(0))
              << "\tpr:" << (debugging_counters.pp_thread_rounds.exchange(0))
              << endl;
      }
      sleep(2);
   }
   bg_threads_counter--;
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
   //TODOutils::writeBinary(FLAGS_free_pages_list_path.c_str(), ssd_free_pages);
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   //TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   return ssd_pages_counter;
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
BufferFrame &BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, FLAGS_dram);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame &BufferManager::allocatePage()
{
   if ( dram_free_list.counter < 10 ) {
      throw RestartException();
   }
   PID free_pid = ssd_pages_counter++;
   BufferFrame &free_bf = dram_free_list.pop();
   assert((&free_bf - bfs) < FLAGS_dram);
   assert(free_bf.header.state == BufferFrame::State::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.lock = 2; // Write lock
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::State::HOT;
   free_bf.header.lastWrittenLSN = free_bf.page.LSN = 0;
   // -------------------------------------------------------------------------------------
   return free_bf;
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
   swip_guard.recheck();
   const PID pid = swip_value.asPageID();
   assert(!swip_value.isSwizzled());
   // -------------------------------------------------------------------------------------
   CIOFrame &cio_frame = cooling_io_ht[pid];
   if ( cio_frame.state == CIOFrame::State::NOT_LOADED ) {
      if ( dram_free_list.counter < 10 ) {
         g_guard.unlock();
         spinAsLongAs(dram_free_list.counter < 10);
         throw RestartException();
      }
      BufferFrame &bf = dram_free_list.pop();
      assert(bf.header.state == BufferFrame::State::FREE);
      bf.header.lock = 2; // Write lock
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
      BufferFrame *bf = *cio_frame.fifo_itr;
      ExclusiveGuard swip_x_lock(swip_guard);
      assert(bf->header.pid == pid);
      swip_value.swizzle(bf);
      cooling_fifo_queue.erase(cio_frame.fifo_itr);
      cooling_bfs_counter--;
      assert(bf->header.state == BufferFrame::State::COLD);
      cio_frame.state = CIOFrame::State::WORKING;
      bf->header.state = BufferFrame::State::HOT; // ATTENTION: SET TO HOT AFTER IT IS SWIZZLED IN
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
      stats.swizzled_pages_counter++;
      // -------------------------------------------------------------------------------------
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
   debugging_counters.io_operations++;
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
   //TODO
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
   const u64 dram_total_size = sizeof(BufferFrame) * u64(FLAGS_dram);
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